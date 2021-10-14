#include <Storages/DeltaMerge/File/DTFileFolderModeWriter.h>
#include <Storages/DeltaMerge/File/DTFileFolderMode.h>
#include <Common/TiFlashException.h>
#include <Storages/DeltaMerge/DeltaMergeHelpers.h>

namespace DB
{
namespace DM
{
DTFileFolderModeWriter::DTFileFolderModeWriter(
    const DTFilePtr & dtfile_,
    const ColumnDefines & write_columns_,
    const FileProviderPtr & file_provider_,
    const WriteLimiterPtr & write_limiter_,
    const IDTFile::WriteOptions & options_)
    : IDTFileWriter(dtfile_,
                    write_columns_,
                    file_provider_,
                    write_limiter_,
                    options_)
    // assume pack_stat_file is the first file created inside DMFile
    // it will create encryption info for the whole DMFile
  , pack_stat_file(createWriteBufferFromFileBaseByFileProvider(file_provider_,
                                                                   dtfile_->path() + "/" + IDTFile::packStatFileName(),
                                                                   EncryptionPath(dtfile_->encryptionBasePath(), IDTFile::packStatFileName()),
                                                                   true,
                                                                   write_limiter_,
                                                                   0,
                                                                   0,
                                                                   options.max_compress_block_size))
{
    for (auto & cd : write_columns)
    {
        bool do_index = details::isColumnSupportIndex(cd);
        addStreams(cd.id, cd.type, do_index);
        dtfile->column_stats.emplace(cd.id, ColumnStat{cd.id, cd.type, /*avg_size=*/0});
    }
}

void DTFileFolderModeWriter::write(const Block &block, const IDTFileWriter::BlockProperty &block_property)
{
    IDTFile::PackStat stat;
    stat.rows = block.rows();
    stat.not_clean = block_property.not_clean_rows;
    stat.bytes = block.bytes(); // This is bytes of pack data in memory.

    auto del_mark_column = tryGetByColumnId(block, TAG_COLUMN_ID).column;

    const ColumnVector<UInt8> * del_mark = !del_mark_column ? nullptr : (const ColumnVector<UInt8> *)del_mark_column.get();

    for (auto & cd : write_columns)
    {
        auto & col = getByColumnId(block, cd.id).column;
        writeColumn(cd.id, *cd.type, *col, del_mark);

        if (cd.id == VERSION_COLUMN_ID)
            stat.first_version = col->get64(0);
        else if (cd.id == TAG_COLUMN_ID)
            stat.first_tag = (UInt8)(col->get64(0));
    }

    writePODBinary(stat, *pack_stat_file);
    dtfile->addPack(stat);

    auto & properties = dtfile->getPackProperties();
    auto * property = properties.add_property();
    property->set_num_rows(block_property.effective_num_rows);
    property->set_gc_hint_version(block_property.gc_hint_version);
}

void DTFileFolderModeWriter::finalize()
{
    pack_stat_file->sync();

    for (auto & cd : write_columns)
    {
        finalizeColumn(cd.id, cd.type);
    }

    static_cast<DTFileFolderMode &>(*dtfile).finalize(file_provider, write_limiter);
}

void DTFileFolderModeWriter::addStreams(ColId col_id, DataTypePtr type, bool do_index)
{
    auto callback = [&](const IDataType::SubstreamPath & substream_path) {
        const auto stream_name = IDTFile::getFileNameBase(col_id, substream_path);
        auto stream = std::make_unique<Stream>(
                dtfile,
                stream_name,
                type,
                options.compression_settings,
                options.max_compress_block_size,
                file_provider,
                write_limiter,
                IDataType::isNullMap(substream_path) ? false : do_index);
        column_streams.emplace(stream_name, std::move(stream));
    };

    type->enumerateStreams(callback, {});
}

void DTFileFolderModeWriter::writeColumn(ColId col_id, const IDataType &type, const IColumn &column,
                                         const ColumnVector<UInt8> *del_mark)
{
    size_t rows = column.size();

    type.enumerateStreams(
        [&](const IDataType::SubstreamPath & substream) {
            const auto name = IDTFile::getFileNameBase(col_id, substream);
            auto & stream = column_streams.at(name);
            if (stream->minmaxes)
                stream->minmaxes->addPack(column, del_mark);

            /// There could already be enough data to compress into the new block.
            if (stream->original_layer.offset() >= options.min_compress_block_size)
                stream->original_layer.next();

            auto offset_in_compressed_block = stream->original_layer.offset();

            writeIntBinary(stream->plain_layer.count(), stream->mark_file);
            writeIntBinary(offset_in_compressed_block, stream->mark_file);
        },
        {});

    type.serializeBinaryBulkWithMultipleStreams(
        column,
        [&](const IDataType::SubstreamPath & substream) {
            const auto stream_name = IDTFile::getFileNameBase(col_id, substream);
            auto & stream = column_streams.at(stream_name);
            return &(stream->original_layer);
        },
        0,
        rows,
        true,
        {});

    type.enumerateStreams(
        [&](const IDataType::SubstreamPath & substream) {
            const auto name = IDTFile::getFileNameBase(col_id, substream);
            auto & stream = column_streams.at(name);
            stream->original_layer.nextIfAtEnd();
        },
        {});

    auto & avg_size = dtfile->column_stats.at(col_id).avg_size;
    IDataType::updateAvgValueSizeHint(column, avg_size);
}

void DTFileFolderModeWriter::finalizeColumn(ColId col_id, DataTypePtr type)
{
    size_t bytes_written = 0;

    auto callback = [&](const IDataType::SubstreamPath & substream) {
        const auto stream_name = IDTFile::getFileNameBase(col_id, substream);
        auto & stream = column_streams.at(stream_name);
        stream->flush();
        bytes_written += stream->getWrittenBytes();

        if (stream->minmaxes)
        {
            WriteBufferFromFileProvider buf(
                    file_provider,
                    dtfile->path() + "/" + IDTFile::colIndexFileName(stream_name),
                    EncryptionPath(dtfile->encryptionBasePath(), IDTFile::colIndexFileName(stream_name)),
                    false,
                    write_limiter);
            stream->minmaxes->write(*type, buf);
            buf.sync();
            bytes_written += buf.getPositionInFile();
        }
    };
    type->enumerateStreams(callback, {});

    // Update column's bytes in disk
    dtfile->column_stats.at(col_id).serialized_bytes = bytes_written;
}
} // namespace DM
} // namespace DB
