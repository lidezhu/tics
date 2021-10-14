#pragma once

#include <Storages/DeltaMerge/File/IDTFileWriter.h>

namespace DB
{
namespace DM
{
class DTFileFolderModeWriter : public IDTFileWriter
{
public:
    using WriteBufferFromFileBasePtr = std::unique_ptr<WriteBufferFromFileBase>;
    struct Stream
    {
        Stream(const DTFilePtr & dtfile,
               const String & file_base_name,
               const DataTypePtr & type,
               CompressionSettings compression_settings,
               size_t max_compress_block_size,
               FileProviderPtr & file_provider,
               const WriteLimiterPtr & write_limiter_,
               bool do_index)
                : plain_file(createWriteBufferFromFileBaseByFileProvider(file_provider,
                                                                         dtfile->path() + "/" + IDTFile::colDataFileName(file_base_name),
                                                                         EncryptionPath(dtfile->encryptionBasePath(), IDTFile::colDataFileName(file_base_name)),
                                                                         false,
                                                                         write_limiter_,
                                                                         0,
                                                                         0,
                                                                         max_compress_block_size))
                , plain_layer(*plain_file)
                , compressed_buf(plain_layer, compression_settings)
                , original_layer(compressed_buf)
                , minmaxes(do_index ? std::make_shared<MinMaxIndex>(*type) : nullptr)
                , mark_file(
                        file_provider,
                        dtfile->path() + "/" + IDTFile::colMarkFileName(file_base_name),
                        EncryptionPath(dtfile->encryptionBasePath(), IDTFile::colMarkFileName(file_base_name)),
                        false,
                        write_limiter_)
        {
        }

        void flush()
        {
            // Note that this method won't flush minmaxes.
            original_layer.next();
            compressed_buf.next();
            plain_layer.next();
            plain_file->next();

            plain_file->sync();
            mark_file.sync();
        }

        // Get written bytes of `plain_file` && `mark_file`. Should be called after `flush`.
        // Note that this class don't take responsible for serializing `minmaxes`,
        // bytes of `minmaxes` won't be counted in this method.
        size_t getWrittenBytes() { return plain_file->getPositionInFile() + mark_file.getPositionInFile(); }

        /// original_hashing -> compressed_buf -> plain_hashing -> plain_file
        WriteBufferFromFileBasePtr plain_file;
        WriteBufferProxy plain_layer;
        CompressedWriteBuffer<> compressed_buf;
        WriteBufferProxy original_layer;

        MinMaxIndexPtr minmaxes;
        WriteBufferFromFileProvider mark_file;
    };
    using StreamPtr = std::unique_ptr<Stream>;
    using ColumnStreams = std::map<String, StreamPtr>;

    DTFileFolderModeWriter(const DTFilePtr & dtfile_,
                  const ColumnDefines & write_columns_,
                  const FileProviderPtr & file_provider_,
                  const WriteLimiterPtr & write_limiter_,
                  const IDTFile::WriteOptions & options_);

    void write(const Block & block, const BlockProperty & block_property) override;
    void finalize() override;

private:
    /// Add streams with specified column id. Since a single column may have more than one Stream,
    /// for example Nullable column has a NullMap column, we would track them with a mapping
    /// FileNameBase -> Stream.
    void addStreams(ColId col_id, DataTypePtr type, bool do_index);

    void finalizeColumn(ColId col_id, DataTypePtr type);
    void writeColumn(ColId col_id, const IDataType & type, const IColumn & column, const ColumnVector<UInt8> * del_mark);

private:
    WriteBufferFromFileBasePtr pack_stat_file;
    ColumnStreams column_streams;
};
}
}
