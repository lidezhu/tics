#include <Common/FailPoint.h>
#include <Common/StringUtils/StringUtils.h>
#include <Common/escapeForFileName.h>
#include <Encryption/ReadBufferFromFileProvider.h>
#include <Encryption/WriteBufferFromFileProvider.h>
#include <Encryption/FileProvider.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <Poco/File.h>
#include <Storages/Page/PageUtil.h>
#include <Storages/DeltaMerge/File/DTFileFolderMode.h>
#include <Storages/DeltaMerge/File/DTFileFolderModeWriter.h>

namespace DB
{
namespace FailPoints
{
    extern const char exception_before_dmfile_remove_encryption[];
    extern const char exception_before_dmfile_remove_from_disk[];
} // namespace FailPoints
namespace DM
{
DTFilePtr
DTFileFolderMode::create(UInt64 file_id, const String &parent_path)
{
    Poco::Logger * log = &Poco::Logger::get("DTFileFolderMode");
    // On create, ref_id is the same as file_id.
    DTFileFolderMode *new_dtfile_rawptr = new DTFileFolderMode(file_id, file_id, parent_path, Status::WRITABLE, log);
    DTFilePtr new_dtfile(new_dtfile_rawptr);

    auto path = new_dtfile->path();
    Poco::File file(path);
    if (file.exists())
    {
        file.remove(true);
        LOG_WARNING(log, __PRETTY_FUNCTION__ << ": Existing dtfile, removed: " << path);
    }

    file.createDirectories();
    // Create a mark file to stop this dmfile from being removed by GC.
    PageUtil::touchFile(new_dtfile_rawptr->ngcPath());

    return new_dtfile;
}

DTFilePtr DTFileFolderMode::restore(const FileProviderPtr &file_provider, UInt64 file_id, UInt64 ref_id,
                                    const String &parent_path, const IDTFile::ReadMetaMode &read_meta_mode)
{
    String path = IDTFile::getPathByStatus(parent_path, file_id, IDTFile::Status::READABLE);
    DTFileFolderMode * dtfile_rawptr = new DTFileFolderMode(
            file_id,
            ref_id,
            parent_path,
            Status::READABLE,
            &Poco::Logger::get("DTFileFolderMode"));
    if (!read_meta_mode.isNone())
        dtfile_rawptr->readMetadata(file_provider, read_meta_mode);
    return DTFilePtr(dtfile_rawptr);
}

DTFileWriterPtr DTFileFolderMode::createWriter(const ColumnDefines & write_columns_,
                                               const FileProviderPtr & file_provider_,
                                               const WriteLimiterPtr & write_limiter_,
                                               const WriteOptions & options_)
{
    return DTFileWriterPtr(new DTFileFolderModeWriter(
            shared_from_this(),
            write_columns_,
            file_provider_,
            write_limiter_,
            options_));
}

DTFileReaderPtr DTFileFolderMode::createReader()
{
    return DB::DM::DTFileReaderPtr();
}

void
DTFileFolderMode::readMetadata(const FileProviderPtr &file_provider, const IDTFile::ReadMetaMode &read_meta_mode)
{
    if (read_meta_mode.isAll())
    {
        initializeSubFileSize();
    }
    if (read_meta_mode.needPackProperty())
    {
        String property_path(path() + "/" + IDTFile::packPropertyFileName());
        Poco::File property_file(property_path);
        if (property_file.exists())
        {
            ReadBufferFromFileProvider buf(
                file_provider,
                property_path,
                EncryptionPath(property_path, IDTFile::packPropertyFileName()),
                std::min(static_cast<size_t>(DBMS_DEFAULT_BUFFER_SIZE), property_file.getSize()));
            String tmp_str;
            readStringBinary(tmp_str, buf);
            pack_properties.ParseFromString(tmp_str);
        }
    }
    if (read_meta_mode.needColumnStat())
    {
        String column_stat_path(path() + "/" + IDTFile::columnStatFileName());
        ReadBufferFromFileProvider buf(
                file_provider,
                column_stat_path,
                EncryptionPath(column_stat_path, IDTFile::columnStatFileName()),
                std::min(static_cast<size_t>(DBMS_DEFAULT_BUFFER_SIZE), Poco::File(column_stat_path).getSize()));
        DMFileFormat::Version ver; // Binary version
        assertString("DTFile format: ", buf);
        DB::readText(ver, buf);
        assertString("\n", buf);
        readText(column_stats, ver, buf);
        upgradeColumnStatIfNeed(file_provider, ver);
    }
    if (read_meta_mode.needPackStat())
    {
        String pack_stat_path(path() + "/" + IDTFile::packStatFileName());
        Poco::File pack_stat_file(pack_stat_path);
        auto pack_stat_file_size = pack_stat_file.getSize();
        size_t packs = pack_stat_file_size / sizeof(PackStat);
        pack_stats.resize(packs);
        ReadBufferFromFileProvider buf(
                file_provider,
                pack_stat_path,
                EncryptionPath(pack_stat_path, IDTFile::packStatFileName()),
                std::min(static_cast<size_t>(DBMS_DEFAULT_BUFFER_SIZE), pack_stat_file_size));
        buf.readStrict((char *)pack_stats.data(), sizeof(PackStat) * packs);
    }
}

void DTFileFolderMode::remove(const FileProviderPtr &file_provider)
{
    // If we use `FileProvider::deleteDirectory`, it may left a broken DMFile on disk.
    // By renaming DMFile with a prefix first, even if there are broken DMFiles left,
    // we can safely clean them when `DMFile::listAllInPath` is called.
    const String dir_path = path();
    if (Poco::File dir_file(dir_path); dir_file.exists())
    {
        setStatus(Status::DROPPED);
        const String deleted_path = path();
        // Rename the directory first (note that we should do it before deleting encryption info)
        dir_file.renameTo(deleted_path);
        FAIL_POINT_TRIGGER_EXCEPTION(FailPoints::exception_before_dmfile_remove_encryption);
        file_provider->deleteEncryptionInfo(EncryptionPath(encryptionBasePath(), ""));
        FAIL_POINT_TRIGGER_EXCEPTION(FailPoints::exception_before_dmfile_remove_from_disk);
        // Then clean the files on disk
        dir_file.remove(true);
    }
}

String DTFileFolderMode::ngcPath() const
{
    return path() + "/" + details::NGC_FILE_NAME;
}

void DTFileFolderMode::finalize(const FileProviderPtr &file_provider, const WriteLimiterPtr &write_limiter)
{
    {
        String meta_path = path() + "/" + IDTFile::columnStatFileName();
        String tmp_meta_path = meta_path + ".tmp";

        {
            WriteBufferFromFileProvider buf(file_provider, tmp_meta_path, EncryptionPath(encryptionBasePath(), IDTFile::columnStatFileName()), false, write_limiter, 4096);
            writeString("DTFile format: ", buf);
            writeIntText(STORAGE_FORMAT_CURRENT.dm_file, buf);
            writeString("\n", buf);
            writeText(column_stats, STORAGE_FORMAT_CURRENT.dm_file, buf);
            buf.sync();
        }
    }
    {
        String property_path = path() + "/" + IDTFile::packPropertyFileName();
        String tmp_property_path = property_path + ".tmp";
        WriteBufferFromFileProvider buf(file_provider, tmp_property_path, EncryptionPath(encryptionBasePath(), IDTFile::packPropertyFileName()), false, write_limiter, 4096);
        String tmp_str;
        pack_properties.SerializeToString(&tmp_str);
        writeStringBinary(tmp_str, buf);
        buf.sync();
        Poco::File(tmp_property_path).renameTo(property_path);
    }
    if (unlikely(status != Status::WRITING))
        throw Exception("Expected WRITING status, now " + IDTFile::statusString(status));
    Poco::File old_file(path());
    setStatus(Status::READABLE);

    auto new_path = path();

    Poco::File file(new_path);
    if (file.exists())
    {
        LOG_WARNING(log, __PRETTY_FUNCTION__ << ": Existing dmfile, removing: " << new_path);
        const String deleted_path = IDTFile::getPathByStatus(parent_path, file_id, Status::DROPPED);
        // no need to delete the encryption info associated with the dmfile path here.
        // because this dmfile path is still a valid path and no obsolete encryption info will be left.
        file.renameTo(deleted_path);
        file.remove(true);
        LOG_WARNING(log, __PRETTY_FUNCTION__ << ": Existing dmfile, removed: " << deleted_path);
    }
    old_file.renameTo(new_path);
    initializeSubFileSize();
}

void DTFileFolderMode::initializeSubFileSize()
{
    Poco::File directory{path()};
    std::vector<std::string> sub_files{};
    directory.list(sub_files);
    for (const auto & name : sub_files)
    {
        if (endsWith(name, details::DATA_FILE_SUFFIX) || endsWith(name, details::INDEX_FILE_SUFFIX)
            || endsWith(name, details::MARK_FILE_SUFFIX))
        {
            auto size = Poco::File(path() + "/" + name).getSize();
            sub_file_sizes.emplace(name, size);
        }
    }
}

void DTFileFolderMode::upgradeColumnStatIfNeed(const FileProviderPtr &file_provider, DMFileFormat::Version ver)
{
    if (unlikely(ver == DMFileFormat::V0))
    {
        // Update ColumnStat.serialized_bytes
        for (auto && c : column_stats)
        {
            auto col_id = c.first;
            auto & stat = c.second;
            c.second.type->enumerateStreams(
                    [col_id, &stat, this](const IDataType::SubstreamPath & substream) {
                        String stream_name = IDTFile::getFileNameBase(col_id, substream);
                        String data_file = path() + "/" + IDTFile::colDataFileName(stream_name);
                        if (Poco::File f(data_file); f.exists())
                            stat.serialized_bytes += f.getSize();
                        String mark_file = path() + "/" + IDTFile::colMarkFileName(stream_name);
                        if (Poco::File f(mark_file); f.exists())
                            stat.serialized_bytes += f.getSize();
                        String index_file = path() + "/" + IDTFile::colIndexFileName(stream_name);
                        if (Poco::File f(index_file); f.exists())
                            stat.serialized_bytes += f.getSize();
                    },
                    {});
        }
        // write ColumnStat in new format
        String column_stat_path = path() + "/" + IDTFile::columnStatFileName();
        String tmp_column_stat_path = column_stat_path + ".tmp";
        WriteBufferFromFileProvider buf(
                file_provider,
                tmp_column_stat_path,
                EncryptionPath(encryptionBasePath(), IDTFile::columnStatFileName()),
                false, nullptr, 4096);
        writeString("DTFile format: ", buf);
        writeIntText(STORAGE_FORMAT_CURRENT.dm_file, buf);
        writeString("\n", buf);
        writeText(column_stats, STORAGE_FORMAT_CURRENT.dm_file, buf);
        buf.sync();
        Poco::File(tmp_column_stat_path).renameTo(column_stat_path);
    }
}
}
}
