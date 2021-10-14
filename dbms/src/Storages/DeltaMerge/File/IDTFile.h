#pragma once

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wunused-parameter"
#include <dmfile.pb.h>
#pragma GCC diagnostic pop

#include <Core/Types.h>
#include <IO/CompressionSettings.h>
#include <Storages/DeltaMerge/ColumnStat.h>
#include <Storages/DeltaMerge/DeltaMergeDefines.h>
#include <Storages/FormatVersion.h>
#include <common/logger_useful.h>


namespace DB
{
class FileProvider;
using FileProviderPtr = std::shared_ptr<FileProvider>;

namespace DM
{
namespace details
{
    inline constexpr static const char *DATA_FILE_SUFFIX = ".dat";
    inline constexpr static const char *INDEX_FILE_SUFFIX = ".idx";
    inline constexpr static const char *MARK_FILE_SUFFIX = ".mrk";
    inline constexpr static const char *NGC_FILE_NAME = "NGC";
    inline constexpr static const char *FOLDER_PREFIX_WRITABLE = ".tmp.dmf_";
    inline constexpr static const char *FOLDER_PREFIX_READABLE = "dmf_";
    inline constexpr static const char *FOLDER_PREFIX_DROPPED = ".del.dmf_";
}
class IDTFileWriter;
using DTFileWriterPtr = std::shared_ptr<IDTFileWriter>;
class IDTFileReader;
using DTFileReaderPtr = std::shared_ptr<IDTFileReader>;


class IDTFile;
using DTFilePtr = std::shared_ptr<IDTFile>;
using DTFiles = std::vector<DTFilePtr>;

class IDTFile : public std::enable_shared_from_this<IDTFile>
{
public:
    enum Status : int
    {
        WRITABLE,
        WRITING,
        READABLE,
        DROPPED,
    };

    static String statusString(Status status)
    {
        switch (status)
        {
            case WRITABLE:
                return "WRITABLE";
            case WRITING:
                return "WRITING";
            case READABLE:
                return "READABLE";
            case DROPPED:
                return "DROPPED";
            default:
                throw Exception("Unexpected status: " + DB::toString((int)status));
        }
    }

    // Describe which part of meta data need to be read
    struct ReadMetaMode
    {
    private:
        static constexpr size_t READ_NONE = 0x00;
        static constexpr size_t READ_COLUMN_STAT = 0x01;
        static constexpr size_t READ_PACK_STAT = 0x02;
        static constexpr size_t READ_PACK_PROPERTY = 0x04;

        size_t value;

    public:
        ReadMetaMode(size_t value_)
                : value(value_)
        {}

        static ReadMetaMode all()
        {
            return ReadMetaMode(READ_COLUMN_STAT | READ_PACK_STAT | READ_PACK_PROPERTY);
        }
        static ReadMetaMode none()
        {
            return ReadMetaMode(READ_NONE);
        }
        // after restore with mode, you can call `getBytesOnDisk` to get disk size of this DMFile
        static ReadMetaMode diskSizeOnly()
        {
            return ReadMetaMode(READ_COLUMN_STAT);
        }
        // after restore with mode, you can call `getRows`, `getBytes` to get memory size of this DMFile,
        // and call `getBytesOnDisk` to get disk size of this DMFile
        static ReadMetaMode memoryAndDiskSize()
        {
            return ReadMetaMode(READ_COLUMN_STAT | READ_PACK_STAT);
        }

        inline bool needColumnStat() const { return value & READ_COLUMN_STAT; }
        inline bool needPackStat() const { return value & READ_PACK_STAT; }
        inline bool needPackProperty() const { return value & READ_PACK_PROPERTY; }

        inline bool isNone() const { return value == READ_NONE; }
        inline bool isAll() const { return needColumnStat() && needPackStat() && needPackProperty(); }
    };

    struct PackStat
    {
        UInt32 rows;
        UInt32 not_clean;
        UInt64 first_version;
        UInt64 bytes;
        UInt8 first_tag;
    };

    using PackStats = PaddedPODArray<PackStat>;
    // `PackProperties` is similar to `PackStats` except it uses protobuf to do serialization
    using PackProperties = dtpb::PackProperties;

    enum Type : int
    {
        FOLDER,
        SINGLE_FILE,
    };

    static DTFilePtr create(UInt64 file_id, const String & parent_path, IDTFile::Type file_type=Type::FOLDER);

    static DTFilePtr restore(
            const FileProviderPtr & file_provider,
            UInt64 file_id,
            UInt64 ref_id,
            const String & parent_path,
            const ReadMetaMode & read_meta_mode);

    struct ListOptions
    {
        // Only return the DTFiles id list that can be GC
        bool only_list_can_gc = true;
        // Try to clean up temporary / dropped files
        bool clean_up = false;
    };
    static std::set<UInt64> listAllInPath(const FileProviderPtr & file_provider, const String & parent_path, const ListOptions & options);

    // static helper function for getting path
    static String getPathByStatus(const String & parent_path, UInt64 file_id, IDTFile::Status status);

    bool canGC();
    void enableGC();
    UInt64 fileId() const { return file_id; }
    UInt64 refId() const { return ref_id; }

    String encryptionBasePath() const;
    String path() const;

    const String & parentPath() const { return parent_path; }

    size_t getRows() const
    {
        size_t rows = 0;
        for (auto & s : pack_stats)
            rows += s.rows;
        return rows;
    }

    size_t getBytes() const
    {
        size_t bytes = 0;
        for (auto & s : pack_stats)
            bytes += s.bytes;
        return bytes;
    }

    size_t getBytesOnDisk() const
    {
        // This include column data & its index bytes in disk.
        // Not counting DMFile's meta and pack stat, they are usally small enough to ignore.
        size_t bytes = 0;
        for (const auto & c : column_stats)
            bytes += c.second.serialized_bytes;
        return bytes;
    }

    size_t getPackNum() const { return pack_stats.size(); }
    const PackStats & getPackStats() const { return pack_stats; }
    PackProperties & getPackProperties() { return pack_properties; }

    const ColumnStat & getColumnStat(ColId col_id) const
    {
        if (auto it = column_stats.find(col_id); likely(it != column_stats.end()))
        {
            return it->second;
        }
        throw Exception("Column [" + DB::toString(col_id) + "] not found in dm file [" + path() + "]");
    }
    bool isColumnExist(ColId col_id) const { return column_stats.find(col_id) != column_stats.end(); }

    virtual ~IDTFile() = default;

    struct WriteOptions
    {
        CompressionSettings compression_settings;
        size_t min_compress_block_size;
        size_t max_compress_block_size;

        WriteOptions() = default;

        WriteOptions(CompressionSettings compression_settings_, size_t min_compress_block_size_, size_t max_compress_block_size_)
                : compression_settings(compression_settings_)
                , min_compress_block_size(min_compress_block_size_)
                , max_compress_block_size(max_compress_block_size_)
        {
        }
    };
    virtual DTFileWriterPtr createWriter(
            const ColumnDefines & write_columns_,
             const FileProviderPtr & file_provider_,
             const WriteLimiterPtr & write_limiter_,
             const WriteOptions & options_) = 0;
    virtual DTFileReaderPtr createReader() = 0;
    virtual void readMetadata(const FileProviderPtr & file_provider, const ReadMetaMode & read_meta_mode) = 0;
    virtual void remove(const FileProviderPtr & file_provider) = 0;

    String toString()
    {
        return "{DMFile, packs: " + DB::toString(getPackNum()) + ", rows: " + DB::toString(getRows()) + ", bytes: " + DB::toString(getBytes())
               + ", file size: " + DB::toString(getBytesOnDisk()) + "}";
    }

protected:
    IDTFile(UInt64 file_id_, UInt64 ref_id_, const String & parent_path_, Status status_)
    : file_id(file_id_)
    , ref_id(ref_id_)
    , parent_path(parent_path_)
    , status(status_)
    {}

    // Do not gc me.
    virtual String ngcPath() const = 0;

    using FileNameBase = String;
    static FileNameBase getFileNameBase(ColId col_id, const IDataType::SubstreamPath & substream = {})
    {
        return IDataType::getFileNameForStream(DB::toString(col_id), substream);
    }

    static String colDataFileName(const IDTFile::FileNameBase & file_name_base);
    static String colIndexFileName(const IDTFile::FileNameBase & file_name_base);
    static String colMarkFileName(const IDTFile::FileNameBase & file_name_base);

    static String columnStatFileName() { return "meta.txt"; }
    static String packStatFileName() { return "pack"; }
    static String packPropertyFileName() { return "property"; }

    void setStatus(Status status_) { status = status_; }

protected:
    PackStats pack_stats;
    PackProperties pack_properties;
    ColumnStats column_stats;

    UInt64 file_id;
    UInt64 ref_id; // It is a reference to file_id, could be the same.
    String parent_path;
    Status status;

private:
    void addPack(const PackStat & pack_stat) { pack_stats.push_back(pack_stat); }

    Status getStatus() const { return status; }

    friend class IDTFileWriter;
    friend class DTFileFolderModeWriter;
    friend class DTFileSingleFileModeWriter;
    friend class IDTFileReader;
    friend class DTFileFolderModeReader;
    friend class DTFileSingleFileModeReader;
};

}
}

