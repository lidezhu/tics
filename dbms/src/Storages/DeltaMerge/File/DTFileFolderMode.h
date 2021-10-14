#pragma once

#include <Storages/DeltaMerge/File/IDTFile.h>

namespace DB
{
namespace DM
{
class DTFileFolderMode : public IDTFile, private boost::noncopyable
{
public:
    static DTFilePtr create(UInt64 file_id, const String & parent_path);

    static DTFilePtr restore(
            const FileProviderPtr & file_provider,
            UInt64 file_id,
            UInt64 ref_id,
            const String & parent_path,
            const ReadMetaMode & read_meta_mode);

    DTFileWriterPtr createWriter(const ColumnDefines & write_columns_,
                                 const FileProviderPtr & file_provider_,
                                 const WriteLimiterPtr & write_limiter_,
                                 const WriteOptions & options_) override;
    DTFileReaderPtr createReader() override;
    void readMetadata(const FileProviderPtr & file_provider, const ReadMetaMode & read_meta_mode) override;
    void remove(const FileProviderPtr & file_provider) override;

protected:
    DTFileFolderMode(UInt64 file_id_, UInt64 ref_id_, const String & parent_path_, Status status_, Poco::Logger * log_)
    : IDTFile(file_id_, ref_id_, parent_path_, status_)
    , log(log_)
    {}

    // Do not gc me.
    virtual String ngcPath() const override;

private:
    void finalize(const FileProviderPtr & file_provider, const WriteLimiterPtr & write_limiter);

    void initializeSubFileSize();

    void upgradeColumnStatIfNeed(const FileProviderPtr & file_provider, DMFileFormat::Version ver);

    friend class DTFileFolderModeWriter;

private:
    using SubFileSizes = std::unordered_map<String, UInt64>;
    SubFileSizes sub_file_sizes;

    Poco::Logger * log;
};
}
}


