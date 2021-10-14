#pragma once

#include <DataStreams/IBlockOutputStream.h>
#include <DataStreams/MarkInCompressedFile.h>
#include <Encryption/WriteBufferFromFileProvider.h>
#include <Encryption/createWriteBufferFromFileBaseByFileProvider.h>
#include <IO/CompressedWriteBuffer.h>
#include <IO/HashingWriteBuffer.h>
#include <IO/WriteBufferFromOStream.h>
#include <IO/WriteBufferProxy.h>
#include <Storages/DeltaMerge/File/IDTFile.h>
#include <Storages/DeltaMerge/Index/MinMaxIndex.h>

namespace DB
{
namespace DM
{
namespace details
{
inline bool isColumnSupportIndex(const ColumnDefine & cd)
{
    // TODO: currently we only generate index for Integers, Date, DateTime types, and this should be configurable by user.
    // TODO: If column type is nullable, we won't generate index for it
    /// for handle column always generate index
    return cd.id == EXTRA_HANDLE_COLUMN_ID || cd.type->isInteger() || cd.type->isDateOrDateTime();
}
}

class IDTFileWriter
{
public:
    struct BlockProperty
    {
        size_t not_clean_rows;
        size_t effective_num_rows;
        size_t gc_hint_version;
    };

public:
    virtual ~IDTFileWriter() =default;
    virtual void write(const Block & block, const BlockProperty & block_property) =0;
    virtual void finalize() =0;

    DTFilePtr getFile() const { return dtfile; }

protected:
    IDTFileWriter(const DTFilePtr & dtfile_,
                  const ColumnDefines & write_columns_,
                  const FileProviderPtr & file_provider_,
                  const WriteLimiterPtr & write_limiter_,
                  const IDTFile::WriteOptions & options_);

protected:
    DTFilePtr dtfile;
    ColumnDefines write_columns;
    IDTFile::WriteOptions options;

    FileProviderPtr file_provider;
    WriteLimiterPtr write_limiter;
};
}
}
