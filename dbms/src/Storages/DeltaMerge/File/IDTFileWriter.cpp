#include <Storages/DeltaMerge/File/IDTFileWriter.h>

namespace DB
{
namespace DM
{
IDTFileWriter::IDTFileWriter(
    const DB::DM::DTFilePtr &dtfile_,
    const DB::DM::ColumnDefines &write_columns_,
    const DB::FileProviderPtr &file_provider_,
    const DB::WriteLimiterPtr &write_limiter_,
    const DB::DM::IDTFileWriter::Options &options_)
        : dtfile(dtfile_)
        , write_columns(write_columns_)
        , options(options_)
        , file_provider(file_provider_)
        , write_limiter(write_limiter_)
{
    dtfile->setStatus(IDTFile::Status::WRITING);
}
}
}
