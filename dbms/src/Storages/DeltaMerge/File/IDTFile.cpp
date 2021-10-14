#include <boost/algorithm/string/classification.hpp>
#include <boost/algorithm/string/split.hpp>

#include <Encryption/FileProvider.h>
#include <Storages/DeltaMerge/File/IDTFile.h>
#include <Poco/File.h>



namespace DB
{
namespace DM
{
namespace details
{
inline String getNGCPath(const String & file_path)
{
    Poco::File file(file_path);
    return file_path + (file.isFile() ? "." : "/") + NGC_FILE_NAME;
}
} // namespace details

DTFilePtr
IDTFile::create(UInt64 file_id, const String &parent_path, DB::DM::IDTFile::Type file_type)
{
    return DB::DM::DTFilePtr();
}

DTFilePtr
IDTFile::restore(const FileProviderPtr &file_provider, UInt64 file_id, UInt64 ref_id, const String &parent_path,
                 const IDTFile::ReadMetaMode &read_meta_mode)
{
//    String path = getPathByStatus(parent_path, file_id, IDTFile::Status::READABLE);
//    if (Poco::File(path).isFile())
//    {
//
//    }
//    else
//    {
//
//    }
//    bool single_file_mode = ;
//    DMFilePtr dmfile(new DMFile(
//            file_id,
//            ref_id,
//            parent_path,
//            single_file_mode ? Mode::SINGLE_FILE : Mode::FOLDER,
//            Status::READABLE,
//            &Poco::Logger::get("DMFile")));
//    if (!read_meta_mode.isNone())
//        dmfile->readMetadata(file_provider, read_meta_mode);
//    return dmfile;
}

std::set<UInt64> IDTFile::listAllInPath(const FileProviderPtr &file_provider, const String &parent_path,
                                        const IDTFile::ListOptions &options)
{
    Poco::File folder(parent_path);
    if (!folder.exists())
        return {};
    std::vector<std::string> file_names;
    folder.list(file_names);
    std::set<UInt64> file_ids;
    Poco::Logger * log = &Poco::Logger::get("DMFile");

    auto try_parse_file_id = [](const String & name) -> std::optional<UInt64> {
        std::vector<std::string> ss;
        boost::split(ss, name, boost::is_any_of("_"));
        if (ss.size() != 2)
            return std::nullopt;
        size_t pos;
        auto id = std::stoull(ss[1], &pos);
        // pos < ss[1].size() means that ss[1] is not an invalid integer
        return pos < ss[1].size() ? std::nullopt : std::make_optional(id);
    };

    for (const auto & name : file_names)
    {
        // Clean up temporary files and files should be deleted
        // Note that you should not do clean up if some DTFiles are writing,
        // or you may delete some writing files
        if (options.clean_up)
        {
            if (startsWith(name, details::FOLDER_PREFIX_WRITABLE))
            {
                // Clear temporary files
                const auto full_path = parent_path + "/" + name;
                if (Poco::File temp_file(full_path); temp_file.exists())
                    temp_file.remove(true);
                LOG_WARNING(log, __PRETTY_FUNCTION__ << ": Existing temporary dmfile, removed: " << full_path);
                continue;
            }
            else if (startsWith(name, details::FOLDER_PREFIX_DROPPED))
            {
                // Clear deleted (maybe broken) DTFiles
                auto res = try_parse_file_id(name);
                if (!res)
                {
                    LOG_INFO(log, "Unrecognized dropped DM file, ignored: " + name);
                    continue;
                }
                UInt64 file_id = *res;
                // The encryption info use readable path. We are not sure the encryption info is deleted or not.
                // Try to delete and ignore if it is already deleted.
                const String readable_path = getPathByStatus(parent_path, file_id, IDTFile::Status::READABLE);
                file_provider->deleteEncryptionInfo(EncryptionPath(readable_path, ""), /* throw_on_error= */ false);
                const auto full_path = parent_path + "/" + name;
                if (Poco::File del_file(full_path); del_file.exists())
                    del_file.remove(true);
                LOG_WARNING(log, __PRETTY_FUNCTION__ << ": Existing dropped dmfile, removed: " << full_path);
                continue;
            }
        }

        if (!startsWith(name, details::FOLDER_PREFIX_READABLE))
            continue;
        // For DTFileSingleFileMode, ngc file will appear in the same level of directory with date file. Just ignore it.
        if (endsWith(name, details::NGC_FILE_NAME))
            continue;
        auto res = try_parse_file_id(name);
        if (!res)
        {
            LOG_INFO(log, "Unrecognized DM file, ignored: " + name);
            continue;
        }
        UInt64 file_id = *res;

        if (options.only_list_can_gc)
        {
            // Only return the ID if the file is able to be GC-ed.
            String ngc_path = details::getNGCPath(parent_path + "/" + name);
            Poco::File ngc_file(ngc_path);
            if (!ngc_file.exists())
                file_ids.insert(file_id);
        }
        else
        {
            file_ids.insert(file_id);
        }
    }
    return file_ids;
}

String IDTFile::getPathByStatus(const String &parent_path, UInt64 file_id, IDTFile::Status status)
{
    String s = parent_path + "/";
    switch (status)
    {
        case IDTFile::Status::READABLE:
            s += details::FOLDER_PREFIX_READABLE;
            break;
        case IDTFile::Status::WRITABLE:
        case IDTFile::Status::WRITING:
            s += details::FOLDER_PREFIX_WRITABLE;
            break;
        case IDTFile::Status::DROPPED:
            s += details::FOLDER_PREFIX_DROPPED;
            break;
    }
    s += DB::toString(file_id);
    return s;
}

bool IDTFile::canGC()
{
    return !Poco::File(ngcPath()).exists();
}

void IDTFile::enableGC()
{
    Poco::File ngc_file(ngcPath());
    if (ngc_file.exists())
        ngc_file.remove();
}

String IDTFile::encryptionBasePath() const
{
    return IDTFile::getPathByStatus(parent_path, file_id, IDTFile::Status::READABLE);
}

String IDTFile::path() const
{
    return IDTFile::getPathByStatus(parent_path, file_id, status);
}

String IDTFile::colDataFileName(const IDTFile::FileNameBase & file_name_base)
{
    return file_name_base + details::DATA_FILE_SUFFIX;
}
String IDTFile::colIndexFileName(const IDTFile::FileNameBase & file_name_base)
{
    return file_name_base + details::INDEX_FILE_SUFFIX;
}
String IDTFile::colMarkFileName(const IDTFile::FileNameBase & file_name_base)
{
    return file_name_base + details::MARK_FILE_SUFFIX;
}
}
}
