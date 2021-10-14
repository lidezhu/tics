#pragma once

#include <Storages/DeltaMerge/File/IDTFile.h>


namespace DB
{
namespace DM
{
class DTFileSingleFileMode : public IDTFile, private boost::noncopyable
{

};
}
}


