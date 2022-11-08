// Copyright 2022 PingCAP, Ltd.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include <Common/ProfileEvents.h>
#include <Storages/Page/V3/Blob/BlobFile.h>
#include <Storages/Page/V3/Blob/BlobStat.h>

#include <boost/algorithm/string/classification.hpp>
#include <boost/algorithm/string/split.hpp>

namespace ProfileEvents
{
extern const Event PSMWritePages;
extern const Event PSMReadPages;
extern const Event PSV3MBlobExpansion;
extern const Event PSV3MBlobReused;
} // namespace ProfileEvents

namespace DB::PS::V3
{

/**********************
  * BlobStats methods *
  *********************/

BlobStats::BlobStats(LoggerPtr log_, PSDiskDelegatorPtr delegator_, BlobConfig & config_)
    : log(std::move(log_))
    , delegator(delegator_)
    , config(config_)
    , stat(std::make_shared<BlobStat>(
               1,
               static_cast<SpaceMap::SpaceMapType>(config.spacemap_type.get()),
               214748364800,
               BlobStats::BlobStatType::NORMAL))
{
    PageFileIdAndLevel id_lvl{1, 0};
    auto path = delegator->choosePath(id_lvl);
    delegator->addPageFileUsedSize({1, 0}, 0, path, true);
}

void BlobStats::restoreByEntry(const PageEntryV3 & entry)
{
    RUNTIME_CHECK_MSG(stat->id == entry.file_id, "Entry id {} not match stat id {}.", entry.file_id, stat->id);
    stat->restoreSpaceMap(entry.offset, entry.getTotalSize());
}

std::pair<BlobFileId, String> BlobStats::getBlobIdFromName(String blob_name)
{
    String err_msg;
    if (!startsWith(blob_name, BlobFile::BLOB_PREFIX_NAME))
    {
        return {INVALID_BLOBFILE_ID, err_msg};
    }

    Strings ss;
    boost::split(ss, blob_name, boost::is_any_of("_"));

    if (ss.size() != 2)
    {
        return {INVALID_BLOBFILE_ID, err_msg};
    }

    try
    {
        const auto & blob_id = std::stoull(ss[1]);
        return {blob_id, err_msg};
    }
    catch (std::invalid_argument & e)
    {
        err_msg = e.what();
    }
    catch (std::out_of_range & e)
    {
        err_msg = e.what();
    }
    return {INVALID_BLOBFILE_ID, err_msg};
}

void BlobStats::restore()
{
    stat->recalculateSpaceMap();
}

BlobStats::BlobStatPtr BlobStats::blobIdToStat(BlobFileId file_id, bool ignore_not_exist)
{
    RUNTIME_CHECK_MSG(stat->id == file_id, "Unknown file id {} not match stat id should be {}.", file_id, stat->id);
    std::ignore = file_id;
    std::ignore = ignore_not_exist;
    return stat;
}

/*********************
  * BlobStat methods *
  ********************/

BlobFileOffset BlobStats::BlobStat::getPosFromStat(size_t buf_size, const std::lock_guard<std::mutex> &)
{
    BlobFileOffset offset = 0;
    UInt64 max_cap = 0;
    bool expansion = true;

    std::tie(offset, max_cap, expansion) = smap->searchInsertOffset(buf_size);
    ProfileEvents::increment(expansion ? ProfileEvents::PSV3MBlobExpansion : ProfileEvents::PSV3MBlobReused);

    /**
     * Whatever `searchInsertOffset` success or failed,
     * Max capability still need update.
     */
    sm_max_caps = max_cap;
    if (offset != INVALID_BLOBFILE_OFFSET)
    {
        if (offset + buf_size > sm_total_size)
        {
            // This file must be expanded
            auto expand_size = buf_size - (sm_total_size - offset);
            sm_total_size += expand_size;
            sm_valid_size += buf_size;
        }
        else
        {
            /**
             * The `offset` reuses the original address. 
             * Current blob file is not expanded.
             * Only update valid size.
             */
            sm_valid_size += buf_size;
        }

        sm_valid_rate = sm_valid_size * 1.0 / sm_total_size;
    }
    return offset;
}

size_t BlobStats::BlobStat::removePosFromStat(BlobFileOffset offset, size_t buf_size, const std::lock_guard<std::mutex> &)
{
    if (!smap->markFree(offset, buf_size))
    {
        smap->logDebugString();
        throw Exception(fmt::format("Remove position from BlobStat failed, invalid position [offset={}] [buf_size={}] [blob_id={}]",
                                    offset,
                                    buf_size,
                                    id),
                        ErrorCodes::LOGICAL_ERROR);
    }

    sm_valid_size -= buf_size;
    sm_valid_rate = sm_valid_size * 1.0 / sm_total_size;
    return sm_valid_size;
}

void BlobStats::BlobStat::restoreSpaceMap(BlobFileOffset offset, size_t buf_size)
{
    if (!smap->markUsed(offset, buf_size))
    {
        smap->logDebugString();
        throw Exception(fmt::format("Restore position from BlobStat failed, the space/subspace is already being used [offset={}] [buf_size={}] [blob_id={}]",
                                    offset,
                                    buf_size,
                                    id),
                        ErrorCodes::LOGICAL_ERROR);
    }
}

void BlobStats::BlobStat::recalculateSpaceMap()
{
    const auto & [total_size, valid_size] = smap->getSizes();
    sm_total_size = total_size;
    sm_valid_size = valid_size;
    sm_valid_rate = total_size == 0 ? 0.0 : valid_size * 1.0 / total_size;
    recalculateCapacity();
}

void BlobStats::BlobStat::recalculateCapacity()
{
    sm_max_caps = smap->updateAccurateMaxCapacity();
}

} // namespace DB::PS::V3
