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

#pragma once

#include <Common/Logger.h>
#include <Storages/Page/Page.h>
#include <Storages/Page/PageDefines.h>
#include <Storages/Page/V3/Blob/BlobConfig.h>
#include <Storages/Page/V3/PageEntry.h>
#include <Storages/Page/V3/spacemap/SpaceMap.h>
#include <Storages/PathPool.h>
#include <common/types.h>

namespace DB::PS::V3
{

class BlobStats
{
public:
    enum BlobStatType
    {
        NORMAL = 1,

        // Read Only.
        // Only after heavy GC, BlobFile will change to READ_ONLY type.
        // After GC remove, empty files will be removed.
        READ_ONLY = 2
    };

    static String blobTypeToString(BlobStatType type)
    {
        switch (type)
        {
        case BlobStatType::NORMAL:
            return "normal";
        case BlobStatType::READ_ONLY:
            return "read only";
        }
        return "Invalid";
    }

    struct BlobStat
    {
        String parent_path;

        const BlobFileId id;
        std::atomic<BlobStatType> type;

        std::mutex sm_lock;
        const SpaceMapPtr smap;

        // The max capacity hint of all available slots in SpaceMap
        // A hint means that it is not an absolutely accurate value after inserting data,
        // but is useful for quickly choosing BlobFile.
        // Should call `recalculateCapacity` to get an accurate value after removing data.
        UInt64 sm_max_caps = 0;
        // The current file size of the BlobFile
        UInt64 sm_total_size = 0;
        // The sum of the size of all valid data in the BlobFile
        UInt64 sm_valid_size = 0;
        // sm_valid_size / sm_total_size
        double sm_valid_rate = 0.0;

    public:
        BlobStat(BlobFileId id_, SpaceMap::SpaceMapType sm_type, UInt64 sm_max_caps_, BlobStatType type_)
            : id(id_)
            , type(type_)
            , smap(SpaceMap::createSpaceMap(sm_type, 0, sm_max_caps_))
            , sm_max_caps(sm_max_caps_)
        {}

        [[nodiscard]] std::lock_guard<std::mutex> lock()
        {
            return std::lock_guard(sm_lock);
        }

        bool isNormal() const
        {
            return type.load() == BlobStatType::NORMAL;
        }

        bool isReadOnly() const
        {
            return type.load() == BlobStatType::READ_ONLY;
        }

        void changeToReadOnly()
        {
            type.store(BlobStatType::READ_ONLY);
        }

        BlobFileOffset getPosFromStat(size_t buf_size, const std::lock_guard<std::mutex> &);

        /**
             * The return value is the valid data size remained in the BlobFile after the remove
             */
        size_t removePosFromStat(BlobFileOffset offset, size_t buf_size, const std::lock_guard<std::mutex> &);

        /**
             * This method is only used when blobstore restore
             * Restore space map won't change the `sm_total_size`/`sm_valid_size`/`sm_valid_rate`
             */
        void restoreSpaceMap(BlobFileOffset offset, size_t buf_size);

        /**
             * After we restore the space map.
             * We still need to recalculate a `sm_total_size`/`sm_valid_size`/`sm_valid_rate`.
             */
        void recalculateSpaceMap();

        /**
             * The `sm_max_cap` is not accurate after GC removes out-of-date data, or after restoring from disk.
             * Caller should call this function to update the `sm_max_cap` so that we can reuse the space in this BlobStat.
             */
        void recalculateCapacity();
    };

    using BlobStatPtr = std::shared_ptr<BlobStat>;

public:
    BlobStats(LoggerPtr log_, PSDiskDelegatorPtr delegator_, BlobConfig & config);

    BlobStatPtr getStat() const { return stat; };

    static std::pair<BlobFileId, String> getBlobIdFromName(String blob_name);

    BlobStatPtr blobIdToStat(BlobFileId file_id, bool ignore_not_exist = false);

#ifndef DBMS_PUBLIC_GTEST
private:
#endif
    void restoreByEntry(const PageEntryV3 & entry);
    void restore();
    template <typename>
    friend class PageDirectoryFactory;

#ifndef DBMS_PUBLIC_GTEST
private:
#endif
    LoggerPtr log;
    PSDiskDelegatorPtr delegator;
    BlobConfig & config;

    BlobStatPtr stat;
};

} // namespace DB::PS::V3
