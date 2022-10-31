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

#include <IO/WriteBufferFromString.h>
#include <IO/WriteHelpers.h>
#include <Storages/Page/Config.h>
#include <Storages/Page/ExternalPageCallbacks.h>
#include <Storages/Page/FileUsage.h>
#include <Storages/Page/PageDefines.h>
#include <Storages/Page/Snapshot.h>
#include <Storages/Page/UniversalPage.h>
#include <Storages/Page/UniversalWriteBatch.h>
#include <Storages/Page/V3/BlobStore.h>
#include <Storages/Page/V3/PageDirectory.h>
#include <Storages/Transaction/TiKVRecordFormat.h>
#include <common/defines.h>

namespace DB
{
class FileProvider;
using FileProviderPtr = std::shared_ptr<FileProvider>;
class PathCapacityMetrics;
using PathCapacityMetricsPtr = std::shared_ptr<PathCapacityMetrics>;
class PSDiskDelegator;
using PSDiskDelegatorPtr = std::shared_ptr<PSDiskDelegator>;
class Context;
class WriteLimiter;
using WriteLimiterPtr = std::shared_ptr<WriteLimiter>;
class ReadLimiter;
using ReadLimiterPtr = std::shared_ptr<ReadLimiter>;

class UniversalPageStorage;
using UniversalPageStoragePtr = std::shared_ptr<UniversalPageStorage>;

class KVStoreReader;

class UniversalPageStorage final
{
public:
    using SnapshotPtr = PageStorageSnapshotPtr;

public:
    static UniversalPageStoragePtr
    create(
        String name,
        PSDiskDelegatorPtr delegator,
        const PageStorageConfig & config,
        const FileProviderPtr & file_provider);

    UniversalPageStorage(
        String name,
        PSDiskDelegatorPtr delegator_,
        const PageStorageConfig & config_,
        const FileProviderPtr & file_provider_)
        : storage_name(std::move(name))
        , delegator(std::move(delegator_))
        , config(config_)
        , file_provider(file_provider_)
    {
    }

    enum class GCStageType
    {
        Unknown,
        OnlyInMem,
        FullGCNothingMoved,
        FullGC,
    };

    struct GCTimeStatistics
    {
        GCStageType stage = GCStageType::Unknown;
        bool executeNextImmediately() const { return stage == GCStageType::FullGC; };

        UInt64 total_cost_ms = 0;

        UInt64 dump_snapshots_ms = 0;
        UInt64 gc_in_mem_entries_ms = 0;
        UInt64 blobstore_remove_entries_ms = 0;
        UInt64 blobstore_get_gc_stats_ms = 0;
        // Full GC
        UInt64 full_gc_get_entries_ms = 0;
        UInt64 full_gc_blobstore_copy_ms = 0;
        UInt64 full_gc_apply_ms = 0;

        // GC external page
        UInt64 clean_external_page_ms = 0;
        UInt64 num_external_callbacks = 0;
        // ms is usually too big for these operation, store by ns (10^-9)
        UInt64 external_page_scan_ns = 0;
        UInt64 external_page_get_alive_ns = 0;
        UInt64 external_page_remove_ns = 0;

        String toLogging() const;
    };

    ~UniversalPageStorage() = default;

    void restore();

    SnapshotPtr getSnapshot(const String & tracing_id) const
    {
        return page_directory->createSnapshot(tracing_id);
    }

    // Get some statistics of all living snapshots and the oldest living snapshot.
    SnapshotsStatistics getSnapshotsStat() const
    {
        return page_directory->getSnapshotsStat();
    }

    FileUsageStatistics getFileUsageStatistics() const
    {
        return blob_store->getFileUsageStatistics();
    }

    void write(UniversalWriteBatch && write_batch, const WriteLimiterPtr & write_limiter = nullptr) const;

    UniversalPageMap read(const UniversalPageId & page_ids, const ReadLimiterPtr & read_limiter = nullptr, SnapshotPtr snapshot = {})
    {
        UNUSED(page_ids, read_limiter, snapshot);
        throw Exception("", ErrorCodes::NOT_IMPLEMENTED);
    }

    using FieldIndices = std::vector<size_t>;
    using PageReadFields = std::pair<UniversalPageId, FieldIndices>;

    UniversalPageMap read(const std::vector<PageReadFields> & page_fields, const ReadLimiterPtr & read_limiter = nullptr, SnapshotPtr snapshot = {})
    {
        UNUSED(page_fields, read_limiter, snapshot);
        throw Exception("", ErrorCodes::NOT_IMPLEMENTED);
    }

    UniversalPage read(const PageReadFields & page_field, const ReadLimiterPtr & read_limiter = nullptr, SnapshotPtr snapshot = {})
    {
        UNUSED(page_field, read_limiter, snapshot);
        throw Exception("", ErrorCodes::NOT_IMPLEMENTED);
    }

    // We may skip the GC to reduce useless reading by default.
    bool gc(bool not_skip = false, const WriteLimiterPtr & write_limiter = nullptr, const ReadLimiterPtr & read_limiter = nullptr)
    {
        // If another thread is running gc, just return;
        bool v = false;
        if (!gc_is_running.compare_exchange_strong(v, true))
            return false;

        const GCTimeStatistics statistics = doGC(write_limiter, read_limiter);
        assert(statistics.stage != GCStageType::Unknown); // `doGC` must set the stage
        LOG_DEBUG(log, statistics.toLogging());

        return statistics.executeNextImmediately();
    }

    GCTimeStatistics doGC(const WriteLimiterPtr & write_limiter, const ReadLimiterPtr & read_limiter);

    // Register and unregister external pages GC callbacks
    // Note that user must ensure that it is safe to call `scanner` and `remover` even after unregister.
    void registerExternalPagesCallbacks(const ExternalPageCallbacks & callbacks) { UNUSED(callbacks); }
    void unregisterExternalPagesCallbacks(NamespaceId /*ns_id*/) {}

    String storage_name; // Identify between different Storage
    PSDiskDelegatorPtr delegator; // Get paths for storing data
    PageStorageConfig config;
    FileProviderPtr file_provider;

    PS::V3::universal::PageDirectoryPtr page_directory;
    PS::V3::universal::BlobStorePtr blob_store;

    std::atomic<bool> gc_is_running = false;
};

class KVStoreReader final
{
public:
    explicit KVStoreReader(UniversalPageStorage & storage)
        : uni_storage(storage)
    {}

    static UniversalPageId toFullPageId(PageId page_id)
    {
        // TODO: Does it need to be mem comparable?
        return fmt::format("k_{}", page_id);
    }

    bool isAppliedIndexExceed(PageId page_id, UInt64 applied_index)
    {
        // assume use this format
        UniversalPageId uni_page_id = toFullPageId(page_id);
        auto snap = uni_storage.getSnapshot("");
        const auto & [id, entry] = uni_storage.page_directory->getByIDOrNull(uni_page_id, snap);
        return (entry.isValid() && entry.tag > applied_index);
    }

    void read(const PageIds & page_ids, std::function<void(const PageId &, const UniversalPage &)> handler) const
    {
        UniversalPageIds uni_page_ids;
        uni_page_ids.reserve(page_ids.size());
        for (const auto & pid : page_ids)
            uni_page_ids.emplace_back(toFullPageId(pid));
        auto snap = uni_storage.getSnapshot("");
        auto page_entries = uni_storage.page_directory->getByIDs(uni_page_ids, snap);
        uni_storage.blob_store->read(page_entries, handler, nullptr);
    }

    void traverse(const std::function<void(const DB::UniversalPage & page)> & /*acceptor*/)
    {
        // Only traverse pages with id prefix
        throw Exception("", ErrorCodes::NOT_IMPLEMENTED);
    }

private:
    UniversalPageStorage & uni_storage;
};

class RaftLogReader final
{
public:
    explicit RaftLogReader(UniversalPageStorage & storage)
        : uni_storage(storage)
    {}

    static UniversalPageId toFullPageId(NamespaceId ns_id, PageId page_id)
    {
        // TODO: Does it need to be mem comparable?
        WriteBufferFromOwnString buff;
        writeString("r_", buff);
        RecordKVFormat::encodeUInt64(ns_id, buff);
        writeString("_", buff);
        RecordKVFormat::encodeUInt64(page_id, buff);
        return buff.releaseStr();
        // return fmt::format("r_{}_{}", ns_id, page_id);
    }

    // scan the pages between [start, end)
    void traverse(const UniversalPageId & start, const UniversalPageId & end, const std::function<void(const DB::UniversalPage & page)> & acceptor)
    {
        // always traverse with the latest snapshot
        auto snapshot = uni_storage.getSnapshot(fmt::format("scan_r_{}_{}", start, end));
        const auto page_ids = uni_storage.page_directory->getRangePageIds(start, end);
        for (const auto & page_id : page_ids)
        {
            const auto page_id_and_entry = uni_storage.page_directory->getByID(page_id, snapshot);
            acceptor(uni_storage.blob_store->read(page_id_and_entry));
        }
    }

    void traverse2(const UniversalPageId & start, const UniversalPageId & end, const std::function<void(DB::UniversalPage page)> & acceptor)
    {
        // always traverse with the latest snapshot
        auto snapshot = uni_storage.getSnapshot(fmt::format("scan_r_{}_{}", start, end));
        const auto page_ids = uni_storage.page_directory->getRangePageIds(start, end);
        for (const auto & page_id : page_ids)
        {
            const auto page_id_and_entry = uni_storage.page_directory->getByID(page_id, snapshot);
            acceptor(uni_storage.blob_store->read(page_id_and_entry));
        }
    }

    UniversalPage read(const UniversalPageId & page_id)
    {
        // always traverse with the latest snapshot
        auto snapshot = uni_storage.getSnapshot(fmt::format("read_{}", page_id));
        const auto page_id_and_entry = uni_storage.page_directory->getByIDOrNull(page_id, snapshot);
        if (page_id_and_entry.second.isValid())
        {
            return uni_storage.blob_store->read(page_id_and_entry);
        }
        else
        {
            return UniversalPage({});
        }
    }

private:
    UniversalPageStorage & uni_storage;
};

} // namespace DB
