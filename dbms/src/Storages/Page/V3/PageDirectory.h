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

#include <Common/CurrentMetrics.h>
#include <Common/Logger.h>
#include <Common/nocopyable.h>
#include <Encryption/FileProvider.h>
#include <Poco/Ext/ThreadNumber.h>
#include <Storages/Page/Page.h>
#include <Storages/Page/PageDefines.h>
#include <Storages/Page/Snapshot.h>
#include <Storages/Page/V3/BlobStore.h>
#include <Storages/Page/V3/MapUtils.h>
#include <Storages/Page/V3/PageDirectory/ExternalIdsByNamespace.h>
#include <Storages/Page/V3/PageEntriesEdit.h>
#include <Storages/Page/V3/PageEntry.h>
#include <Storages/Page/V3/WAL/serialize.h>
#include <Storages/Page/V3/WALStore.h>
#include <common/defines.h>
#include <common/types.h>

#include <memory>
#include <mutex>
#include <shared_mutex>
#include <unordered_map>

namespace CurrentMetrics
{
extern const Metric PSMVCCNumSnapshots;
} // namespace CurrentMetrics

namespace DB::PS::V3
{

class PageDirectorySnapshot : public DB::PageStorageSnapshot
{
public:
    using TimePoint = std::chrono::time_point<std::chrono::steady_clock>;

    explicit PageDirectorySnapshot(UInt64 seq, const String & tracing_id_)
        : sequence(seq)
        , create_thread(Poco::ThreadNumber::get())
        , tracing_id(tracing_id_)
        , create_time(std::chrono::steady_clock::now())
    {
        CurrentMetrics::add(CurrentMetrics::PSMVCCNumSnapshots);
    }

    ~PageDirectorySnapshot() override
    {
        CurrentMetrics::sub(CurrentMetrics::PSMVCCNumSnapshots);
    }

    double elapsedSeconds() const
    {
        auto end = std::chrono::steady_clock::now();
        std::chrono::duration<double> diff = end - create_time;
        return diff.count();
    }

public:
    const UInt64 sequence;
    const unsigned create_thread;
    const String tracing_id;

private:
    const TimePoint create_time;
};
using PageDirectorySnapshotPtr = std::shared_ptr<PageDirectorySnapshot>;

struct EntryOrDelete
{
    bool is_delete = true;
    Int64 being_ref_count = 1;
    PageEntryV3 entry;

    static EntryOrDelete newDelete()
    {
        return EntryOrDelete{
            .is_delete = true,
            .being_ref_count = 1, // meaningless
            .entry = {}, // meaningless
        };
    }
    static EntryOrDelete newNormalEntry(const PageEntryV3 & entry)
    {
        return EntryOrDelete{
            .is_delete = false,
            .being_ref_count = 1,
            .entry = entry,
        };
    }
    static EntryOrDelete newReplacingEntry(const EntryOrDelete & ori_entry, const PageEntryV3 & entry)
    {
        return EntryOrDelete{
            .is_delete = false,
            .being_ref_count = ori_entry.being_ref_count,
            .entry = entry,
        };
    }

    static EntryOrDelete newFromRestored(PageEntryV3 entry, Int64 being_ref_count)
    {
        return EntryOrDelete{
            .is_delete = false,
            .being_ref_count = being_ref_count,
            .entry = entry,
        };
    }

    bool isDelete() const { return is_delete; }
    bool isEntry() const { return !is_delete; }

    String toDebugString() const
    {
        return fmt::format(
            "{{is_delete:{}, entry:{}, being_ref_count:{}}}",
            is_delete,
            ::DB::PS::V3::toDebugString(entry),
            being_ref_count);
    }
};

using PageLock = std::lock_guard<std::mutex>;

enum class ResolveResult
{
    FAIL,
    TO_REF,
    TO_NORMAL,
};

template <typename Trait>
class VersionedPageEntries
{
public:
    VersionedPageEntries()
        : type(EditRecordType::VAR_DELETE)
        , is_deleted(false)
        , create_ver(0)
        , delete_ver(0)
        , ori_page_id{}
        , being_ref_count(1)
    {}

    [[nodiscard]] PageLock acquireLock() const
    {
        return std::lock_guard(m);
    }

    void createNewEntry(const PageVersion & ver, const PageEntryV3 & entry);

    bool createNewRef(const PageVersion & ver, const typename Trait::PageId & ori_page_id);

    typename Trait::PageIdSharedPtr createNewExternal(const PageVersion & ver);

    void createDelete(const PageVersion & ver);

    typename Trait::PageIdSharedPtr fromRestored(const typename Trait::EditRecord & rec);

    std::tuple<ResolveResult, typename Trait::PageId, PageVersion>
    resolveToPageId(UInt64 seq, bool ignore_delete, PageEntryV3 * entry);

    Int64 incrRefCount(const PageVersion & ver);

    std::optional<PageEntryV3> getEntry(UInt64 seq) const;

    std::optional<PageEntryV3> getLastEntry() const;

    bool isVisible(UInt64 seq) const;

    /**
     * If there are entries point to file in `blob_ids`, take out the <page_id, ver, entry> and
     * store them into `blob_versioned_entries`.
     * Return the total size of entries in this version list.
     */
    PageSize getEntriesByBlobIds(
        const std::unordered_set<BlobFileId> & blob_ids,
        const typename Trait::PageId & page_id,
        typename Trait::GcEntriesMap & blob_versioned_entries);

    /**
     * Given a `lowest_seq`, this will clean all outdated entries before `lowest_seq`.
     * It takes good care of the entries being ref by another page id.
     *
     * `normal_entries_to_deref`: Return the informations that the entries need
     *   to be decreased the ref count by `derefAndClean`.
     *   The elem is <page_id, <version, num to decrease ref count>> 
     * `entries_removed`: Return the entries removed from the version list
     *
     * Return `true` iff this page can be totally removed from the whole `PageDirectory`.
     */
    bool cleanOutdatedEntries(
        UInt64 lowest_seq,
        typename Trait::EntriesDerefMap * normal_entries_to_deref,
        PageEntriesV3 * entries_removed,
        const PageLock & page_lock);
    bool derefAndClean(
        UInt64 lowest_seq,
        const typename Trait::PageId & page_id,
        const PageVersion & deref_ver,
        Int64 deref_count,
        PageEntriesV3 * entries_removed);

    void collapseTo(UInt64 seq, const typename Trait::PageId & page_id, typename Trait::PageEntriesEdit & edit);

    size_t size() const
    {
        auto lock = acquireLock();
        return entries.size();
    }

    String toDebugString() const
    {
        return fmt::format(
            "{{"
            "type:{}, create_ver: {}, is_deleted: {}, delete_ver: {}, "
            "ori_page_id: {}, being_ref_count: {}, num_entries: {}"
            "}}",
            type,
            create_ver,
            is_deleted,
            delete_ver,
            ori_page_id,
            being_ref_count,
            entries.size());
    }
    friend class PageStorageControlV3;

private:
    using ExternalIdTrait = typename Trait::ExternalIdTrait;

private:
    mutable std::mutex m;

    // Valid value of `type` is one of
    // - VAR_DELETE
    // - VAR_ENTRY
    // - VAR_REF
    // - VAR_EXTERNAL
    EditRecordType type;

    // Has been deleted, valid when type == VAR_REF/VAR_EXTERNAL
    bool is_deleted;
    // Entries sorted by version, valid when type == VAR_ENTRY
    std::multimap<PageVersion, EntryOrDelete> entries;
    // The created version, valid when type == VAR_REF/VAR_EXTERNAL
    PageVersion create_ver;
    // The deleted version, valid when type == VAR_REF/VAR_EXTERNAL && is_deleted = true
    PageVersion delete_ver;
    // Original page id, valid when type == VAR_REF
    typename Trait::PageId ori_page_id;
    // Being ref counter, valid when type == VAR_EXTERNAL
    Int64 being_ref_count;
    // A shared ptr to a holder, valid when type == VAR_EXTERNAL
    typename Trait::PageIdSharedPtr external_holder;
};

// `PageDirectory` store multi-versions entries for the same
// page id. User can acquire a snapshot from it and get a
// consist result by the snapshot.
// All its functions are consider concurrent safe.
// User should call `gc` periodic to remove outdated version
// of entries in order to keep the memory consumption as well
// as the restoring time in a reasonable level.
template <typename Trait>
class PageDirectory
{
public:
    explicit PageDirectory(String storage_name, WALStorePtr && wal, UInt64 max_persisted_log_files_ = MAX_PERSISTED_LOG_FILES);

    PageDirectorySnapshotPtr createSnapshot(const String & tracing_id = "") const;

    SnapshotsStatistics getSnapshotsStat() const;

    typename Trait::PageIdAndEntry getByID(const typename Trait::PageId & page_id, const DB::PageStorageSnapshotPtr & snap) const
    {
        return getByIDImpl(page_id, toConcreteSnapshot(snap), /*throw_on_not_exist=*/true);
    }
    typename Trait::PageIdAndEntry getByIDOrNull(const typename Trait::PageId & page_id, const DB::PageStorageSnapshotPtr & snap) const
    {
        return getByIDImpl(page_id, toConcreteSnapshot(snap), /*throw_on_not_exist=*/false);
    }

    typename Trait::PageIdAndEntries getByIDs(const typename Trait::PageIds & page_ids, const DB::PageStorageSnapshotPtr & snap) const
    {
        return std::get<0>(getByIDsImpl(page_ids, toConcreteSnapshot(snap), /*throw_on_not_exist=*/true));
    }
    typename Trait::PageIdAndEntriesWithError getByIDsOrNull(const typename Trait::PageIds & page_ids, const DB::PageStorageSnapshotPtr & snap) const
    {
        return getByIDsImpl(page_ids, toConcreteSnapshot(snap), /*throw_on_not_exist=*/false);
    }

    typename Trait::PageId getNormalPageId(const typename Trait::PageId & page_id, const DB::PageStorageSnapshotPtr & snap, bool throw_on_not_exist) const;

    PageId getMaxId() const;

    typename Trait::PageIdSet getAllPageIds();

    typename Trait::PageIdSet getRangePageIds(const typename Trait::PageId & start, const typename Trait::PageId & end);

    void apply(typename Trait::PageEntriesEdit && edit, const WriteLimiterPtr & write_limiter = nullptr);

    std::pair<typename Trait::GcEntriesMap, PageSize>
    getEntriesByBlobIds(const std::vector<BlobFileId> & blob_ids) const;

    void gcApply(typename Trait::PageEntriesEdit && migrated_edit, const WriteLimiterPtr & write_limiter = nullptr);

    /// When create PageDirectory for dump snapshot, we should keep the last valid var_entry when it is deleted.
    /// Because there may be some upsert entry in later wal files, and we should keep the valid var_entry and the delete entry to delete the later upsert entry.
    /// And we don't restore the entries in blob store, because this PageDirectory is just read only for its entries.
    bool tryDumpSnapshot(const ReadLimiterPtr & read_limiter = nullptr, const WriteLimiterPtr & write_limiter = nullptr, bool force = false);

    // Perform a GC for in-memory entries and return the removed entries.
    // If `return_removed_entries` is false, then just return an empty set.
    PageEntriesV3 gcInMemEntries(bool return_removed_entries = true);

private:
    using ExternalIdTrait = typename Trait::ExternalIdTrait;

public:
    // Get the external id that is not deleted or being ref by another id by
    // `ns_id`.
    std::set<DB::PageId> getAliveExternalIds(const typename ExternalIdTrait::Prefix & ns_id) const
    {
        return external_ids_by_ns.getAliveIds(ns_id);
    }

    // After table dropped, the `getAliveIds` with specified
    // `ns_id` will not be cleaned. We need this method to
    // cleanup all external id ptrs.
    void unregisterNamespace(const typename ExternalIdTrait::Prefix & ns_id)
    {
        external_ids_by_ns.unregisterNamespace(ns_id);
    }

    typename Trait::PageEntriesEdit dumpSnapshotToEdit(PageDirectorySnapshotPtr snap = nullptr);

    size_t numPages() const
    {
        std::shared_lock read_lock(table_rw_mutex);
        return mvcc_table_directory.size();
    }

    // No copying and no moving
    DISALLOW_COPY_AND_MOVE(PageDirectory);

    template <typename>
    friend class PageDirectoryFactory;
    friend class PageStorageControlV3;

private:
    typename Trait::PageIdAndEntry
    getByIDImpl(const typename Trait::PageId & page_id, const PageDirectorySnapshotPtr & snap, bool throw_on_not_exist) const;
    typename Trait::PageIdAndEntriesWithError
    getByIDsImpl(const typename Trait::PageIds & page_ids, const PageDirectorySnapshotPtr & snap, bool throw_on_not_exist) const;

private:
    // Only `std::map` is allow for `MVCCMap`. Cause `std::map::insert` ensure that
    // "No iterators or references are invalidated"
    // https://en.cppreference.com/w/cpp/container/map/insert
    using VersionedPageEntriesPtr = std::shared_ptr<VersionedPageEntries<Trait>>;
    using MVCCMapType = std::map<typename Trait::PageId, VersionedPageEntriesPtr>;

    static void applyRefEditRecord(
        MVCCMapType & mvcc_table_directory,
        const VersionedPageEntriesPtr & version_list,
        const typename Trait::EditRecord & rec,
        const PageVersion & version);

    static inline PageDirectorySnapshotPtr
    toConcreteSnapshot(const DB::PageStorageSnapshotPtr & ptr)
    {
        return std::static_pointer_cast<PageDirectorySnapshot>(ptr);
    }

private:
    PageId max_page_id;
    std::atomic<UInt64> sequence;
    mutable std::shared_mutex table_rw_mutex;
    MVCCMapType mvcc_table_directory;

    mutable std::mutex snapshots_mutex;
    mutable std::list<std::weak_ptr<PageDirectorySnapshot>> snapshots;

    mutable ExternalIdsByNamespace<typename Trait::ExternalIdTrait> external_ids_by_ns;

    WALStorePtr wal;
    const UInt64 max_persisted_log_files;
    LoggerPtr log;
};

namespace universal
{
struct PageDirectoryTrait
{
    using PageId = UniversalPageId;
    using PageIdSharedPtr = std::shared_ptr<PageId>;
    using PageEntriesEdit = DB::PS::V3::PageEntriesEdit<PageId>;
    using EditRecord = PageEntriesEdit::EditRecord;

    using EntriesDerefMap = std::map<PageId, std::pair<PageVersion, Int64>>;

    using GcEntries = std::vector<std::tuple<PageId, PageVersion, PageEntryV3>>;
    using GcEntriesMap = std::map<BlobFileId, GcEntries>;

    using PageIdSet = std::set<PageId>;
    using PageIds = std::vector<PageId>;
    using PageIdAndEntry = std::pair<UniversalPageId, PageEntryV3>;
    using PageIdAndEntries = std::vector<PageIdAndEntry>;
    using PageIdAndEntriesWithError = std::pair<PageIdAndEntries, PageIds>;

    using ExternalIdTrait = ExternalIdTrait;
    using Serializer = Serializer;
};

using PageDirectoryPtr = std::unique_ptr<PageDirectory<PageDirectoryTrait>>;
} // namespace universal
namespace u128
{
struct PageDirectoryTrait
{
    using PageId = PageIdV3Internal;
    using PageIdSharedPtr = std::shared_ptr<PageId>;
    using PageEntriesEdit = DB::PS::V3::PageEntriesEdit<PageId>;
    using EditRecord = PageEntriesEdit::EditRecord;

    using EntriesDerefMap = std::map<PageId, std::pair<PageVersion, Int64>>;

    using GcEntries = std::vector<std::tuple<PageId, PageVersion, PageEntryV3>>;
    using GcEntriesMap = std::map<BlobFileId, GcEntries>;

    using PageIdSet = std::set<PageId>;
    using PageIds = std::vector<PageId>;
    using PageIdAndEntry = PageIDAndEntryV3;
    using PageIdAndEntries = PageIDAndEntriesV3;
    using PageIdAndEntriesWithError = std::pair<PageIdAndEntries, PageIds>;

    using ExternalIdTrait = ExternalIdTrait;
    using Serializer = Serializer;
};
using PageDirectoryPtr = std::unique_ptr<PageDirectory<PageDirectoryTrait>>;
using VersionedPageEntries = DB::PS::V3::VersionedPageEntries<PageDirectoryTrait>;
using VersionedPageEntriesPtr = std::shared_ptr<VersionedPageEntries>;
} // namespace u128


} // namespace DB::PS::V3
