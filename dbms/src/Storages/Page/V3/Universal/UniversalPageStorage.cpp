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

#include <Common/SyncPoint/SyncPoint.h>
#include <Common/TiFlashMetrics.h>
#include <Storages/Page/V3/Blob/BlobConfig.h>
#include <Storages/Page/V3/BlobStore.h>
#include <Storages/Page/V3/PageDirectoryFactory.h>
#include <Storages/Page/V3/Universal/UniversalPageStorage.h>
#include <Storages/Page/V3/Universal/UniversalWriteBatchImpl.h>
#include <Storages/Page/V3/WAL/WALConfig.h>
#include <Storages/Page/V3/CheckpointFile/CPDataFileReader.h>

namespace DB
{
UniversalPageStoragePtr UniversalPageStorage::create(
    const String & name,
    PSDiskDelegatorPtr delegator,
    const PageStorageConfig & config,
    const FileProviderPtr & file_provider,
    const String & remote_dir)
{
    UniversalPageStoragePtr storage = std::make_shared<UniversalPageStorage>(name, delegator, config, file_provider);
    storage->blob_store = std::make_unique<PS::V3::universal::BlobStoreType>(
        name,
        file_provider,
        delegator,
        PS::V3::BlobConfig::from(config));
    if (!remote_dir.empty())
    {
        storage->remote_reader = std::make_unique<PS::V3::CPDataFileReader>(remote_dir);
    }
    return storage;
}

void UniversalPageStorage::restore()
{
    blob_store->registerPaths();

    PS::V3::universal::PageDirectoryFactory factory;
    page_directory = factory
                         .setBlobStore(*blob_store)
                         .create(storage_name, file_provider, delegator, PS::V3::WALConfig::from(config));
}

size_t UniversalPageStorage::getNumberOfPages(const String & prefix) const
{
    return page_directory->numPagesWithPrefix(prefix);
}

void UniversalPageStorage::write(UniversalWriteBatch && write_batch, const WriteLimiterPtr & write_limiter) const
{
    if (unlikely(write_batch.empty()))
        return;

    Stopwatch watch;
    SCOPE_EXIT({ GET_METRIC(tiflash_storage_page_write_duration_seconds, type_total).Observe(watch.elapsedSeconds()); });
    auto edit = blob_store->write(write_batch, write_limiter);
    page_directory->apply(std::move(edit), write_limiter);
}

Page UniversalPageStorage::read(const UniversalPageId & page_id, const ReadLimiterPtr & read_limiter, SnapshotPtr snapshot, bool throw_on_not_exist) const
{
    if (!snapshot)
    {
        snapshot = this->getSnapshot("");
    }

    auto page_entry = throw_on_not_exist ? page_directory->getByID(page_id, snapshot) : page_directory->getByIDOrNull(page_id, snapshot);
    auto & checkpoint_info = page_entry.second.checkpoint_info;
    if (checkpoint_info.has_value() && checkpoint_info->is_local_data_reclaimed)
    {
        auto page = remote_reader->read(page_entry);
        UniversalWriteBatch ub;
        auto buf = std::make_shared<ReadBufferFromMemory>(page.data.begin(), page.data.size());
        ub.putPage(page_id, 0, buf, page.data.size());
        auto edit = blob_store->write(ub);
        auto ignored_entries = page_directory->updateRemotePages(std::move(edit), snapshot);
        if (!ignored_entries.empty())
        {
            blob_store->remove(ignored_entries);
        }
        return page;
    }
    else
    {
        return blob_store->read(page_entry, read_limiter);
    }
}

UniversalPageMap UniversalPageStorage::read(const UniversalPageIds & page_ids, const ReadLimiterPtr & read_limiter, SnapshotPtr snapshot, bool throw_on_not_exist) const
{
    if (!snapshot)
    {
        snapshot = this->getSnapshot("");
    }

    if (throw_on_not_exist)
    {
        auto page_entries = page_directory->getByIDs(page_ids, snapshot);
        UniversalPageIdAndEntries local_entries, remote_entries;
        for (auto & entry : page_entries)
        {
            auto & checkpoint_info = entry.second.checkpoint_info;
            if (checkpoint_info.has_value() && checkpoint_info->is_local_data_reclaimed)
            {
                remote_entries.emplace_back(std::move(entry));
            }
            else
            {
                local_entries.emplace_back(std::move(entry));
            }
        }
        auto local_page_map = blob_store->read(local_entries, read_limiter);
        auto remote_page_map = remote_reader->read(remote_entries);
        UniversalWriteBatch ub;
        for (const auto & [page_id, page] : remote_page_map)
        {
            auto buf = std::make_shared<ReadBufferFromMemory>(page.data.begin(), page.data.size());
            ub.putPage(page_id, 0, buf, page.data.size());
            local_page_map.emplace(page_id, page);
        }
        auto edit = blob_store->write(ub);
        auto ignored_entries = page_directory->updateRemotePages(std::move(edit), snapshot);
        if (!ignored_entries.empty())
        {
            blob_store->remove(ignored_entries);
        }
        return local_page_map;
    }
    else
    {
        auto [page_entries, page_ids_not_found] = page_directory->getByIDsOrNull(page_ids, snapshot);
        UniversalPageIdAndEntries local_entries, remote_entries;
        for (auto & entry : page_entries)
        {
            auto & checkpoint_info = entry.second.checkpoint_info;
            if (checkpoint_info.has_value() && checkpoint_info->is_local_data_reclaimed)
            {
                remote_entries.emplace_back(std::move(entry));
            }
            else
            {
                local_entries.emplace_back(std::move(entry));
            }
        }
        auto local_page_map = blob_store->read(local_entries, read_limiter);
        auto remote_page_map = remote_reader->read(remote_entries);
        UniversalWriteBatch ub;
        for (const auto & [page_id, page] : remote_page_map)
        {
            auto buf = std::make_shared<ReadBufferFromMemory>(page.data.begin(), page.data.size());
            ub.putPage(page_id, 0, buf, page.data.size());
            local_page_map.emplace(page_id, page);
        }
        auto edit = blob_store->write(ub);
        auto ignored_entries = page_directory->updateRemotePages(std::move(edit), snapshot);
        if (!ignored_entries.empty())
        {
            blob_store->remove(ignored_entries);
        }
        for (const auto & page_id_not_found : page_ids_not_found)
        {
            local_page_map.emplace(page_id_not_found, Page::invalidPage());
        }
        return local_page_map;
    }
}

UniversalPageMap UniversalPageStorage::read(const std::vector<PageReadFields> & page_fields, const ReadLimiterPtr & read_limiter, SnapshotPtr snapshot, bool throw_on_not_exist) const
{
    if (!snapshot)
    {
        snapshot = this->getSnapshot("");
    }

    // get the entries from directory, keep track
    // for not found page_ids
    UniversalPageIds page_ids_not_found;
    PS::V3::universal::BlobStoreType::FieldReadInfos local_read_infos, remote_read_infos;
    for (const auto & [page_id, field_indices] : page_fields)
    {
        const auto & [id, entry] = throw_on_not_exist ? page_directory->getByID(page_id, snapshot) : page_directory->getByIDOrNull(page_id, snapshot);

        if (entry.isValid())
        {
            auto info = PS::V3::universal::BlobStoreType::FieldReadInfo(page_id, entry, field_indices);
            const auto & checkpoint_info = entry.checkpoint_info;
            if (checkpoint_info.has_value() && checkpoint_info->is_local_data_reclaimed)
            {
                remote_read_infos.emplace_back(info);
            }
            else
            {
                local_read_infos.emplace_back(info);
            }
        }
        else
        {
            page_ids_not_found.emplace_back(id);
        }
    }
    // read page data from blob_store
    auto local_page_map = blob_store->read(local_read_infos, read_limiter);
    auto remote_page_map = remote_reader->read(remote_read_infos);
    for (const auto & [page_id, page] : remote_page_map)
    {
        local_page_map.emplace(page_id, page);
    }
    for (const auto & page_id_not_found : page_ids_not_found)
    {
        local_page_map.emplace(page_id_not_found, Page::invalidPage());
    }
    return local_page_map;
}

void UniversalPageStorage::traverse(const String & prefix, const std::function<void(const UniversalPageId & page_id, const DB::Page & page)> & acceptor, SnapshotPtr snapshot) const
{
    if (!snapshot)
    {
        snapshot = this->getSnapshot("");
    }

    // TODO: This could hold the read lock of `page_directory` for a long time
    const auto page_ids = page_directory->getAllPageIdsWithPrefix(prefix, snapshot);
    for (const auto & page_id : page_ids)
    {
        const auto page_id_and_entry = page_directory->getByID(page_id, snapshot);
        acceptor(page_id_and_entry.first, blob_store->read(page_id_and_entry));
    }
}

UniversalPageId UniversalPageStorage::getNormalPageId(const UniversalPageId & page_id, SnapshotPtr snapshot, bool throw_on_not_exist) const
{
    if (!snapshot)
    {
        snapshot = this->getSnapshot("");
    }

    return page_directory->getNormalPageId(page_id, snapshot, throw_on_not_exist);
}

DB::PageEntry UniversalPageStorage::getEntry(const UniversalPageId & page_id, SnapshotPtr snapshot) const
{
    if (!snapshot)
    {
        snapshot = this->getSnapshot("");
    }

    try
    {
        const auto & [id, entry] = page_directory->getByIDOrNull(page_id, snapshot);
        (void)id;
        PageEntry entry_ret;
        entry_ret.file_id = entry.file_id;
        entry_ret.offset = entry.offset;
        entry_ret.tag = entry.tag;
        entry_ret.size = entry.size;
        entry_ret.field_offsets = entry.field_offsets;
        entry_ret.checksum = entry.checksum;

        return entry_ret;
    }
    catch (DB::Exception & e)
    {
        LOG_WARNING(log, "{}", e.message());
        return {.file_id = INVALID_BLOBFILE_ID}; // return invalid PageEntry
    }
}

PageIdU64 UniversalPageStorage::getMaxIdAfterRestart() const
{
    return page_directory->getMaxIdAfterRestart();
}

bool UniversalPageStorage::gc(bool /*not_skip*/, const WriteLimiterPtr & write_limiter, const ReadLimiterPtr & read_limiter)
{
    return manager.gc(*blob_store, *page_directory, write_limiter, read_limiter, log);
}

void UniversalPageStorage::registerUniversalExternalPagesCallbacks(const UniversalExternalPageCallbacks & callbacks)
{
    manager.registerExternalPagesCallbacks(callbacks);
}
void UniversalPageStorage::unregisterUniversalExternalPagesCallbacks(const String & prefix)
{
    manager.unregisterExternalPagesCallbacks(prefix);
    // clean all external ids ptrs
    page_directory->unregisterNamespace(prefix);
}
} // namespace DB
