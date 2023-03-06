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

#include "FastAddPeer.h"

#include <Common/FailPoint.h>
#include <Common/ThreadPool.h>
#include <Poco/DirectoryIterator.h>
#include <Storages/Page/V3/Universal/UniversalWriteBatchImpl.h>
#include <Storages/Page/V3/Universal/UniversalPageStorage.h>
#include <Storages/Page/V3/Universal/RaftDataReader.h>
#include <Storages/Page/V3/Universal/UniversalPageIdFormatImpl.h>
#include <Storages/Page/V3/PageDirectory.h>
#include <Storages/Transaction/ProxyFFICommon.h>
#include <Storages/Transaction/Region.h>
#include <Storages/Transaction/TMTContext.h>
#include <fmt/core.h>
#include <Storages/S3/S3Common.h>
#include <Storages/S3/S3Filename.h>
#include <Storages/S3/S3GCManager.h>
#include <Storages/S3/CheckpointManifestS3Set.h>

#include <cstdio>
#include <future>
#include <memory>
#include <mutex>
#include <thread>
#include <unordered_map>
#include <utility>

#include "common/defines.h"
#include "common/logger_useful.h"


namespace DB
{
namespace FailPoints
{
extern const char fast_add_peer_sleep[];
}

FastAddPeerRes genFastAddPeerRes(FastAddPeerStatus status, std::string && apply_str, std::string && region_str)
{
    auto * apply = RawCppString::New(apply_str);
    auto * region = RawCppString::New(region_str);
    return FastAddPeerRes{
        .status = status,
        .apply_state = CppStrWithView{.inner = GenRawCppPtr(apply, RawCppPtrTypeImpl::String), .view = BaseBuffView{apply->data(), apply->size()}},
        .region = CppStrWithView{.inner = GenRawCppPtr(region, RawCppPtrTypeImpl::String), .view = BaseBuffView{region->data(), region->size()}},
    };
}

UniversalPageStoragePtr createTempPageStorage(Context & context, const String & manifest_key)
{
    // FIXME: how to get TiFlashS3Client?
    std::shared_ptr<S3::TiFlashS3Client> client;
    auto file_provider = context.getFileProvider();
    PageStorageConfig config;
    auto num = local_ps_num.fetch_add(1, std::memory_order_relaxed);
    auto local_ps = UniversalPageStorage::create( //
        "local",
        context.getPathPool().getPSDiskDelegatorGlobalMulti(fmt::format("local_{}", num)),
        config,
        file_provider);
    local_ps->restore();

    auto reader = CheckpointManifestFileReader<PageDirectoryTrait>::create(CheckpointManifestFileReader<PageDirectoryTrait>::Options{
        .file_path = checkpoint_manifest_path});
    auto t_edit = reader->read();
    const auto & records = t_edit.getRecords();
    UniversalWriteBatch wb;
    // insert delete records at last
    PageEntriesEdit<UniversalPageId>::EditRecords ref_records;
    PageEntriesEdit<UniversalPageId>::EditRecords delete_records;
    for (const auto & record : records)
    {
        if (record.type == EditRecordType::VAR_ENTRY)
        {
            const auto & location = record.entry.remote_info->data_location;
            MemoryWriteBuffer buf;
            writeStringBinary(*location.data_file_id, buf);
            writeIntBinary(location.offset_in_file, buf);
            writeIntBinary(location.size_in_file, buf);
            auto field_sizes = Page::fieldOffsetsToSizes(record.entry.field_offsets, location.size_in_file);
            writeIntBinary(field_sizes.size(), buf);
            for (size_t i = 0; i < field_sizes.size(); i++)
            {
                writeIntBinary(field_sizes[i], buf);
            }
            wb.putPage(record.page_id, record.entry.tag, buf.tryGetReadBuffer(), buf.count());
        }
        else if (record.type == EditRecordType::VAR_REF)
        {
            ref_records.emplace_back(record);
        }
        else if (record.type == EditRecordType::VAR_DELETE)
        {
            delete_records.emplace_back(record);
        }
        else if (record.type == EditRecordType::VAR_EXTERNAL)
        {
            wb.putExternal(record.page_id, record.entry.tag);
        }
        else
        {
            std::cout << "Unknown record type" << std::endl;
            RUNTIME_CHECK_MSG(false, fmt::format("Unknown record type {}", typeToString(record.type)));
        }
    }
    for (const auto & record : ref_records)
    {
        RUNTIME_CHECK(record.type == EditRecordType::VAR_REF);
        wb.putRefPage(record.page_id, record.ori_page_id);
    }
    for (const auto & record : delete_records)
    {
        RUNTIME_CHECK(record.type == EditRecordType::VAR_DELETE);
        wb.delPage(record.page_id);
    }
    local_ps->write(std::move(wb));
    return local_ps;
}

UniversalPageStoragePtr reuseOrCreateTempPageStorage(Context & context, const String & manifest_key)
{

    // TODO: use manifest_key as cache key
    // TODO: how to clear old temp ps
//    auto * log = &Poco::Logger::get("FastAddPeer");
//    auto & local_ps_cache = context.getLocalPageStorageCache();
//    auto maybe_cached_ps = local_ps_cache.maybeGet(store_id, upload_seq);
//    if (maybe_cached_ps.has_value())
//    {
//        LOG_DEBUG(log, "use cache for remote ps [store_id={}] [version={}]", store_id, upload_seq);
//        return maybe_cached_ps.value();
//    }
//    else
//    {
//        LOG_DEBUG(log, "no cache found for remote ps [store_id={}] [version={}]", store_id, upload_seq);
//    }
//    auto local_ps = PS::V3::CheckpointPageManager::createTempPageStorage(context, optimal, checkpoint_data_dir);
//    local_ps_cache.insert(store_id, upload_seq, local_ps);
//    return local_ps;
        return nullptr;
}

std::optional<CheckpointInfoPtr> tryGetCheckpointInfo(Context & context, const String & manifest_key, uint64_t region_id, TiFlashRaftProxyHelper * proxy_helper)
{
    auto * log = &Poco::Logger::get("FastAddPeer");

    auto checkpoint_info = std::make_shared<CheckpointInfo>();
    auto manifest_key_view = S3::S3FilenameView::fromKey(manifest_key);
    checkpoint_info->remote_store_id = manifest_key_view.store_id;
    checkpoint_info->temp_ps = reuseOrCreateTempPageStorage(context, manifest_key);

    try
    {
        auto apply_state_key = UniversalPageIdFormat::toRaftApplyStateKeyInKVEngine(region_id);
        auto page = checkpoint_info->temp_ps->read(apply_state_key);
        checkpoint_info->apply_state.ParseFromArray(page.data, page.data.size());
    }
    catch (...)
    {
        LOG_DEBUG(log, "Failed to find apply state key [region_id={}]", region_id);
        return std::nullopt;
    }

    try
    {
        auto local_state_key = UniversalPageIdFormat::toRegionLocalStateKeyInKVEngine(region_id);
        auto page = checkpoint_info->temp_ps->read(local_state_key);
        checkpoint_info->region_state.ParseFromArray(page.data, page.data.size());
    }
    catch (...)
    {
        LOG_DEBUG(log, "Failed to find region local state key [region_id={}]", region_id);
        return std::nullopt;
    }

    try
    {
        auto region_key = UniversalPageIdFormat::toKVStoreKey(region_id);
        auto page = checkpoint_info->temp_ps->read(region_key);
        ReadBufferFromMemory buf(page.data.begin(), page.data.size());
        checkpoint_info->region = Region::deserialize(buf, proxy_helper);
    }
    catch (...)
    {
        LOG_DEBUG(log, "Failed to find region key [region_id={}]", region_id);
        return std::nullopt;
    }

    return checkpoint_info;
}

std::optional<CheckpointInfoPtr> selectCheckpointInfo(Context & context, uint64_t region_id, uint64_t new_peer_id, TiFlashRaftProxyHelper * proxy_helper)
{
    auto * log = &Poco::Logger::get("FastAddPeer");

    std::vector<CheckpointInfoPtr> candidates;
    std::map<uint64_t, std::string> reason;
    std::map<uint64_t, std::string> candidate_stat;

    // FIXME: how to get TiFlashS3Client?
    std::shared_ptr<S3::TiFlashS3Client> client;
    auto all_store_ids = S3::S3GCManager::getAllStoreIds(client);
    auto current_store_id = context.getTMTContext().getKVStore()->getStoreMeta().id();
    for (const auto & store_id : all_store_ids)
    {
        if (store_id == current_store_id)
            continue;
        const auto manifests = S3::CheckpointManifestS3Set::getFromS3(*client, store_id);
        if (manifests.empty())
        {
            LOG_DEBUG(log, "no manifest on this store, skip gc_store_id={}", store_id);
            continue;
        }
        const auto & latest_manifest_key = manifests.latestManifestKey();
        auto region_info = tryGetCheckpointInfo(context, latest_manifest_key, region_id, proxy_helper);
        if (region_info.has_value())
        {
            candidates.push_back(std::move(*region_info));
        }
    }

    if (candidates.empty())
    {
        LOG_INFO(log, "No candidate. [region_id={}]", region_id);
        return std::nullopt;
    }

    std::optional<CheckpointInfo> winner = std::nullopt;
    uint64_t largest_applied_index = 0;
    for (auto it = candidates.begin(); it != candidates.end(); it++)
    {
        auto store_id = it->remote_store_id;
        const auto & region_state = it->region_state;
        const auto & apply_state = it->apply_state;
        const auto & peers = region_state.region().peers();
        bool ok = false;
        for (auto && pr : peers)
        {
            if (pr.id() == new_peer_id)
            {
                ok = true;
                break;
            }
        }
        if (!ok)
        {
            // Can't use this peer if it has no new_peer_id.
            reason[store_id] = fmt::format("has no peer_id {}", region_state.ShortDebugString());
            continue;
        }
        auto peer_state = region_state.state();
        if (peer_state == PeerState::Tombstone || peer_state == PeerState::Applying)
        {
            // Can't use this peer in these states.
            reason[store_id] = fmt::format("bad peer_state {}", region_state.ShortDebugString());
            continue;
        }
        auto applied_index = apply_state.applied_index();
        if (!winner.has_value() || applied_index > largest_applied_index)
        {
            candidate_stat[store_id] = fmt::format("applied index {}", applied_index);
            winner = *it;
        }
    }

    if (winner.has_value())
    {
        return winner;
    }
    else
    {
        FmtBuffer fmt_buf;
        for (auto iter = reason.begin(); iter != reason.end(); iter++)
        {
            fmt_buf.fmtAppend("store {} reason {}, ", iter->first, iter->second);
        }
        std::string failed_reason = fmt_buf.toString();
        fmt_buf.clear();
        for (auto iter = candidate_stat.begin(); iter != candidate_stat.end(); iter++)
        {
            fmt_buf.fmtAppend("store {} stat {}, ", iter->first, iter->second);
        }
        std::string choice_stat = fmt_buf.toString();
        LOG_INFO(log, "Failed to find remote candidate [region_id={}] [new_peer_id={}] [total_choices={}]; reason: {}; candidates: {};", region_id, new_peer_id, choices.size(), failed_reason, choice_stat);
        return std::nullopt;
    }
}

void resetPeerIdInRegion(RegionPtr region, const RegionLocalState & region_state, uint64_t new_peer_id)
{
    for (auto && pr : region_state.region().peers())
    {
        if (pr.id() == new_peer_id)
        {
            auto cpr = pr;
            region->mutMeta().setPeer(std::move(cpr));
            return;
        }
    }
    RUNTIME_CHECK(false);
}

FastAddPeerRes FastAddPeerImpl(EngineStoreServerWrap * server, uint64_t region_id, uint64_t new_peer_id)
{
    try
    {
        auto * log = &Poco::Logger::get("FastAddPeer");
        fiu_do_on(FailPoints::fast_add_peer_sleep, {
            LOG_INFO(log, "failpoint sleep before add peer");
            std::this_thread::sleep_for(std::chrono::milliseconds(500));
        });
        auto kvstore = server->tmt->getKVStore();
        auto current_store_id = kvstore->getStoreMeta().id();
        Stopwatch watch;
        std::optional<CheckpointInfoPtr> maybe_checkpoint_info;
        while (true)
        {
            maybe_checkpoint_info = selectCheckpointInfo(server->tmt->getContext(), region_id, new_peer_id, server->proxy_helper);
            if (!maybe_checkpoint_info.has_value())
            {
                if (watch.elapsedSeconds() >= 60)
                    return genFastAddPeerRes(FastAddPeerStatus::NoSuitable, "", "");
                std::this_thread::sleep_for(std::chrono::milliseconds(500));
            }
            else
                break;
        }
        auto checkpoint_info = *maybe_checkpoint_info;
        LOG_INFO(log, "Select checkpoint from store {} takes {} seconds; [region_id={}]", checkpoint_info->remote_store_id, watch.elapsedSeconds(), region_id);

        resetPeerIdInRegion(checkpoint_info->region, checkpoint_info->region_state, new_peer_id);
        kvstore->handleIngestCheckpoint(checkpoint_info, *server->tmt);

        // Write raft log to uni ps
        UniversalWriteBatch wb;
        RaftDataReader raft_data_reader(*(checkpoint_info->temp_ps));
        raft_data_reader.traverseRaftLogForRegion(region_id, [&](const UniversalPageId & page_id, DB::Page page) {
            MemoryWriteBuffer buf;
            buf.write(page.data.begin(), page.data.size());
            wb.putPage(page_id, 0, buf.tryGetReadBuffer(), page.data.size());
        });
        auto wn_ps = server->tmt->getContext().getWriteNodePageStorage();
        wn_ps->write(std::move(wb));

        return genFastAddPeerRes(FastAddPeerStatus::Ok, checkpoint_info->apply_state.SerializeAsString(), checkpoint_info->region_state.region().SerializeAsString());
    }
    catch (...)
    {
        DB::tryLogCurrentException("FastAddPeer", "Failed when try to restore from checkpoint");
        return genFastAddPeerRes(FastAddPeerStatus::BadData, "", "");
    }
}

FastAddPeerRes FastAddPeer(EngineStoreServerWrap * server, uint64_t region_id, uint64_t new_peer_id)
{
    try
    {
        auto * log = &Poco::Logger::get("FastAddPeer");
        auto fap_ctx = server->tmt->getContext().getFastAddPeerContext();
        if (!fap_ctx->tasks_trace->isScheduled(region_id))
        {
            // We need to schedule the task.
            auto res = fap_ctx->tasks_trace->addTask(region_id, [server, region_id, new_peer_id]() {
                return FastAddPeerImpl(server, region_id, new_peer_id);
            });
            if (res) {
                LOG_INFO(log, "add new task [new_peer_id={}] [region_id={}]", new_peer_id, region_id);
            } else {
                LOG_WARNING(log, "add new task fail(queue full) [new_peer_id={}] [region_id={}]", new_peer_id, region_id);
                return genFastAddPeerRes(FastAddPeerStatus::WaitForData, "", "");
            }
        }

        if (fap_ctx->tasks_trace->isReady(region_id))
        {
            LOG_INFO(log, "fetch task result [new_peer_id={}] [region_id={}]", new_peer_id, region_id);
            return fap_ctx->tasks_trace->fetchResult(region_id);
        }
        else
        {
            LOG_DEBUG(log, "the task is still pending [new_peer_id={}] [region_id={}]", new_peer_id, region_id);
            return genFastAddPeerRes(FastAddPeerStatus::WaitForData, "", "");
        }
    }
    catch (...)
    {
        DB::tryLogCurrentException("FastAddPeer", fmt::format("Failed when try to restore from checkpoint {}", StackTrace().toString()));
        return genFastAddPeerRes(FastAddPeerStatus::OtherError, "", "");
    }
}

} // namespace DB
