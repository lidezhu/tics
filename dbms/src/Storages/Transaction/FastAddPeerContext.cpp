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


#include <Storages/Transaction/ProxyFFI.h>
#include <Storages/Transaction/FastAddPeer.h>
#include <Storages/Page/V3/Universal/UniversalWriteBatchImpl.h>
#include <Storages/Page/V3/Universal/UniversalPageStorage.h>
#include <Storages/S3/S3Common.h>
#include <Storages/S3/S3Filename.h>
#include <Storages/S3/S3GCManager.h>
#include <Storages/S3/CheckpointManifestS3Set.h>
#include <Storages/S3/S3RandomAccessFile.h>
#include <Storages/Page/V3/CheckpointFile/CPManifestFileReader.h>

#include <memory>
#include <future>
#include <map>
#include <mutex>

namespace DB
{
FastAddPeerContext::FastAddPeerContext()
{
    static constexpr int ffi_handle_sec = 5;
    static constexpr int region_per_sec = 2;
    int thread_count = ffi_handle_sec * region_per_sec;
    tasks_trace = new AsyncTasks(thread_count);
}

FastAddPeerContext::~FastAddPeerContext()
{
    delete tasks_trace;
}

bool FastAddPeerContext::AsyncTasks::addTask(Key k, Func f)
{
    using P = std::packaged_task<FastAddPeerRes()>;
    std::shared_ptr<P> p = std::make_shared<P>(P(f));

    auto res = thread_pool->trySchedule([p]() { (*p)(); }, 0, 0);
    if (res)
    {
        std::scoped_lock l(mtx);
        futures[k] = p->get_future();
    }
    return res;
}

bool FastAddPeerContext::AsyncTasks::isScheduled(Key key) const
{
    std::scoped_lock l(mtx);
    return futures.count(key);
}

bool FastAddPeerContext::AsyncTasks::isReady(Key key) const
{
    using namespace std::chrono_literals;
    std::scoped_lock l(mtx);
    if (!futures.count(key))
        return false;
    if (futures.at(key).wait_for(0ms) == std::future_status::ready)
    {
        return true;
    }
    return false;
}

FastAddPeerRes FastAddPeerContext::AsyncTasks::fetchResult(Key key)
{
    std::unique_lock<std::mutex> l(mtx);
    auto it = futures.find(key);
    auto fut = std::move(it->second);
    futures.erase(it);
    l.unlock();
    return fut.get();
}

UniversalPageStoragePtr createTempPageStorage(Context & context, const String & manifest_key)
{
    auto file_provider = context.getFileProvider();
    PageStorageConfig config;
    auto s3_client = S3::ClientFactory::instance().sharedClient();
    auto bucket = S3::ClientFactory::instance().bucket();
    auto local_ps = UniversalPageStorage::create( //
        "local",
        context.getPathPool().getPSDiskDelegatorGlobalMulti("local"),
        config,
        file_provider,
        s3_client,
        bucket);
    local_ps->restore();

    RandomAccessFilePtr manifest_file = S3::S3RandomAccessFile::create(manifest_key);
    auto reader = PS::V3::CPManifestFileReader::create({
        .plain_file = manifest_file,
    });
    auto im = PS::V3::CheckpointProto::StringsInternMap{};
    auto prefix = reader->readPrefix();
    auto edits = reader->readEdits(im);
    auto records = edits->getRecords();


    UniversalWriteBatch wb;
    wb.disableRemoteWrite();
    // insert delete records at last
    PS::V3::PageEntriesEdit<UniversalPageId>::EditRecords ref_records;
    PS::V3::PageEntriesEdit<UniversalPageId>::EditRecords delete_records;
    for (auto & record : records)
    {
        if (record.type == PS::V3::EditRecordType::VAR_ENTRY)
        {
            wb.putRemotePage(record.page_id, record.entry.tag, record.entry.checkpoint_info->data_location, std::move(record.entry.field_offsets));
        }
        else if (record.type == PS::V3::EditRecordType::VAR_REF)
        {
            ref_records.emplace_back(record);
        }
        else if (record.type == PS::V3::EditRecordType::VAR_DELETE)
        {
            delete_records.emplace_back(record);
        }
        else if (record.type == PS::V3::EditRecordType::VAR_EXTERNAL)
        {
            if (record.entry.checkpoint_info.has_value())
            {
                wb.putRemoteExternal(record.page_id, record.entry.checkpoint_info->data_location);
            }
            else
            {
                // TODO: check this is enough
                wb.putExternal(record.page_id, 0);
            }
        }
        else
        {
            std::cout << "Unknown record type" << std::endl;
            RUNTIME_CHECK_MSG(false, fmt::format("Unknown record type {}", typeToString(record.type)));
        }
    }
    for (const auto & record : ref_records)
    {
        RUNTIME_CHECK(record.type == PS::V3::EditRecordType::VAR_REF);
        wb.putRefPage(record.page_id, record.ori_page_id);
    }
    for (const auto & record : delete_records)
    {
        RUNTIME_CHECK(record.type == PS::V3::EditRecordType::VAR_DELETE);
        wb.delPage(record.page_id);
    }
    local_ps->write(std::move(wb));
    return local_ps;
}
} // namespace DB
