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

#include <Storages/Page/V3/Universal/RaftDataReader.h>

namespace DB
{
Page RaftDataReader::read(const UniversalPageId & page_id)
{
    auto snapshot = uni_ps.getSnapshot(fmt::format("read_r_{}", page_id));
    return uni_ps.read(page_id, nullptr, snapshot, /*throw_on_not_exist*/ false);
}

void RaftDataReader::traverse(const UniversalPageId & start, const UniversalPageId & end, const std::function<void(const UniversalPageId & page_id, DB::Page page)> & acceptor)
{
    auto snapshot = uni_ps.getSnapshot(fmt::format("scan_r_{}_{}", start, end));
    const auto page_ids = uni_ps.page_directory->getAllPageIdsInRange(start, end, snapshot);
    for (const auto & page_id : page_ids)
    {
        const auto page_id_and_entry = uni_ps.page_directory->getByID(page_id, snapshot);
        acceptor(page_id, uni_ps.blob_store->read(page_id_and_entry));
    }
}

std::optional<UniversalPageId> RaftDataReader::getLowerBound(const UniversalPageId & page_id)
{
    auto snapshot = uni_ps.getSnapshot(fmt::format("lower_bound_r_{}", page_id));
    return uni_ps.page_directory->getLowerBound(page_id, snapshot);
}
} // namespace DB
