#include <Common/CurrentMetrics.h>
#include <Encryption/AESCTRCipherStream.h>
#include <Interpreters/Context.h>
#include <Storages/PathCapacityMetrics.h>
#include <Storages/Transaction/KVStore.h>
#include <Storages/Transaction/ProxyFFIType.h>
#include <Storages/Transaction/Region.h>
#include <Storages/Transaction/TMTContext.h>
#include <sys/statvfs.h>

#include <boost/algorithm/string/join.hpp>

namespace CurrentMetrics
{
extern const Metric RaftNumSnapshotsPendingApply;
}

namespace DB
{

const std::string ColumnFamilyName::Lock = "lock";
const std::string ColumnFamilyName::Default = "default";
const std::string ColumnFamilyName::Write = "write";

ColumnFamilyType NameToCF(const std::string & cf)
{
    if (cf.empty() || cf == ColumnFamilyName::Default)
        return ColumnFamilyType::Default;
    if (cf == ColumnFamilyName::Lock)
        return ColumnFamilyType::Lock;
    if (cf == ColumnFamilyName::Write)
        return ColumnFamilyType::Write;
    throw Exception("Unsupported cf name " + cf, ErrorCodes::LOGICAL_ERROR);
}

const std::string & CFToName(const ColumnFamilyType type)
{
    switch (type)
    {
        case ColumnFamilyType::Default:
            return ColumnFamilyName::Default;
        case ColumnFamilyType::Write:
            return ColumnFamilyName::Write;
        case ColumnFamilyType::Lock:
            return ColumnFamilyName::Lock;
        default:
            throw Exception("Can not tell cf type " + std::to_string(static_cast<uint8_t>(type)), ErrorCodes::LOGICAL_ERROR);
    }
}

RawCppPtr GenCppRawString(BaseBuffView view)
{
    return RawCppPtr(view.len ? new std::string(view.data, view.len) : nullptr, RawCppPtrType::String);
}

static_assert(alignof(TiFlashServerHelper) == alignof(void *));

TiFlashApplyRes HandleWriteRaftCmd(const TiFlashServer * server, WriteCmdsView cmds, RaftCmdHeader header)
{
    try
    {
        return server->tmt->getKVStore()->handleWriteRaftCmd(cmds, header.region_id, header.index, header.term, *server->tmt);
    }
    catch (...)
    {
        tryLogCurrentException(__PRETTY_FUNCTION__);
        exit(-1);
    }
}

TiFlashApplyRes HandleAdminRaftCmd(const TiFlashServer * server, BaseBuffView req_buff, BaseBuffView resp_buff, RaftCmdHeader header)
{
    try
    {
        raft_cmdpb::AdminRequest request;
        raft_cmdpb::AdminResponse response;
        request.ParseFromArray(req_buff.data, (int)req_buff.len);
        response.ParseFromArray(resp_buff.data, (int)resp_buff.len);

        auto & kvstore = server->tmt->getKVStore();
        return kvstore->handleAdminRaftCmd(
            std::move(request), std::move(response), header.region_id, header.index, header.term, *server->tmt);
    }
    catch (...)
    {
        tryLogCurrentException(__PRETTY_FUNCTION__);
        exit(-1);
    }
}

void AtomicUpdateProxy(DB::TiFlashServer * server, DB::TiFlashRaftProxyHelper * proxy) { server->proxy_helper = proxy; }

void HandleDestroy(TiFlashServer * server, RegionId region_id)
{
    try
    {
        auto & kvstore = server->tmt->getKVStore();
        kvstore->handleDestroy(region_id, *server->tmt);
    }
    catch (...)
    {
        tryLogCurrentException(__PRETTY_FUNCTION__);
        exit(-1);
    }
}

TiFlashApplyRes HandleIngestSST(TiFlashServer * server, SnapshotViewArray snaps, RaftCmdHeader header)
{
    try
    {
        auto & kvstore = server->tmt->getKVStore();
        return kvstore->handleIngestSST(header.region_id, snaps, header.index, header.term, *server->tmt);
    }
    catch (...)
    {
        tryLogCurrentException(__PRETTY_FUNCTION__);
        exit(-1);
    }
}

uint8_t HandleCheckTerminated(TiFlashServer * server) { return server->tmt->getTerminated().load(std::memory_order_relaxed) ? 1 : 0; }

FsStats HandleComputeFsStats(TiFlashServer * server)
{
    FsStats res; // res.ok = false by default
    try
    {
        auto global_capacity = server->tmt->getContext().getPathCapacity();
        res = global_capacity->getFsStats();
    }
    catch (...)
    {
        tryLogCurrentException(__PRETTY_FUNCTION__);
    }
    return res;
}

TiFlashStatus HandleGetTiFlashStatus(TiFlashServer * server) { return server->status.load(); }

bool TiFlashRaftProxyHelper::checkServiceStopped() const { return fn_handle_check_service_stopped(proxy_ptr); }
bool TiFlashRaftProxyHelper::checkEncryptionEnabled() const { return fn_is_encryption_enabled(proxy_ptr); }
EncryptionMethod TiFlashRaftProxyHelper::getEncryptionMethod() const { return fn_encryption_method(proxy_ptr); }
FileEncryptionInfo TiFlashRaftProxyHelper::getFile(std::string_view view) const { return fn_handle_get_file(proxy_ptr, view); }
FileEncryptionInfo TiFlashRaftProxyHelper::newFile(std::string_view view) const { return fn_handle_new_file(proxy_ptr, view); }
FileEncryptionInfo TiFlashRaftProxyHelper::deleteFile(std::string_view view) const { return fn_handle_delete_file(proxy_ptr, view); }
FileEncryptionInfo TiFlashRaftProxyHelper::linkFile(std::string_view src, std::string_view dst) const
{
    return fn_handle_link_file(proxy_ptr, src, dst);
}
FileEncryptionInfo TiFlashRaftProxyHelper::renameFile(std::string_view src, std::string_view dst) const
{
    return fn_handle_rename_file(proxy_ptr, src, dst);
}

struct PreHandledTiKVSnapshot
{
    ~PreHandledTiKVSnapshot() { CurrentMetrics::sub(CurrentMetrics::RaftNumSnapshotsPendingApply); }
    PreHandledTiKVSnapshot(const RegionPtr & region_) : region(region_)
    {
        CurrentMetrics::add(CurrentMetrics::RaftNumSnapshotsPendingApply);
    }
    RegionPtr region;
};

RawCppPtr PreHandleTiKVSnapshot(
    TiFlashServer * server, BaseBuffView region_buff, uint64_t peer_id, SnapshotViewArray snaps, uint64_t index, uint64_t term)
{
    try
    {
        metapb::Region region;
        region.ParseFromArray(region_buff.data, (int)region_buff.len);
        auto & tmt = *server->tmt;
        auto & kvstore = tmt.getKVStore();
        auto new_region = GenRegionPtr(std::move(region), peer_id, index, term, tmt);
        kvstore->preHandleTiKVSnapshot(new_region, snaps, tmt);
        auto res = new PreHandledTiKVSnapshot{new_region};
        return RawCppPtr{res, RawCppPtrType::PreHandledTiKVSnapshot};
    }
    catch (...)
    {
        tryLogCurrentException(__PRETTY_FUNCTION__);
        exit(-1);
    }
}

void ApplyPreHandledTiKVSnapshot(TiFlashServer * server, PreHandledTiKVSnapshot * snap)
{
    try
    {
        auto & kvstore = server->tmt->getKVStore();
        kvstore->handleApplySnapshot(snap->region, *server->tmt);
    }
    catch (...)
    {
        tryLogCurrentException(__PRETTY_FUNCTION__);
        exit(-1);
    }
}

void ApplyPreHandledTiFlashSnapshot(TiFlashServer * server, PreHandledTiFlashSnapshot * snap)
{
    TiFlashSnapshotHandler::applyPreHandledTiFlashSnapshot(server->tmt, snap);
}

void ApplyPreHandledSnapshot(TiFlashServer * server, void * res, RawCppPtrType type)
{
    switch (type)
    {
        case RawCppPtrType::PreHandledTiKVSnapshot:
        {
            PreHandledTiKVSnapshot * snap = reinterpret_cast<PreHandledTiKVSnapshot *>(res);
            ApplyPreHandledTiKVSnapshot(server, snap);
            break;
        }
        case RawCppPtrType::PreHandledTiFlashSnapshot:
        {
            PreHandledTiFlashSnapshot * snap = reinterpret_cast<PreHandledTiFlashSnapshot *>(res);
            ApplyPreHandledTiFlashSnapshot(server, snap);
            break;
        }
        default:
            LOG_ERROR(&Logger::get(__PRETTY_FUNCTION__), "unknown type " + std::to_string(uint32_t(type)));
            exit(-1);
    }
}

void GcRawCppPtr(TiFlashServer *, RawCppPtr p)
{
    auto ptr = p.ptr;
    auto type = p.type;
    if (ptr)
    {
        std::cerr << "RawCppPtr::gc raw cpp ptr type " << static_cast<uint32_t>(type) << "\n";

        switch (type)
        {
            case RawCppPtrType::String:
                delete reinterpret_cast<TiFlashRawString>(ptr);
                break;
            case RawCppPtrType::PreHandledTiKVSnapshot:
                delete reinterpret_cast<PreHandledTiKVSnapshot *>(ptr);
                break;
            case RawCppPtrType::TiFlashSnapshot:
                TiFlashSnapshotHandler::deleteTiFlashSnapshot(reinterpret_cast<TiFlashSnapshot *>(ptr));
                break;
            case RawCppPtrType::PreHandledTiFlashSnapshot:
                TiFlashSnapshotHandler::deletePreHandledTiFlashSnapshot(reinterpret_cast<PreHandledTiFlashSnapshot *>(ptr));
                break;
            case RawCppPtrType::SplitKeys:
                delete reinterpret_cast<SplitKeys *>(ptr);
                break;
            default:
                LOG_ERROR(&Logger::get(__PRETTY_FUNCTION__), "unknown type " + std::to_string(uint32_t(type)));
                exit(-1);
        }
    }
}

const char * IntoEncryptionMethodName(EncryptionMethod method)
{
    static const char * EncryptionMethodName[] = {
        "Unknown",
        "Plaintext",
        "Aes128Ctr",
        "Aes192Ctr",
        "Aes256Ctr",
    };
    return EncryptionMethodName[static_cast<uint8_t>(method)];
}

RawCppPtr GenTiFlashSnapshot(TiFlashServer * server, RaftCmdHeader header)
{
    std::cerr << "GenTiFlashSnapshot of region " << header.region_id << " index " << header.index << "\n";

    try
    {
        auto & tmt = server->tmt;
        auto & kvstore = tmt->getKVStore();
        // flush all data of region and persist
        if (!kvstore->preGenTiFlashSnapshot(header.region_id, header.index, *tmt))
            return RawCppPtr(nullptr, RawCppPtrType::None);

        return RawCppPtr(TiFlashSnapshotHandler::genTiFlashSnapshot(tmt, header.region_id), RawCppPtrType::TiFlashSnapshot);
    }
    catch (...)
    {
        tryLogCurrentException(__PRETTY_FUNCTION__);
        exit(-1);
    }
}

SerializeTiFlashSnapshotRes SerializeTiFlashSnapshotInto(TiFlashServer * server, TiFlashSnapshot *snapshot, BaseBuffView path)
{
    std::string real_path(path.data, path.len);
    std::cerr << "serialize TiFlashSnapshot into path " << real_path << "\n";
    auto res = TiFlashSnapshotHandler::serializeTiFlashSnapshotInto(server->tmt, snapshot, real_path);
    std::cerr << "finish write " << res.total_size << " bytes "
              << "\n";
    return res;
}

uint8_t IsTiFlashSnapshot(TiFlashServer * server, BaseBuffView path)
{
    std::string real_path(path.data, path.len);
    std::cerr << "IsTiFlashSnapshot of path " << real_path << "\n";
    auto res = TiFlashSnapshotHandler::isTiFlashSnapshot(server->tmt, real_path);
    std::cerr << "start to check IsTiFlashSnapshot, res " << res << "\n";
    return res;
}

RawCppPtr PreHandleTiFlashSnapshot(
    TiFlashServer * server, BaseBuffView region_buff, uint64_t peer_id, uint64_t index, uint64_t term, BaseBuffView path)
{
    try
    {
        metapb::Region region;
        region.ParseFromArray(region_buff.data, (int)region_buff.len);
        auto & tmt = *server->tmt;
        auto new_region = GenRegionPtr(std::move(region), peer_id, index, term, tmt);
        std::string real_path(path.data, path.len);

        std::cerr << "PreHandleTiFlashSnapshot from path " << real_path << " region " << region.id() << " peer " << peer_id
                  << " index " << index << " term " << term << "\n";
        return RawCppPtr(TiFlashSnapshotHandler::preHandleTiFlashSnapshot(new_region, real_path), RawCppPtrType::PreHandledTiFlashSnapshot);
    }
    catch (...)
    {
        tryLogCurrentException(__PRETTY_FUNCTION__);
        exit(-1);
    }
}

GetRegionApproximateSizeKeysRes GetRegionApproximateSizeKeys(
    TiFlashServer * server, uint64_t region_id, BaseBuffView start_key, BaseBuffView end_key)
{
    std::cerr << __FUNCTION__ << " region " << region_id << "\n";
    (void)start_key;
    (void)end_key;

    auto & tmt = *server->tmt;
    auto region = tmt.getKVStore()->getRegion(region_id);
    if (!region)
    {
        LOG_ERROR(&Logger::get(__PRETTY_FUNCTION__), "Region " << region_id << " not exists");
        return GetRegionApproximateSizeKeysRes{.ok = 0};
    }
    TableID table_id = region->getMappedTableID();
    auto storage = tmt.getStorages().get(table_id);
    if (!storage || storage->isTombstone())
    {
        LOG_ERROR(&Logger::get(__PRETTY_FUNCTION__), "Table " << table_id << " not exists");
        return GetRegionApproximateSizeKeysRes{.ok = 0};
    }
    if (storage->engineType() != TiDB::StorageEngine::DT)
    {
        LOG_ERROR(&Logger::get(__PRETTY_FUNCTION__), "Only DT engine supports region split when region is leader");
        return GetRegionApproximateSizeKeysRes{.ok = 0};
    }

    auto dt_storage = std::dynamic_pointer_cast<StorageDeltaMerge>(storage);
    auto store = dt_storage->getStore();
    auto range = DM::RowKeyRange::fromRegionRange(region->getRange(), table_id, store->isCommonHandle(), store->getRowKeyColumnSize());

    auto [rows, bytes] = store->getRowsAndBytesInRange(tmt.getContext(), range, false);

    std::cerr << __FUNCTION__ << "region " << region_id << " has approximate " << rows << "rows and " << bytes << "bytes\n";

    return GetRegionApproximateSizeKeysRes{.ok = 1, .size = bytes, .keys = rows};
}

SplitKeysRes ScanSplitKeys(
    TiFlashServer * server, uint64_t region_id, BaseBuffView start_key, BaseBuffView end_key, CheckerConfig checker_config)
{
    (void)start_key;
    (void)end_key;

    std::cerr << __FUNCTION__ << " region " << region_id << "\n";

    auto & tmt = *server->tmt;
    auto region = tmt.getKVStore()->getRegion(region_id);
    if (!region)
    {
        LOG_ERROR(&Logger::get(__PRETTY_FUNCTION__), "Region " << region_id << " not exists");
        return SplitKeysRes{.ok = 0, .size = 0, .keys = 0, .split_keys = SplitKeysWithView({})};
    }
    TableID table_id = region->getMappedTableID();
    auto storage = tmt.getStorages().get(table_id);
    if (!storage || storage->isTombstone())
    {
        LOG_ERROR(&Logger::get(__PRETTY_FUNCTION__), "Table " << table_id << " not exists");
        return SplitKeysRes{.ok = 0, .size = 0, .keys = 0, .split_keys = SplitKeysWithView({})};
    }
    if (storage->engineType() != TiDB::StorageEngine::DT)
    {
        LOG_ERROR(&Logger::get(__PRETTY_FUNCTION__), "Only DT engine supports region split when region is leader");
        return SplitKeysRes{.ok = 0, .size = 0, .keys = 0, .split_keys = SplitKeysWithView({})};
    }

    auto dt_storage = std::dynamic_pointer_cast<StorageDeltaMerge>(storage);
    auto store = dt_storage->getStore();
    auto range = DM::RowKeyRange::fromRegionRange(region->getRange(), table_id, store->isCommonHandle(), store->getRowKeyColumnSize());

    auto res = store->getRegionSplitPoint(tmt.getContext(), range, checker_config.max_size, checker_config.split_size);

    std::vector<std::string> split_keys;
    for (auto & p : res.split_points)
        split_keys.push_back(RecordKVFormat::encodeAsTiKVKey(*p.toRegionKey(table_id)));

    std::cerr << __FUNCTION__ << "region " << region_id << " has exactly " << res.exact_rows << "rows and " << res.exact_bytes
              << "bytes, decide to split with keys: [" << boost::algorithm::join(split_keys, ", ") << "]";

    return SplitKeysRes{.ok = 1, .size = res.exact_bytes, .keys = res.exact_rows, .split_keys = SplitKeysWithView(std::move(split_keys))};
}

SplitKeys::~SplitKeys()
{
    std::cerr << "GC SplitKeys success"
              << "\n";
}

} // namespace DB
