#include <Storages/Page/UniversalPageStorage.h>
#include <Storages/Page/V3/Blob/BlobConfig.h>
#include <Storages/Page/V3/BlobStore.h>
#include <Storages/Page/V3/PageDirectoryFactory.h>
#include <Storages/Page/V3/PageStorageImpl.h>
#include <Storages/Page/V3/WAL/WALConfig.h>

namespace DB
{

UniversalPageStoragePtr UniversalPageStorage::create(
    String name,
    PSDiskDelegatorPtr delegator,
    const UniversalPageStorage::Config & config,
    const FileProviderPtr & file_provider)
{
    UniversalPageStoragePtr storage = std::make_shared<UniversalPageStorage>(name, delegator, config, file_provider);
    PS::V3::BlobConfig blob_config; // FIXME: parse from config
    storage->blob_store = std::make_shared<PS::V3::BlobStore<PS::V3::universal::BlobStoreTrait>>(
        name,
        file_provider,
        delegator,
        blob_config);
    return storage;
}

void UniversalPageStorage::restore()
{
    blob_store->registerPaths();

    PS::V3::universal::PageDirectoryFactory factory;
    PS::V3::WALConfig wal_config; // FIXME: parse from config
    page_directory = factory.setBlobStore(*blob_store).create(storage_name, file_provider, delegator, wal_config);
}

} // namespace DB
