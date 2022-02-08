#include <Common/CurrentMetrics.h>
#include <DataStreams/OneBlockInputStream.h>
#include <Storages/DeltaMerge/DMContext.h>
#include <Storages/DeltaMerge/DeltaMergeStore.h>
#include <Storages/DeltaMerge/File/DMFileBlockOutputStream.h>
#include <Storages/DeltaMerge/Segment.h>
#include <Storages/Transaction/TMTContext.h>
#include <Storages/tests/TiFlashStorageTestBasic.h>
#include <TestUtils/TiFlashTestBasic.h>

#include <memory>

#include "dm_basic_include.h"

namespace CurrentMetrics
{
extern const Metric DT_SnapshotOfRead;
} // namespace CurrentMetrics
namespace DB
{
namespace DM
{
namespace tests
{
void assertBlocksEqual(const Blocks & blocks1, const Blocks & blocks2)
{
    ASSERT_EQ(blocks1.size(), blocks2.size());
    for (size_t i = 0; i < blocks1.size(); ++i)
        ASSERT_EQ(blocks1[i].rows(), blocks2[i].rows());

    // use hash to check the read results
    SipHash hash1;
    for (const auto & block : blocks1)
        block.updateHash(hash1);

    SipHash hash2;
    for (const auto & block : blocks2)
        block.updateHash(hash2);

    ASSERT_EQ(hash1.get64(), hash2.get64());
}

class DeltaValueSpaceTest : public DB::base::TiFlashStorageTestBasic
{
public:
    static void SetUpTestCase() {}

    void SetUp() override
    {
        TiFlashStorageTestBasic::SetUp();
        table_columns = std::make_shared<ColumnDefines>();

        delta = reload();
        ASSERT_EQ(delta->getId(), delta_id);
    }

protected:
    DeltaValueSpacePtr reload(const ColumnDefinesPtr & pre_define_columns = {}, DB::Settings && db_settings = DB::Settings())
    {
        TiFlashStorageTestBasic::reload(std::move(db_settings));
        storage_path_pool = std::make_unique<StoragePathPool>(db_context->getPathPool().withTable("test", "t1", false));
        storage_pool = std::make_unique<StoragePool>("test.t1", *storage_path_pool, *db_context, db_context->getSettingsRef());
        storage_pool->restore();
        ColumnDefinesPtr cols = (!pre_define_columns) ? DMTestEnv::getDefaultColumns() : pre_define_columns;
        setColumns(cols);

        return std::make_unique<DeltaValueSpace>(delta_id);
    }

    // setColumns should update dm_context at the same time
    void setColumns(const ColumnDefinesPtr & columns)
    {
        *table_columns = *columns;

        dm_context = std::make_unique<DMContext>(*db_context,
                                                 *storage_path_pool,
                                                 *storage_pool,
                                                 0,
                                                 /*min_version_*/ 0,
                                                 settings.not_compress_columns,
                                                 false,
                                                 1,
                                                 db_context->getSettingsRef());
    }

    const ColumnDefinesPtr & tableColumns() const { return table_columns; }

    DMContext & dmContext() { return *dm_context; }

protected:
    /// all these var lives as ref in dm_context
    std::unique_ptr<StoragePathPool> storage_path_pool;
    std::unique_ptr<StoragePool> storage_pool;
    ColumnDefinesPtr table_columns;
    DM::DeltaMergeStore::Settings settings;
    /// dm_context
    std::unique_ptr<DMContext> dm_context;

    // the delta we are going to test
    DeltaValueSpacePtr delta;

    static constexpr PageId delta_id = 1;
    static constexpr size_t num_rows_write_per_batch = 100;
};

TEST_F(DeltaValueSpaceTest, WriteRead)
{
    Blocks write_blocks;
    size_t total_rows_write = 0;
    // write data to memory and read it
    {
        Block block = DMTestEnv::prepareSimpleWriteBlock(0, num_rows_write_per_batch, false);
        write_blocks.push_back(block);
        delta->appendToCache(dmContext(), block, 0, block.rows());
        total_rows_write += num_rows_write_per_batch;
        // read
        auto snapshot = delta->createSnapshot(dmContext(), false, CurrentMetrics::DT_SnapshotOfRead);
        auto rows = snapshot->getRows();
        ASSERT_EQ(rows, total_rows_write);
        auto reader = std::make_shared<DeltaValueReader>(
            dmContext(),
            snapshot,
            table_columns,
            RowKeyRange::newAll(false, 1));
        {
            auto columns = block.cloneEmptyColumns();
            ASSERT_EQ(reader->readRows(columns, 0, total_rows_write, nullptr), total_rows_write);
            Blocks result_blocks;
            result_blocks.push_back(block.cloneWithColumns(std::move(columns)));
            assertBlocksEqual(write_blocks, result_blocks);
        }
        // read with a specific range
        {
            auto columns = block.cloneEmptyColumns();
            RowKeyRange read_range = RowKeyRange::fromHandleRange(HandleRange(0, num_rows_write_per_batch / 2));
            ASSERT_EQ(reader->readRows(columns, 0, total_rows_write, &read_range), num_rows_write_per_batch / 2);
        }
    }

    // flush data to disk and read again
    {
        ASSERT_EQ(delta->getUnsavedRows(), total_rows_write);
        delta->flush(dmContext());
        ASSERT_EQ(delta->getUnsavedRows(), 0);
        auto snapshot = delta->createSnapshot(dmContext(), false, CurrentMetrics::DT_SnapshotOfRead);
        auto rows = snapshot->getRows();
        ASSERT_EQ(rows, total_rows_write);
        auto reader = std::make_shared<DeltaValueReader>(
            dmContext(),
            snapshot,
            table_columns,
            RowKeyRange::newAll(false, 1));
        {
            auto columns = write_blocks[0].cloneEmptyColumns();
            ASSERT_EQ(reader->readRows(columns, 0, total_rows_write, nullptr), total_rows_write);
            Blocks result_blocks;
            result_blocks.push_back(write_blocks[0].cloneWithColumns(std::move(columns)));
            assertBlocksEqual(write_blocks, result_blocks);
        }
        // read with a specific range
        {
            auto columns = write_blocks[0].cloneEmptyColumns();
            RowKeyRange read_range = RowKeyRange::fromHandleRange(HandleRange(0, num_rows_write_per_batch / 2));
            ASSERT_EQ(reader->readRows(columns, 0, total_rows_write, &read_range), num_rows_write_per_batch / 2);
        }
    }

    // write more data to memory and read again
    {
        Block block = DMTestEnv::prepareSimpleWriteBlock(total_rows_write, total_rows_write + num_rows_write_per_batch, false);
        write_blocks.push_back(block);
        delta->appendToCache(dmContext(), block, 0, block.rows());
        total_rows_write += num_rows_write_per_batch;
        // read
        auto snapshot = delta->createSnapshot(dmContext(), false, CurrentMetrics::DT_SnapshotOfRead);
        auto rows = snapshot->getRows();
        ASSERT_EQ(rows, total_rows_write);
        auto reader = std::make_shared<DeltaValueReader>(
            dmContext(),
            snapshot,
            table_columns,
            RowKeyRange::newAll(false, 1));
        {
            size_t total_read_rows = 0;
            Blocks result_blocks;
            while (total_read_rows < total_rows_write)
            {
                auto columns = block.cloneEmptyColumns();
                size_t read_rows = reader->readRows(columns, total_read_rows, num_rows_write_per_batch, nullptr);
                ASSERT_EQ(read_rows, num_rows_write_per_batch);
                total_read_rows += read_rows;
                result_blocks.push_back(block.cloneWithColumns(std::move(columns)));
            }
            ASSERT_EQ(total_read_rows, total_rows_write);
            assertBlocksEqual(write_blocks, result_blocks);
        }
        // read with a specific range
        {
            auto columns = block.cloneEmptyColumns();
            RowKeyRange read_range = RowKeyRange::fromHandleRange(HandleRange(0, num_rows_write_per_batch + num_rows_write_per_batch / 2));
            ASSERT_EQ(reader->readRows(columns, 0, total_rows_write, &read_range), num_rows_write_per_batch + num_rows_write_per_batch / 2);
        }
    }

    // flush to disk, write a delete range and write more data
    {
        ASSERT_EQ(delta->getUnsavedRows(), num_rows_write_per_batch);
        delta->flush(dmContext());
        ASSERT_EQ(delta->getUnsavedRows(), 0);
        // the actual delete range value doesn't matter
        delta->appendDeleteRange(dmContext(), RowKeyRange::fromHandleRange(HandleRange(0, num_rows_write_per_batch)));

        Block block = DMTestEnv::prepareSimpleWriteBlock(total_rows_write, total_rows_write + num_rows_write_per_batch, false);
        write_blocks.push_back(block);
        delta->appendToCache(dmContext(), block, 0, block.rows());
        total_rows_write += num_rows_write_per_batch;
        // read
        auto snapshot = delta->createSnapshot(dmContext(), false, CurrentMetrics::DT_SnapshotOfRead);
        auto rows = snapshot->getRows();
        ASSERT_EQ(rows, total_rows_write);
        auto reader = std::make_shared<DeltaValueReader>(
            dmContext(),
            snapshot,
            table_columns,
            RowKeyRange::newAll(false, 1));
        {
            size_t total_read_rows = 0;
            Blocks result_blocks;
            while (total_read_rows < total_rows_write)
            {
                auto columns = block.cloneEmptyColumns();
                size_t read_rows = reader->readRows(columns, total_read_rows, num_rows_write_per_batch, nullptr);
                ASSERT_EQ(read_rows, num_rows_write_per_batch);
                total_read_rows += read_rows;
                result_blocks.push_back(block.cloneWithColumns(std::move(columns)));
            }
            ASSERT_EQ(total_read_rows, total_rows_write);
            assertBlocksEqual(write_blocks, result_blocks);
        }
        // read with a specific range
        {
            auto columns = block.cloneEmptyColumns();
            RowKeyRange read_range = RowKeyRange::fromHandleRange(HandleRange(0, 2 * num_rows_write_per_batch + num_rows_write_per_batch / 2));
            ASSERT_EQ(reader->readRows(columns, 0, total_rows_write, &read_range), 2 * num_rows_write_per_batch + num_rows_write_per_batch / 2);
        }
    }
}

void appendBlockToDeltaValueSpace(DMContext & context, DeltaValueSpacePtr delta, size_t rows_start, size_t rows_num)
{
    Block block = DMTestEnv::prepareSimpleWriteBlock(rows_start, rows_start + rows_num, false);
    delta->appendToCache(context, block, 0, block.rows());
}

void appendColumnFileToDeltaValueSpace(DMContext & context, DeltaValueSpacePtr delta, size_t rows_start, size_t rows_num, WriteBatches & wbs)
{
    Block block = DMTestEnv::prepareSimpleWriteBlock(rows_start, rows_start + rows_num, false);
    auto tiny_file = ColumnFileTiny::writeColumnFile(context, block, 0, block.rows(), wbs);
    wbs.writeLogAndData();
    delta->appendColumnFile(context, tiny_file);
}

// Write data to MemTableSet when do flush at the same time
TEST_F(DeltaValueSpaceTest, Flush)
{
    auto mem_table_set = delta->getMemTableSet();
    auto persisted_file_set = delta->getPersistedFileSet();
    WriteBatches wbs(dmContext().storage_pool, dmContext().getWriteLimiter());
    size_t total_rows_write = 0;
    // write some column_file
    {
        {
            appendBlockToDeltaValueSpace(dmContext(), delta, total_rows_write, num_rows_write_per_batch);
            total_rows_write += num_rows_write_per_batch;
        }
        {
            delta->appendDeleteRange(dmContext(), RowKeyRange::fromHandleRange(HandleRange(0, num_rows_write_per_batch)));
        }
        {
            appendColumnFileToDeltaValueSpace(dmContext(), delta, total_rows_write, num_rows_write_per_batch, wbs);
            total_rows_write += num_rows_write_per_batch;
        }
    }
    // build flush task and finish prepare stage
    ColumnFileFlushTaskPtr flush_task;
    {
        flush_task = mem_table_set->buildFlushTask(dmContext(), persisted_file_set->getRows(), persisted_file_set->getDeletes(), persisted_file_set->getCurrentFlushVersion());
        ASSERT_EQ(flush_task->getTaskNum(), 3);
        ASSERT_EQ(flush_task->getFlushRows(), 2 * num_rows_write_per_batch);
        ASSERT_EQ(flush_task->getFlushDeletes(), 1);
        flush_task->prepare(wbs);
    }
    // another thread write more data to the delta value space
    {
        appendBlockToDeltaValueSpace(dmContext(), delta, total_rows_write, num_rows_write_per_batch);
        total_rows_write += num_rows_write_per_batch;
    }
    // commit the flush task and check the status after flush
    {
        ASSERT_TRUE(flush_task->commit(persisted_file_set, wbs));
        ASSERT_EQ(persisted_file_set->getRows(), 2 * num_rows_write_per_batch);
        ASSERT_EQ(persisted_file_set->getDeletes(), 1);
        ASSERT_EQ(mem_table_set->getRows(), total_rows_write - persisted_file_set->getRows());
    }
}

TEST_F(DeltaValueSpaceTest, MinorCompaction)
{
    auto persisted_file_set = delta->getPersistedFileSet();
    WriteBatches wbs(dmContext().storage_pool, dmContext().getWriteLimiter());
    size_t total_rows_write = 0;
    // write some column_file and flush
    {
        {
            appendBlockToDeltaValueSpace(dmContext(), delta, total_rows_write, num_rows_write_per_batch);
            total_rows_write += num_rows_write_per_batch;
        }
        {
            appendColumnFileToDeltaValueSpace(dmContext(), delta, total_rows_write, num_rows_write_per_batch, wbs);
            total_rows_write += num_rows_write_per_batch;
        }
        {
            appendBlockToDeltaValueSpace(dmContext(), delta, total_rows_write, num_rows_write_per_batch);
            total_rows_write += num_rows_write_per_batch;
        }
        {
            delta->appendDeleteRange(dmContext(), RowKeyRange::fromHandleRange(HandleRange(0, num_rows_write_per_batch)));
        }
        delta->flush(dmContext());
    }
    // build compaction task and finish prepare stage
    MinorCompactionPtr compaction_task;
    {
        PageStorage::SnapshotPtr log_storage_snap = dmContext().storage_pool.log()->getSnapshot();
        PageReader reader(dmContext().storage_pool.log(), std::move(log_storage_snap), dmContext().getReadLimiter());
        compaction_task = persisted_file_set->pickUpMinorCompaction(dmContext());
        ASSERT_EQ(compaction_task->getCompactionSourceLevel(), 0);
        // There should be two compaction sub_tasks.
        // The first task try to compact the first three column files to a larger one,
        // and the second task is just a trivial move for the last column file which is a delete range.
        const auto & tasks = compaction_task->getTasks();
        ASSERT_EQ(tasks.size(), 2);
        ASSERT_EQ(tasks[0].to_compact.size(), 3);
        ASSERT_EQ(tasks[0].is_trivial_move, false);
        ASSERT_EQ(tasks[1].to_compact.size(), 1);
        ASSERT_EQ(tasks[1].is_trivial_move, true);
        compaction_task->prepare(dmContext(), wbs, reader);
    }
    // another thread write more data to the delta value space and flush it
    {
        appendBlockToDeltaValueSpace(dmContext(), delta, total_rows_write, num_rows_write_per_batch);
        total_rows_write += num_rows_write_per_batch;
        delta->flush(dmContext());
        ASSERT_EQ(delta->getUnsavedRows(), 0);
        ASSERT_EQ(persisted_file_set->getRows(), total_rows_write);
        ASSERT_EQ(persisted_file_set->getDeletes(), 1);
        ASSERT_EQ(persisted_file_set->getColumnFileCount(), 5);
    }
    // commit the compaction task and check the status
    {
        ASSERT_TRUE(compaction_task->commit(persisted_file_set, wbs));
        ASSERT_EQ(persisted_file_set->getRows(), total_rows_write);
        ASSERT_EQ(persisted_file_set->getDeletes(), 1);
        ASSERT_EQ(persisted_file_set->getColumnFileCount(), 3);
    }
    // after compaction, the column file in persisted_file_set should be like the following:
    // level 0: T_100
    // level 1: T_300, D_0_100
    // so there is no compaction task to do
    {
        compaction_task = persisted_file_set->pickUpMinorCompaction(dmContext());
        ASSERT_TRUE(!compaction_task);
    }
    // do a lot of minor compaction and check the status
    {
        for (size_t i = 0; i < 20; i++)
        {
            appendBlockToDeltaValueSpace(dmContext(), delta, total_rows_write, num_rows_write_per_batch);
            total_rows_write += num_rows_write_per_batch;
            delta->flush(dmContext());
            while (true)
            {
                PageStorage::SnapshotPtr log_storage_snap = dmContext().storage_pool.log()->getSnapshot();
                PageReader reader(dmContext().storage_pool.log(), std::move(log_storage_snap), dmContext().getReadLimiter());
                auto minor_compaction_task = persisted_file_set->pickUpMinorCompaction(dmContext());
                if (!minor_compaction_task)
                    break;
                minor_compaction_task->prepare(dmContext(), wbs, reader);
                minor_compaction_task->commit(persisted_file_set, wbs);
            }
            wbs.writeRemoves();
            ASSERT_EQ(persisted_file_set->getRows(), total_rows_write);
            ASSERT_EQ(persisted_file_set->getDeletes(), 1);
        }
    }
}

TEST_F(DeltaValueSpaceTest, Restore)
{
    auto persisted_file_set = delta->getPersistedFileSet();
    size_t total_rows_write = 0;
    // write some column_file, flush and compact it
    {
        WriteBatches wbs(dmContext().storage_pool, dmContext().getWriteLimiter());
        {
            appendBlockToDeltaValueSpace(dmContext(), delta, total_rows_write, num_rows_write_per_batch);
            total_rows_write += num_rows_write_per_batch;
        }
        {
            appendColumnFileToDeltaValueSpace(dmContext(), delta, total_rows_write, num_rows_write_per_batch, wbs);
            total_rows_write += num_rows_write_per_batch;
        }
        {
            delta->appendDeleteRange(dmContext(), RowKeyRange::fromHandleRange(HandleRange(0, num_rows_write_per_batch)));
        }
        delta->flush(dmContext());
        delta->compact(dmContext());
        // after compaction, the two ColumnFileTiny must be compacted to a large column file, so there are just two column files left.
        ASSERT_EQ(delta->getColumnFileCount(), 2);
    }
    // write more data and flush it, and then there are two levels in the persisted_file_set
    {
        {
            appendBlockToDeltaValueSpace(dmContext(), delta, total_rows_write, num_rows_write_per_batch);
            total_rows_write += num_rows_write_per_batch;
        }
        delta->flush(dmContext());
        ASSERT_EQ(persisted_file_set->getColumnFileLevelCount(), 2);
        ASSERT_EQ(delta->getColumnFileCount(), 3);
        ASSERT_EQ(delta->getRows(), total_rows_write);
    }
    // check the column file order remain the same after restore
    {
        Blocks old_delta_blocks;
        {
            auto old_delta_snapshot = delta->createSnapshot(dmContext(), false, CurrentMetrics::DT_SnapshotOfRead);
            DeltaValueInputStream old_delta_stream(dmContext(), old_delta_snapshot, table_columns, RowKeyRange::newAll(false, 1));
            old_delta_stream.readPrefix();
            while (true)
            {
                auto block = old_delta_stream.read();
                if (!block)
                    break;
                old_delta_blocks.push_back(std::move(block));
            }
            old_delta_stream.readSuffix();
        }
        Blocks new_delta_blocks;
        {
            auto new_delta = delta->restore(dmContext(), RowKeyRange::newAll(false, 1), delta_id);
            auto new_delta_snapshot = new_delta->createSnapshot(dmContext(), false, CurrentMetrics::DT_SnapshotOfRead);
            DeltaValueInputStream new_delta_stream(dmContext(), new_delta_snapshot, table_columns, RowKeyRange::newAll(false, 1));
            new_delta_stream.readPrefix();
            while (true)
            {
                auto block = new_delta_stream.read();
                if (!block)
                    break;
                new_delta_blocks.push_back(std::move(block));
            }
            new_delta_stream.readSuffix();
        }
        assertBlocksEqual(old_delta_blocks, new_delta_blocks);
    }
}

TEST_F(DeltaValueSpaceTest, CheckHeadAndCloneTail)
{
    auto persisted_file_set = delta->getPersistedFileSet();
    size_t total_rows_write = 0;
    WriteBatches wbs(dmContext().storage_pool, dmContext().getWriteLimiter());
    // create three levels in persisted_file_set
    {
        // one column file in level 1
        {
            {appendBlockToDeltaValueSpace(dmContext(), delta, total_rows_write, num_rows_write_per_batch);
    total_rows_write += num_rows_write_per_batch;
}
{
    appendColumnFileToDeltaValueSpace(dmContext(), delta, total_rows_write, num_rows_write_per_batch, wbs);
    total_rows_write += num_rows_write_per_batch;
}
delta->flush(dmContext());
delta->compact(dmContext());
ASSERT_EQ(delta->getColumnFileCount(), 1);
ASSERT_EQ(persisted_file_set->getColumnFileLevelCount(), 2);
} // namespace tests
// one column files in level 2
{
    {
        appendBlockToDeltaValueSpace(dmContext(), delta, total_rows_write, num_rows_write_per_batch);
        total_rows_write += num_rows_write_per_batch;
    }
    {
        appendColumnFileToDeltaValueSpace(dmContext(), delta, total_rows_write, num_rows_write_per_batch, wbs);
        total_rows_write += num_rows_write_per_batch;
    }
    delta->flush(dmContext());
    // compact two level 0 files to level 1
    delta->compact(dmContext());
    // compact two level 1 files to level 2
    delta->compact(dmContext());
    ASSERT_EQ(delta->getColumnFileCount(), 1);
    ASSERT_EQ(persisted_file_set->getColumnFileLevelCount(), 3);
}
// one column files in level 1 and one column files in level 2
{
    {
        appendBlockToDeltaValueSpace(dmContext(), delta, total_rows_write, num_rows_write_per_batch);
        total_rows_write += num_rows_write_per_batch;
    }
    {
        appendColumnFileToDeltaValueSpace(dmContext(), delta, total_rows_write, num_rows_write_per_batch, wbs);
        total_rows_write += num_rows_write_per_batch;
    }
    delta->flush(dmContext());
    delta->compact(dmContext());
    ASSERT_EQ(delta->getColumnFileCount(), 2);
    ASSERT_EQ(persisted_file_set->getColumnFileLevelCount(), 3);
}
// one column files in level 0, one column files in level 1 and one column files in level 2
{
    {
        appendColumnFileToDeltaValueSpace(dmContext(), delta, total_rows_write, num_rows_write_per_batch, wbs);
        total_rows_write += num_rows_write_per_batch;
    }
    delta->flush(dmContext());
    ASSERT_EQ(delta->getColumnFileCount(), 3);
    ASSERT_EQ(persisted_file_set->getColumnFileLevelCount(), 3);
}
} // namespace DM

{
    auto snapshot = delta->createSnapshot(dmContext(), true, CurrentMetrics::DT_SnapshotOfRead);
    auto rows = snapshot->getRows();
    ASSERT_EQ(rows, total_rows_write);
    // write some more data after create snapshot
    {
        appendBlockToDeltaValueSpace(dmContext(), delta, total_rows_write, num_rows_write_per_batch);
        total_rows_write += num_rows_write_per_batch;
    }
    delta->flush(dmContext());
    {
        appendBlockToDeltaValueSpace(dmContext(), delta, total_rows_write, num_rows_write_per_batch);
    }
    auto [persisted_column_files, in_memory_files] = delta->checkHeadAndCloneTail(dmContext(), RowKeyRange::newAll(false, 1), snapshot->getColumnFilesInSnapshot(), wbs);
    wbs.writeLogAndData();

    ASSERT_EQ(persisted_column_files.size(), 1);
    ASSERT_EQ(persisted_column_files[0]->getRows(), num_rows_write_per_batch);
    ASSERT_EQ(in_memory_files.size(), 1);
    ASSERT_EQ(in_memory_files[0]->getRows(), num_rows_write_per_batch);
}
} // namespace DB
} // namespace tests
} // namespace DM
} // namespace DB
