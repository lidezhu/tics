#pragma once

#include "FlushColumnFileTask.h"
#include "Storages/DeltaMerge/ColumnFile/ColumnFile.h"
#include "Storages/DeltaMerge/ColumnFile/ColumnFileSetSnapshot.h"


namespace DB
{
namespace DM
{

class MemTableSet : public std::enable_shared_from_this<MemTableSet>
    , private boost::noncopyable
{
private:
    // may contain `ColumnInMemoryFile`, `ColumnDeleteRangeFile`, `ColumnBigFile`
    ColumnFiles column_files;

    std::atomic<size_t> rows = 0;
    std::atomic<size_t> bytes = 0;
    std::atomic<size_t> deletes = 0;

    Poco::Logger * log;

private:
    BlockPtr lastSchema();

    void appendColumnFileInner(const ColumnFilePtr & column_file);

public:
    MemTableSet(const ColumnFiles & in_memory_files = {})
        : column_files(in_memory_files)
        , log(&Poco::Logger::get("MemTableSet"))
    {}

    ColumnFiles cloneColumnFiles() { return column_files; }

    size_t getColumnFileCount() const { return column_files.size(); }
    size_t getRows() const { return rows; }
    size_t getBytes() const { return bytes; }
    size_t getDeletes() const { return deletes; }

    /// The following methods returning false means this operation failed, caused by other threads could have done
    /// some updates on this instance. E.g. this instance have been abandoned.
    /// Caller should try again from the beginning.
    void appendColumnFile(const ColumnFilePtr & column_file);

    void appendToCache(DMContext & dm_context, const Block & block, size_t offset, size_t limit);

    void appendDeleteRange(const RowKeyRange & delete_range);

    void ingestColumnFiles(const RowKeyRange & range, const ColumnFiles & column_files_, bool clear_data_in_range);

    /// Create a constant snapshot for read.
    /// Returns empty if this instance is abandoned, you should try again.
    ColumnFileSetSnapshotPtr createSnapshot();

    FlushColumnFileTaskPtr buildFlushTask(DMContext & context, size_t rows_offset, size_t deletes_offset);

    void removeColumnFilesInFlushTask(const FlushColumnFileTask & flush_task);
};

using MemTableSetPtr = std::shared_ptr<MemTableSet>;

}
}