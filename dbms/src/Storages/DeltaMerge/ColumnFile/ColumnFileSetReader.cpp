#include "ColumnFileSetReader.h"

#include <Storages/DeltaMerge/DMContext.h>

#include "ColumnInMemoryFile.h"
#include "ColumnTinyFile.h"
#include "Storages/DeltaMerge/ColumnFile/ColumnBigFile.h"
#include "Storages/DeltaMerge/ColumnFile/ColumnDeleteRangeFile.h"

namespace DB
{
namespace DM
{

std::pair<size_t, size_t> findColumnFile(const ColumnFiles & column_files, size_t rows_offset, size_t deletes_offset)
{
    size_t rows_count = 0;
    size_t deletes_count = 0;
    size_t column_file_index = 0;
    for (; column_file_index < column_files.size(); ++column_file_index)
    {
        if (rows_count == rows_offset && deletes_count == deletes_offset)
            return {column_file_index, 0};
        auto & column_file = column_files[column_file_index];

        if (column_file->isDeleteRange())
        {
            if (deletes_count == deletes_offset)
            {
                if (unlikely(rows_count != rows_offset))
                    throw Exception("rows_count and rows_offset are expected to be equal. pack_index: " + DB::toString(column_file_index)
                                    + ", pack_size: " + DB::toString(column_files.size()) + ", rows_count: " + DB::toString(rows_count)
                                    + ", rows_offset: " + DB::toString(rows_offset) + ", deletes_count: " + DB::toString(deletes_count)
                                    + ", deletes_offset: " + DB::toString(deletes_offset));
                return {column_file_index, 0};
            }
            ++deletes_count;
        }
        else
        {
            rows_count += column_file->getRows();
            if (rows_count > rows_offset)
            {
                if (unlikely(deletes_count != deletes_offset))
                    throw Exception("deletes_count and deletes_offset are expected to be equal. pack_index: " + DB::toString(column_file_index)
                                    + ", pack_size: " + DB::toString(column_files.size()) + ", rows_count: " + DB::toString(rows_count)
                                    + ", rows_offset: " + DB::toString(rows_offset) + ", deletes_count: " + DB::toString(deletes_count)
                                    + ", deletes_offset: " + DB::toString(deletes_offset));

                return {column_file_index, column_file->getRows() - (rows_count - rows_offset)};
            }
        }
    }
    if (rows_count != rows_offset || deletes_count != deletes_offset)
        throw Exception("illegal rows_offset and deletes_offset. pack_size: " + DB::toString(column_files.size())
                        + ", rows_count: " + DB::toString(rows_count) + ", rows_offset: " + DB::toString(rows_offset)
                        + ", deletes_count: " + DB::toString(deletes_count) + ", deletes_offset: " + DB::toString(deletes_offset));

    return {column_file_index, 0};
}

ColumnFileSetReader::ColumnFileSetReader(
    const DMContext & context,
    const ColumnFileSetSnapshotPtr & snapshot_,
    const ColumnDefinesPtr & col_defs_,
    const RowKeyRange & segment_range_)
    : snapshot(snapshot_)
    , col_defs(col_defs_)
    , segment_range(segment_range_)
{
    size_t total_rows = 0;
    for (auto & f : snapshot->getColumnFiles())
    {
        total_rows += f->getRows();
        column_file_rows.push_back(f->getRows());
        column_file_rows_end.push_back(total_rows);
        column_file_readers.push_back(f->getReader(context, snapshot->getStorageSnapshot(), col_defs));
    }
}

ColumnFileSetReaderPtr ColumnFileSetReader::createNewReader(const ColumnDefinesPtr & new_col_defs)
{
    auto new_reader = new ColumnFileSetReader();
    new_reader->snapshot = snapshot;
    new_reader->col_defs = new_col_defs;
    new_reader->segment_range = segment_range;
    new_reader->column_file_rows = column_file_rows;
    new_reader->column_file_rows_end = column_file_rows_end;

    for (auto & fr : column_file_readers)
        new_reader->column_file_readers.push_back(fr->createNewReader(new_col_defs));

    return std::shared_ptr<ColumnFileSetReader>(new_reader);
}

Block ColumnFileSetReader::readPKVersion(size_t offset, size_t limit)
{
    MutableColumns cols;
    for (size_t i = 0; i < 2; ++i)
        cols.push_back((*col_defs)[i].type->createColumn());
    readRows(cols, offset, limit, nullptr);
    Block block;
    for (size_t i = 0; i < 2; ++i)
    {
        const auto & cd = (*col_defs)[i];
        block.insert(ColumnWithTypeAndName(std::move(cols[i]), cd.type, cd.name, cd.id));
    }
    return block;
}

size_t ColumnFileSetReader::readRows(MutableColumns & output_columns, size_t offset, size_t limit, const RowKeyRange * range)
{
    // Note that DeltaMergeBlockInputStream could ask for rows with larger index than total_delta_rows,
    // because DeltaIndex::placed_rows could be larger than total_delta_rows.
    // Here is the example:
    //  1. Thread A create a delta snapshot with 10 rows. Now DeltaValueSnapshot::shared_delta_index->placed_rows == 10.
    //  2. Thread B insert 5 rows into the delta
    //  3. Thread B call Segment::ensurePlace to generate a new DeltaTree, placed_rows = 15, and update DeltaValueSnapshot::shared_delta_index = 15
    //  4. Thread A call Segment::ensurePlace, and DeltaValueReader::shouldPlace will return false. Because placed_rows(15) >= 10
    //  5. Thread A use the DeltaIndex with placed_rows = 15 to do the merge in DeltaMergeBlockInputStream
    //
    // So here, we should filter out those out-of-range rows.

    auto total_delta_rows = snapshot->getRows();

    auto start = std::min(offset, total_delta_rows);
    auto end = std::min(offset + limit, total_delta_rows);
    if (end == start)
        return 0;

    auto [start_pack_index, rows_start_in_start_pack] = locatePosByAccumulation(column_file_rows_end, start);
    auto [end_pack_index, rows_end_in_end_pack] = locatePosByAccumulation(column_file_rows_end, end);

    size_t actual_read = 0;
    for (size_t pack_index = start_pack_index; pack_index <= end_pack_index; ++pack_index)
    {
        size_t rows_start_in_pack = pack_index == start_pack_index ? rows_start_in_start_pack : 0;
        size_t rows_end_in_pack = pack_index == end_pack_index ? rows_end_in_end_pack : column_file_rows[pack_index];
        size_t rows_in_pack_limit = rows_end_in_pack - rows_start_in_pack;

        // Nothing to read.
        if (rows_start_in_pack == rows_end_in_pack)
            continue;

        auto & column_file_reader = column_file_readers[pack_index];
        actual_read += column_file_reader->readRows(output_columns, rows_start_in_pack, rows_in_pack_limit, range);
    }
    return actual_read;
}

void ColumnFileSetReader::getPlaceItems(BlockOrDeletes & place_items, size_t rows_begin, size_t deletes_begin, size_t rows_end, size_t deletes_end)
{
    /// Note that we merge the consecutive DeltaPackBlock together, which are seperated in groups by DeltaPackDelete and DeltePackFile.
    auto & column_files = snapshot->getColumnFiles();

    auto [start_pack_index, rows_start_in_start_pack] = findColumnFile(column_files, rows_begin, deletes_begin);
    auto [end_pack_index, rows_end_in_end_pack] = findColumnFile(column_files, rows_end, deletes_end);

    size_t block_rows_start = rows_begin;
    size_t block_rows_end = rows_begin;

    for (size_t pack_index = start_pack_index; pack_index < column_files.size() && pack_index <= end_pack_index; ++pack_index)
    {
        auto & pack = *column_files[pack_index];

        if (pack.isDeleteRange() || pack.isBigFile())
        {
            // First, compact the DeltaPackBlocks before this pack into one block.
            if (block_rows_end != block_rows_start)
            {
                auto block = readPKVersion(block_rows_start, block_rows_end - block_rows_start);
                place_items.emplace_back(std::move(block), block_rows_start);
            }

            // Second, take current pack.
            if (auto pack_delete = pack.tryToDeleteRange(); pack_delete)
            {
                place_items.emplace_back(pack_delete->getDeleteRange());
            }
            else if (pack.isBigFile() && pack.getRows())
            {
                auto block = readPKVersion(block_rows_end, pack.getRows());
                place_items.emplace_back(std::move(block), block_rows_end);
            }

            block_rows_end += pack.getRows();
            block_rows_start = block_rows_end;
        }
        else
        {
            // It is a DeltaPackBlock.
            size_t rows_start_in_pack = pack_index == start_pack_index ? rows_start_in_start_pack : 0;
            size_t rows_end_in_pack = pack_index == end_pack_index ? rows_end_in_end_pack : pack.getRows();

            block_rows_end += rows_end_in_pack - rows_start_in_pack;

            if (pack_index == column_files.size() - 1 || pack_index == end_pack_index)
            {
                // It is the last pack.
                if (block_rows_end != block_rows_start)
                {
                    auto block = readPKVersion(block_rows_start, block_rows_end - block_rows_start);
                    place_items.emplace_back(std::move(block), block_rows_start);
                }
                block_rows_start = block_rows_end;
            }
        }
    }
}

bool ColumnFileSetReader::shouldPlace(const DMContext & context,
                                      const RowKeyRange & relevant_range,
                                      UInt64 max_version,
                                      size_t placed_rows)
{
    auto & column_files = snapshot->getColumnFiles();
    auto [start_pack_index, rows_start_in_start_pack] = locatePosByAccumulation(column_file_rows_end, placed_rows);

    for (size_t pack_index = start_pack_index; pack_index < snapshot->getColumnFileCount(); ++pack_index)
    {
        auto & column_file = column_files[pack_index];

        // Always do place index if DeltaPackFile exists.
        if (column_file->isBigFile())
            return true;
        if (unlikely(column_file->isDeleteRange()))
            throw Exception("pack is delete range", ErrorCodes::LOGICAL_ERROR);

        size_t rows_start_in_pack = pack_index == start_pack_index ? rows_start_in_start_pack : 0;
        size_t rows_end_in_pack = column_file_rows[pack_index];

        auto & pack_reader = column_file_readers[pack_index];
        if (column_file->isInMemoryFile())
        {
            auto & dpb_reader = typeid_cast<ColumnInMemoryFileReader &>(*pack_reader);
            auto pk_column = dpb_reader.getPKColumn();
            auto version_column = dpb_reader.getVersionColumn();

            auto rkcc = RowKeyColumnContainer(pk_column, context.is_common_handle);
            auto & version_col_data = toColumnVectorData<UInt64>(version_column);

            for (auto i = rows_start_in_pack; i < rows_end_in_pack; ++i)
            {
                if (version_col_data[i] <= max_version && relevant_range.check(rkcc.getRowKeyValue(i)))
                    return true;
            }
        }
        else
        {
            auto & dpb_reader = typeid_cast<ColumnTinyFileReader &>(*pack_reader);
            auto pk_column = dpb_reader.getPKColumn();
            auto version_column = dpb_reader.getVersionColumn();

            auto rkcc = RowKeyColumnContainer(pk_column, context.is_common_handle);
            auto & version_col_data = toColumnVectorData<UInt64>(version_column);

            for (auto i = rows_start_in_pack; i < rows_end_in_pack; ++i)
            {
                if (version_col_data[i] <= max_version && relevant_range.check(rkcc.getRowKeyValue(i)))
                    return true;
            }
        }
    }

    return false;
}

}
}