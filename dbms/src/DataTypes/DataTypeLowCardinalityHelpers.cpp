#include <Columns/ColumnArray.h>
#include <Columns/ColumnConst.h>
#include <Columns/ColumnTuple.h>
#include <Columns/ColumnLowCardinality.h>

#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeTuple.h>

#include <Common/assert_cast.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_COLUMN;
    extern const int TYPE_MISMATCH;
}

DataTypePtr recursiveRemoveLowCardinality(const DataTypePtr & type)
{
    if (!type)
        return type;

    if (const auto * low_cardinality_type = typeid_cast<const DataTypeLowCardinality *>(type.get()))
        return low_cardinality_type->getDictionaryType();

    return type;
}

ColumnPtr recursiveRemoveLowCardinality(const ColumnPtr & column)
{
    if (!column)
        return column;

    if (const auto * column_const = typeid_cast<const ColumnConst *>(column.get()))
    {
        const auto & nested = column_const->getDataColumnPtr();
        auto nested_no_lc = recursiveRemoveLowCardinality(nested);
        if (nested.get() == nested_no_lc.get())
            return column;

        return ColumnConst::create(nested_no_lc, column_const->size());
    }

    if (const auto * column_low_cardinality = typeid_cast<const ColumnLowCardinality *>(column.get()))
        return column_low_cardinality->convertToFullColumn();

    return column;
}

ColumnPtr recursiveTypeConversion(const ColumnPtr & column, const DataTypePtr & from_type, const DataTypePtr & to_type)
{
    if (!column)
        return column;

    if (from_type->equals(*to_type))
        return column;

    if (const auto * column_const = typeid_cast<const ColumnConst *>(column.get()))
    {
        const auto & nested = column_const->getDataColumnPtr();
        auto nested_no_lc = recursiveTypeConversion(nested, from_type, to_type);
        if (nested.get() == nested_no_lc.get())
            return column;

        return ColumnConst::create(nested_no_lc, column_const->size());
    }

    if (const auto * low_cardinality_type = typeid_cast<const DataTypeLowCardinality *>(from_type.get()))
    {
        if (to_type->equals(*low_cardinality_type->getDictionaryType()))
            return column->convertToFullColumnIfLowCardinality();
    }

    if (const auto * low_cardinality_type = typeid_cast<const DataTypeLowCardinality *>(to_type.get()))
    {
        if (from_type->equals(*low_cardinality_type->getDictionaryType()))
        {
            auto col = low_cardinality_type->createColumn();
            assert_cast<ColumnLowCardinality &>(*col).insertRangeFromFullColumn(*column, 0, column->size());
            return col;
        }
    }

    throw Exception("Cannot convert: " + from_type->getName() + " to " + to_type->getName(), ErrorCodes::TYPE_MISMATCH);
}

}
