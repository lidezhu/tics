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

#pragma once

#include <Storages/Transaction/TiKVRecordFormat.h>

#include <map>

namespace DB
{

struct CFKeyHasher
{
    size_t operator()(const std::pair<HandleID, Timestamp> & k) const noexcept
    {
        const static Timestamp mask = std::numeric_limits<Timestamp>::max() << 40 >> 40;
        size_t res = k.first << 24 | (k.second & mask);
        return res;
    }
};

struct RegionWriteCFDataTrait
{
    using DecodedWriteCFValue = RecordKVFormat::InnerDecodedWriteCFValue;
    using Key = std::pair<RawTiDBPK, Timestamp>;
    using Value = std::tuple<std::shared_ptr<const TiKVKey>, std::shared_ptr<const TiKVValue>, DecodedWriteCFValue>;
    using Map = std::map<Key, Value>;

    static std::pair<std::optional<Map::value_type>, size_t> genKVPair(TiKVKey && key, const DecodedTiKVKey & raw_key, TiKVValue && value)
    {
        auto decoded_val = RecordKVFormat::decodeWriteCfValue(value);
        if (!decoded_val)
            return std::pair(std::nullopt, 0);

        RawTiDBPK tidb_pk = RecordKVFormat::getRawTiDBPK(raw_key);
        Timestamp ts = RecordKVFormat::getTs(key);

        size_t memory_size = 0;
        memory_size += key.capacity();
        memory_size += raw_key.capacity();
        memory_size += value.capacity();
        memory_size += sizeof(decoded_val);
        memory_size += sizeof(RecordKVFormat::InnerDecodedWriteCFValue);
        memory_size += sizeof(decoded_val->short_value);
        memory_size += decoded_val->short_value->capacity();
        memory_size += sizeof(tidb_pk);
        memory_size += tidb_pk->capacity();
        memory_size += sizeof(ts);
        memory_size += sizeof(Key);
        memory_size += sizeof(Value);
        memory_size += sizeof(std::shared_ptr<const TiKVKey>);
        memory_size += sizeof(std::shared_ptr<const TiKVValue>);
        memory_size += sizeof(Map::value_type);

        return std::make_pair(Map::value_type(Key(std::move(tidb_pk), ts),
            Value(std::make_shared<const TiKVKey>(std::move(key)), std::make_shared<const TiKVValue>(std::move(value)),
                std::move(*decoded_val))), memory_size);
    }

    static const std::shared_ptr<const TiKVValue> & getRecordRawValuePtr(const Value & value) { return std::get<2>(value).short_value; }

    static UInt8 getWriteType(const Value & value) { return std::get<2>(value).write_type; }
};


struct RegionDefaultCFDataTrait
{
    using Key = std::pair<RawTiDBPK, Timestamp>;
    using Value = std::tuple<std::shared_ptr<const TiKVKey>, std::shared_ptr<const TiKVValue>>;
    using Map = std::map<Key, Value>;

    static std::pair<std::optional<Map::value_type>, size_t> genKVPair(TiKVKey && key, const DecodedTiKVKey & raw_key, TiKVValue && value)
    {
        RawTiDBPK tidb_pk = RecordKVFormat::getRawTiDBPK(raw_key);
        Timestamp ts = RecordKVFormat::getTs(key);
        size_t memory_size = 0;
        memory_size += key.capacity();
        memory_size += raw_key.capacity();
        memory_size += value.capacity();
        memory_size += sizeof(tidb_pk);
        memory_size += tidb_pk->capacity();
        memory_size += sizeof(ts);
        memory_size += sizeof(Key);
        memory_size += sizeof(Value);
        memory_size += sizeof(std::shared_ptr<const TiKVKey>);
        memory_size += sizeof(std::shared_ptr<const TiKVValue>);
        memory_size += sizeof(Map::value_type);
        return std::make_pair(Map::value_type(Key(std::move(tidb_pk), ts),
            Value(std::make_shared<const TiKVKey>(std::move(key)), std::make_shared<const TiKVValue>(std::move(value)))), memory_size);
    }

    static std::shared_ptr<const TiKVValue> getTiKVValue(const Map::const_iterator & it) { return std::get<1>(it->second); }
};

template <typename T>
class MyAlloc {
public:
    typedef T value_type;

    MyAlloc() =default;

    T* allocate (std::size_t num)
    {
        memory_allocated += num * sizeof(T);
        return static_cast<T*>(::operator new(num*sizeof(T)));
    }
    void deallocate (T* p, std::size_t num)
    {
        memory_allocated -= num * sizeof(T);
        ::operator delete(p);
    }

    size_t getMemoryAllocatedSize() noexcept { return memory_allocated; }

private:
    size_t memory_allocated = 0;
};

template <typename T1, typename T2>
bool operator== (const MyAlloc<T1>&, const MyAlloc<T2>&) noexcept { return false; }

template <typename T1, typename T2>
bool operator!= (const MyAlloc<T1>&, const MyAlloc<T2>&) noexcept { return true; }

struct RegionLockCFDataTrait
{
    struct Key
    {
        std::shared_ptr<const TiKVKey> key;
        std::string_view view;
        struct Hash
        {
            size_t operator()(const Key & x) const { return std::hash<std::string_view>()(x.view); }
        };
        bool operator==(const Key & tar) const { return view == tar.view; }
    };
    using DecodedLockCFValue = RecordKVFormat::DecodedLockCFValue;
    using Value = std::tuple<std::shared_ptr<const TiKVKey>, std::shared_ptr<const TiKVValue>, std::shared_ptr<const DecodedLockCFValue>>;
    using Map = std::unordered_map<Key, Value, Key::Hash, std::equal_to<Key>, MyAlloc<std::pair<const Key, Value>>>;

    static Map::value_type genKVPair(TiKVKey && key_, TiKVValue && value_)
    {
        auto key = std::make_shared<const TiKVKey>(std::move(key_));
        auto value = std::make_shared<const TiKVValue>(std::move(value_));
        return {{key, std::string_view(key->data(), key->dataSize())},
            Value{key, value, std::make_shared<const DecodedLockCFValue>(key, value)}};
    }
};

} // namespace DB
