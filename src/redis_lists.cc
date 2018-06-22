//  Copyright (c) 2017-present The blackwidow Authors.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.


#include <memory>

#include "blackwidow/util.h"
#include "src/redis_lists.h"
#include "src/lists_filter.h"
#include "src/scope_record_lock.h"
#include "src/scope_snapshot.h"

namespace blackwidow {

const rocksdb::Comparator* ListsDataKeyComparator() {
  static ListsDataKeyComparatorImpl ldkc;
  return &ldkc;
}

RedisLists::~RedisLists() {
  for (auto handle : handles_) {
    delete handle;
  }
}

Status RedisLists::Open(const rocksdb::Options& options,
                        const std::string& db_path) {
  rocksdb::Options ops(options);
  Status s = rocksdb::DB::Open(ops, db_path, &db_);
  if (s.ok()) {
    // Create column family
    rocksdb::ColumnFamilyHandle* cf;
    rocksdb::ColumnFamilyOptions cfo;
    cfo.comparator = ListsDataKeyComparator();
    s = db_->CreateColumnFamily(cfo, "data_cf", &cf);
    if (!s.ok()) {
      return s;
    }
    // Close DB
    delete cf;
    delete db_;
  }

  // Open
  rocksdb::DBOptions db_ops(options);
  rocksdb::ColumnFamilyOptions meta_cf_ops(options);
  rocksdb::ColumnFamilyOptions data_cf_ops(options);
  meta_cf_ops.compaction_filter_factory =
    std::make_shared<ListsMetaFilterFactory>();
  data_cf_ops.compaction_filter_factory =
    std::make_shared<ListsDataFilterFactory>(&db_, &handles_);
  data_cf_ops.comparator = ListsDataKeyComparator();
  std::vector<rocksdb::ColumnFamilyDescriptor> column_families;
  // Meta CF
  column_families.push_back(rocksdb::ColumnFamilyDescriptor(
      rocksdb::kDefaultColumnFamilyName, meta_cf_ops));
  // Data CF
  column_families.push_back(rocksdb::ColumnFamilyDescriptor(
      "data_cf", data_cf_ops));
  return rocksdb::DB::Open(db_ops, db_path, column_families, &handles_, &db_);
}

Status RedisLists::CompactRange(const rocksdb::Slice* begin,
                                 const rocksdb::Slice* end) {
  Status s = db_->CompactRange(default_compact_range_options_,
      handles_[0], begin, end);
  if (!s.ok()) {
    return s;
  }
  return db_->CompactRange(default_compact_range_options_,
      handles_[1], begin, end);
}

Status RedisLists::GetProperty(const std::string& property, std::string* out) {
  db_->GetProperty(property, out);
  return Status::OK();
}

Status RedisLists::ScanKeyNum(uint64_t* num) {

  uint64_t count = 0;
  rocksdb::ReadOptions iterator_options;
  const rocksdb::Snapshot* snapshot;
  ScopeSnapshot ss(db_, &snapshot);
  iterator_options.snapshot = snapshot;
  iterator_options.fill_cache = false;

  rocksdb::Iterator* iter = db_->NewIterator(iterator_options, handles_[0]);
  for (iter->SeekToFirst();
       iter->Valid();
       iter->Next()) {
    ParsedListsMetaValue parsed_lists_meta_value(iter->value());
    if (!parsed_lists_meta_value.IsStale()
      && parsed_lists_meta_value.count() != 0) {
      count++;
    }
  }
  *num = count;
  delete iter;
  return Status::OK();
}

Status RedisLists::ScanKeys(const std::string& pattern,
                              std::vector<std::string>* keys) {
  std::string key;
  rocksdb::ReadOptions iterator_options;
  const rocksdb::Snapshot* snapshot;
  ScopeSnapshot ss(db_, &snapshot);
  iterator_options.snapshot = snapshot;
  iterator_options.fill_cache = false;

  rocksdb::Iterator* iter = db_->NewIterator(iterator_options, handles_[0]);
  for (iter->SeekToFirst();
       iter->Valid();
       iter->Next()) {
    ParsedListsMetaValue parsed_lists_meta_value(iter->value());
    if (!parsed_lists_meta_value.IsStale()
      && parsed_lists_meta_value.count() != 0) {
      key = iter->key().ToString();
      if (StringMatch(pattern.data(), pattern.size(), key.data(), key.size(), 0)) {
        keys->push_back(key);
      }
    }
  }
  delete iter;
  return Status::OK();
}

Status RedisLists::LIndex(const Slice& key, int64_t index, std::string* element) {
  rocksdb::ReadOptions read_options;
  const rocksdb::Snapshot* snapshot;

  ScopeSnapshot ss(db_, &snapshot);
  read_options.snapshot = snapshot;
  std::string meta_value;
  Status s = db_->Get(read_options, handles_[0], key, &meta_value);
  if (s.ok()) {
    ParsedListsMetaValue parsed_lists_meta_value(&meta_value);
    int32_t version = parsed_lists_meta_value.version();
    if (parsed_lists_meta_value.IsStale()) {
      return Status::NotFound("Stale");
    } else if (parsed_lists_meta_value.count() == 0) {
      return Status::NotFound();
    } else {
      std::string tmp_element;
      uint64_t positive_direction_index = index >= 0 ?
            parsed_lists_meta_value.left_index() + index + 1 :
            parsed_lists_meta_value.right_index() + index;
      ListsDataKey lists_data_key(key, version, positive_direction_index);
      s = db_->Get(read_options, handles_[1], lists_data_key.Encode(), &tmp_element);
      if (s.ok()) {
        *element = tmp_element;
      }
    }
  }
  return s;
}

Status RedisLists::LInsert(const Slice& key,
                           const BeforeOrAfter& before_or_after,
                           const std::string& pivot,
                           const std::string& value,
                           int64_t* ret) {
  *ret = 0;
  rocksdb::WriteBatch batch;
  ScopeRecordLock l(lock_mgr_, key);
  std::string meta_value;
  Status s = db_->Get(default_read_options_, handles_[0], key, &meta_value);
  if (s.ok()) {
    ParsedListsMetaValue parsed_lists_meta_value(&meta_value);
    if (parsed_lists_meta_value.IsStale()) {
      *ret = 0;
      return Status::NotFound("Stale");
    } else if (parsed_lists_meta_value.count() == 0) {
      *ret = 0;
      return Status::NotFound();
    } else {
      bool find_pivot = false;
      uint64_t pivot_index = 0;
      uint32_t version = parsed_lists_meta_value.version();
      uint64_t current_index = parsed_lists_meta_value.left_index() + 1;
      rocksdb::Iterator* iter = db_->NewIterator(default_read_options_, handles_[1]);
      ListsDataKey start_data_key(key, version, current_index);
      for (iter->Seek(start_data_key.Encode());
           iter->Valid() && current_index < parsed_lists_meta_value.right_index();
           iter->Next(), current_index++) {
          if (strcmp(iter->value().ToString().data(), pivot.data()) == 0) {
            find_pivot = true;
            pivot_index = current_index;
            break;
          }
      }
      delete iter;
      if (!find_pivot) {
        *ret = -1;
        return Status::NotFound();
      } else {
        uint64_t target_index;
        std::vector<std::string> list_nodes;
        uint64_t mid_index = parsed_lists_meta_value.left_index()
            + (parsed_lists_meta_value.right_index() - parsed_lists_meta_value.left_index()) / 2;
        if (pivot_index <= mid_index) {
          target_index = (before_or_after == Before) ? pivot_index - 1 : pivot_index;
          current_index = parsed_lists_meta_value.left_index() + 1;
          rocksdb::Iterator* first_half_iter = db_->NewIterator(default_read_options_, handles_[1]);
          ListsDataKey start_data_key(key, version, current_index);
          for (first_half_iter->Seek(start_data_key.Encode());
               first_half_iter->Valid() && current_index <= pivot_index;
               first_half_iter->Next(), current_index++) {
              if (current_index == pivot_index) {
                if (before_or_after == After) {
                  list_nodes.push_back(first_half_iter->value().ToString());
                }
                break;
              }
              list_nodes.push_back(first_half_iter->value().ToString());
          }
          delete first_half_iter;

          current_index = parsed_lists_meta_value.left_index();
          for (const auto& node : list_nodes) {
            ListsDataKey lists_data_key(key, version, current_index++);
            batch.Put(handles_[1], lists_data_key.Encode(), node);
          }
          parsed_lists_meta_value.ModifyLeftIndex(1);
        } else {
          target_index = (before_or_after == Before) ? pivot_index : pivot_index + 1;
          current_index = pivot_index;
          rocksdb::Iterator* after_half_iter = db_->NewIterator(default_read_options_, handles_[1]);
          ListsDataKey start_data_key(key, version, current_index);
          for (after_half_iter->Seek(start_data_key.Encode());
               after_half_iter->Valid() && current_index < parsed_lists_meta_value.right_index();
               after_half_iter->Next(), current_index++) {
              if (current_index == pivot_index
                && before_or_after == BeforeOrAfter::After) {
                continue;
              }
              list_nodes.push_back(after_half_iter->value().ToString());
          }
          delete after_half_iter;

          current_index = target_index + 1;
          for (const auto& node : list_nodes) {
            ListsDataKey lists_data_key(key, version, current_index++);
            batch.Put(handles_[1], lists_data_key.Encode(), node);
          }
          parsed_lists_meta_value.ModifyRightIndex(1);
        }
        parsed_lists_meta_value.ModifyCount(1);
        batch.Put(handles_[0], key, meta_value);
        ListsDataKey lists_target_key(key, version, target_index);
        batch.Put(handles_[1], lists_target_key.Encode(), value);
        *ret = parsed_lists_meta_value.count();
        return db_->Write(default_write_options_, &batch);
      }
    }
  } else if (s.IsNotFound()){
    *ret = 0;
  }
  return s;
}

Status RedisLists::LLen(const Slice& key, uint64_t* len) {
  *len = 0;
  std::string meta_value;
  Status s = db_->Get(default_read_options_, handles_[0], key, &meta_value);
  if (s.ok()) {
    ParsedListsMetaValue parsed_lists_meta_value(&meta_value);
    if (parsed_lists_meta_value.IsStale()) {
      *len = 0;
      return Status::NotFound("Stale");
    } else if (parsed_lists_meta_value.count() == 0) {
      *len = 0;
      return Status::NotFound();
    } else {
      *len = parsed_lists_meta_value.count();
      return s;
    }
  }
  return s;
}

Status RedisLists::LPop(const Slice& key, std::string* element) {
  rocksdb::WriteBatch batch;
  ScopeRecordLock l(lock_mgr_, key);
  std::string meta_value;
  Status s = db_->Get(default_read_options_, handles_[0], key, &meta_value);
  if (s.ok()) {
    ParsedListsMetaValue parsed_lists_meta_value(&meta_value);
    if (parsed_lists_meta_value.IsStale()) {
      return Status::NotFound("Stale");
    } else if (parsed_lists_meta_value.count() == 0) {
      return Status::NotFound();
    } else {
      int32_t version = parsed_lists_meta_value.version();
      uint64_t first_node_index = parsed_lists_meta_value.left_index() + 1;
      ListsDataKey lists_data_key(key, version, first_node_index);
      s = db_->Get(default_read_options_, handles_[1], lists_data_key.Encode(), element);
      if (s.ok()) {
        batch.Delete(handles_[1], lists_data_key.Encode());
        parsed_lists_meta_value.ModifyCount(-1);
        parsed_lists_meta_value.ModifyLeftIndex(-1);
        batch.Put(handles_[0], key, meta_value);
        return db_->Write(default_write_options_, &batch);
      } else {
        return s;
      }
    }
  }
  return s;
}

Status RedisLists::LPush(const Slice& key,
                         const std::vector<std::string>& values,
                         uint64_t* ret) {
  *ret = 0;
  rocksdb::WriteBatch batch;
  ScopeRecordLock l(lock_mgr_, key);

  uint64_t index = 0;
  int32_t version = 0;
  std::string meta_value;
  Status s = db_->Get(default_read_options_, handles_[0], key, &meta_value);
  if (s.ok()) {
    ParsedListsMetaValue parsed_lists_meta_value(&meta_value);
    if (parsed_lists_meta_value.IsStale()) {
      version = parsed_lists_meta_value.InitialMetaValue();
    } else {
      version = parsed_lists_meta_value.version();
    }
    for (const auto& value : values) {
      index = parsed_lists_meta_value.left_index();
      parsed_lists_meta_value.ModifyLeftIndex(1);
      parsed_lists_meta_value.ModifyCount(1);
      ListsDataKey lists_data_key(key, version, index);
      batch.Put(handles_[1], lists_data_key.Encode(), value);
    }
    batch.Put(handles_[0], key, meta_value);
    *ret = parsed_lists_meta_value.count();
  } else if (s.IsNotFound()) {
    char str[8];
    EncodeFixed64(str, values.size());
    ListsMetaValue lists_meta_value(Slice(str, sizeof(uint64_t)));
    version = lists_meta_value.UpdateVersion();
    for (const auto& value : values) {
      index = lists_meta_value.left_index();
      lists_meta_value.ModifyLeftIndex(1);
      ListsDataKey lists_data_key(key, version, index);
      batch.Put(handles_[1], lists_data_key.Encode(), value);
    }
    batch.Put(handles_[0], key, lists_meta_value.Encode());
    *ret = lists_meta_value.right_index() - lists_meta_value.left_index() - 1;
  } else {
    return s;
  }
  return db_->Write(default_write_options_, &batch);
}

Status RedisLists::LPushx(const Slice& key, const Slice& value, uint64_t* len) {
  *len = 0;
  rocksdb::WriteBatch batch;
  ScopeRecordLock l(lock_mgr_, key);

  std::string meta_value;
  Status s = db_->Get(default_read_options_, handles_[0], key, &meta_value);
  if (s.ok()) {
    ParsedListsMetaValue parsed_lists_meta_value(&meta_value);
    if (parsed_lists_meta_value.IsStale()) {
      return Status::NotFound("Stale");
    } else if (parsed_lists_meta_value.count() == 0) {
      return Status::NotFound();
    } else {
      uint32_t version = parsed_lists_meta_value.version();
      uint64_t index = parsed_lists_meta_value.left_index();
      parsed_lists_meta_value.ModifyCount(1);
      parsed_lists_meta_value.ModifyLeftIndex(1);
      ListsDataKey lists_data_key(key, version, index);
      batch.Put(handles_[0], key, meta_value);
      batch.Put(handles_[1], lists_data_key.Encode(), value);
      *len = parsed_lists_meta_value.count();
      return db_->Write(default_write_options_, &batch);
    }
  }
  return s;
}

Status RedisLists::LRange(const Slice& key, int64_t start, int64_t stop,
                          std::vector<std::string>* ret) {
  rocksdb::ReadOptions read_options;
  const rocksdb::Snapshot* snapshot;

  ScopeSnapshot ss(db_, &snapshot);
  read_options.snapshot = snapshot;

  std::string meta_value;
  Status s = db_->Get(read_options, handles_[0], key, &meta_value);
  if (s.ok()) {
    ParsedListsMetaValue parsed_lists_meta_value(&meta_value);
    if (parsed_lists_meta_value.IsStale()) {
      return Status::NotFound("Stale");
    } else if (parsed_lists_meta_value.count() == 0) {
      return Status::NotFound();
    } else {
      int32_t version = parsed_lists_meta_value.version();
      uint64_t start_index = start >= 0 ?
                             parsed_lists_meta_value.left_index() + start + 1 :
                             parsed_lists_meta_value.right_index() + start;
      uint64_t stop_index = stop >= 0 ?
                            parsed_lists_meta_value.left_index() + stop + 1 :
                            parsed_lists_meta_value.right_index() + stop;
      if (start_index > stop_index) {
        return s;
      }
      if (start_index <= parsed_lists_meta_value.left_index()) {
        start_index = parsed_lists_meta_value.left_index() + 1;
      }
      if (stop_index >= parsed_lists_meta_value.right_index()) {
        stop_index = parsed_lists_meta_value.right_index() - 1;
      }
      rocksdb::Iterator* iter = db_->NewIterator(read_options,
              handles_[1]);
      ListsDataKey start_data_key(key, version, start_index);
      for (iter->Seek(start_data_key.Encode());
           iter->Valid() && start_index <= stop_index;
           iter->Next(), start_index++) {
        ret->push_back(iter->value().ToString());
      }
      delete iter;
      return s;
    }
  } else {
    return s;
  }
}

Status RedisLists::LRem(const Slice& key, int64_t count,
                        const Slice& value, uint64_t* ret) {
  *ret = 0;
  rocksdb::WriteBatch batch;
  ScopeRecordLock l(lock_mgr_, key);
  std::string meta_value;
  Status s = db_->Get(default_read_options_, handles_[0], key, &meta_value);
  if (s.ok()) {
    ParsedListsMetaValue parsed_lists_meta_value(&meta_value);
    if (parsed_lists_meta_value.IsStale()) {
      *ret = 0;
      return Status::NotFound("Stale");
    } else if (parsed_lists_meta_value.count() == 0) {
      *ret = 0;
      return Status::NotFound();
    } else {
      uint64_t current_index;
      std::vector<uint64_t> target_index;
      std::vector<uint64_t> delete_index;
      uint64_t rest = (count < 0) ? -count : count;
      uint32_t version = parsed_lists_meta_value.version();
      uint64_t start_index = parsed_lists_meta_value.left_index() + 1;
      uint64_t stop_index = parsed_lists_meta_value.right_index() - 1;
      ListsDataKey start_data_key(key, version, start_index);
      ListsDataKey stop_data_key(key, version, stop_index);
      rocksdb::Iterator* iter = db_->NewIterator(default_read_options_, handles_[1]);
      if (count >= 0) {
        current_index = start_index;
        for (iter->Seek(start_data_key.Encode());
             iter->Valid() && current_index <= stop_index && (!count || rest != 0);
             iter->Next(), current_index++) {
          if (strcmp(iter->value().ToString().data(), value.data()) == 0) {
            target_index.push_back(current_index);
            if (count != 0) {
              rest--;
            }
          }
        }
      } else {
        current_index = stop_index;
        for (iter->Seek(stop_data_key.Encode());
             iter->Valid() && current_index >= start_index && (!count || rest != 0);
             iter->Prev(), current_index--) {
          if (strcmp(iter->value().ToString().data(), value.data()) == 0) {
            target_index.push_back(current_index);
            if (count != 0) {
              rest--;
            }
          }
        }
      }
      if (target_index.empty()) {
        *ret = 0;
        return Status::NotFound();
      } else {
        rest = target_index.size();
        uint64_t sublist_left_index  = (count >= 0) ? target_index[0] : target_index[target_index.size() - 1];
        uint64_t sublist_right_index = (count >= 0) ? target_index[target_index.size() - 1] : target_index[0];
        uint64_t left_part_len = sublist_right_index - start_index;
        uint64_t right_part_len = stop_index - sublist_left_index;
        if (left_part_len <= right_part_len) {
          uint64_t left = sublist_right_index;
          current_index  = sublist_right_index;
          ListsDataKey sublist_right_key(key, version, sublist_right_index);
          for (iter->Seek(sublist_right_key.Encode());
               iter->Valid() && current_index >= start_index;
               iter->Prev(), current_index--) {
            if (!strcmp(iter->value().ToString().data(), value.data()) && rest > 0) {
              rest--;
            } else {
              ListsDataKey lists_data_key(key, version, left--);
              batch.Put(handles_[1], lists_data_key.Encode(), iter->value());
            }
          }
          uint64_t left_index = parsed_lists_meta_value.left_index();
          for (uint64_t idx = 0; idx < target_index.size(); ++idx) {
            delete_index.push_back(left_index + idx + 1);
          }
          parsed_lists_meta_value.ModifyLeftIndex(-target_index.size());
        } else {
          uint64_t right = sublist_left_index;
          current_index = sublist_left_index;
          ListsDataKey sublist_left_key(key, version, sublist_left_index);
          for (iter->Seek(sublist_left_key.Encode());
               iter->Valid() && current_index <= stop_index;
               iter->Next(), current_index++) {
            if (!strcmp(iter->value().ToString().data(), value.data()) && rest > 0) {
              rest--;
            } else {
              ListsDataKey lists_data_key(key, version, right++);
              batch.Put(handles_[1], lists_data_key.Encode(), iter->value());
            }
          }
          uint64_t right_index = parsed_lists_meta_value.right_index();
          for (uint64_t idx = 0; idx < target_index.size(); ++idx) {
            delete_index.push_back(right_index - idx - 1);
          }
          parsed_lists_meta_value.ModifyRightIndex(-target_index.size());
        }
        delete iter;
        parsed_lists_meta_value.ModifyCount(-target_index.size());
        batch.Put(handles_[0], key, meta_value);
        for (const auto& idx : delete_index) {
          ListsDataKey lists_data_key(key, version, idx);
          batch.Delete(handles_[1], lists_data_key.Encode());
        }
        *ret = target_index.size();
        return db_->Write(default_write_options_, &batch);
      }
    }
  } else if (s.IsNotFound()) {
    *ret = 0;
  }
  return s;
}

Status RedisLists::LSet(const Slice& key, int64_t index, const Slice& value) {
  ScopeRecordLock l(lock_mgr_, key);
  std::string meta_value;
  Status s = db_->Get(default_read_options_, handles_[0], key, &meta_value);
  if (s.ok()) {
    ParsedListsMetaValue parsed_lists_meta_value(&meta_value);
    if (parsed_lists_meta_value.IsStale()) {
      return Status::NotFound("Stale");
    } else if (parsed_lists_meta_value.count() == 0) {
      return Status::NotFound();
    } else {
      uint32_t version = parsed_lists_meta_value.version();
      uint64_t target_index = index >= 0 ?
        parsed_lists_meta_value.left_index() + index + 1
        : parsed_lists_meta_value.right_index() + index;
      if (target_index <= parsed_lists_meta_value.left_index()
        || target_index >= parsed_lists_meta_value.right_index()) {
        return Status::Corruption("index out of range");
      }
      ListsDataKey lists_data_key(key, version, target_index);
      return db_->Put(default_write_options_, handles_[1],
                      lists_data_key.Encode(), value);
    }
  }
  return s;
}

Status RedisLists::LTrim(const Slice& key, int64_t start, int64_t stop) {

  rocksdb::WriteBatch batch;
  ScopeRecordLock l(lock_mgr_, key);

  std::string meta_value;
  Status s = db_->Get(default_read_options_, handles_[0], key, &meta_value);
  if (s.ok()) {
    ParsedListsMetaValue parsed_lists_meta_value(&meta_value);
    int32_t version = parsed_lists_meta_value.version();
    if (parsed_lists_meta_value.IsStale()) {
      return Status::NotFound("Stale");
    } else if (parsed_lists_meta_value.count() == 0) {
      return Status::NotFound();
    } else {
      uint64_t origin_left_index = parsed_lists_meta_value.left_index() + 1;
      uint64_t origin_right_index = parsed_lists_meta_value.right_index() - 1;
      uint64_t sublist_left_index  = start >= 0 ?
                                     origin_left_index + start :
                                     origin_right_index + start + 1;
      uint64_t sublist_right_index = stop >= 0 ?
                                     origin_left_index + stop :
                                     origin_right_index + stop + 1;

      if (sublist_left_index > sublist_right_index) {
        parsed_lists_meta_value.InitialMetaValue();
        batch.Put(handles_[0], key, meta_value);
      } else {
        if (sublist_left_index < origin_left_index) {
          sublist_left_index = origin_left_index;
        }
        if (sublist_left_index > origin_right_index) {
          sublist_left_index = origin_right_index;
        }

        if (sublist_right_index < origin_left_index) {
          sublist_right_index = origin_left_index;
        }
        if (sublist_right_index > origin_right_index) {
          sublist_right_index = origin_right_index;
        }

        uint64_t delete_node_num = (sublist_left_index - origin_left_index)
          + (origin_right_index - sublist_right_index);
        parsed_lists_meta_value.ModifyLeftIndex(-(sublist_left_index - origin_left_index));
        parsed_lists_meta_value.ModifyRightIndex(-(origin_right_index - sublist_right_index));
        parsed_lists_meta_value.ModifyCount(-delete_node_num);
        batch.Put(handles_[0], key, meta_value);
        for (uint64_t idx = origin_left_index; idx < sublist_left_index; ++idx) {
          ListsDataKey lists_data_key(key, version, idx);
          batch.Delete(handles_[1], lists_data_key.Encode());
        }
        for (uint64_t idx = origin_right_index; idx > sublist_right_index; --idx) {
          ListsDataKey lists_data_key(key, version, idx);
          batch.Delete(handles_[1], lists_data_key.Encode());
        }
      }
    }
  } else {
    return s;
  }
  return db_->Write(default_write_options_, &batch);
}

Status RedisLists::RPop(const Slice& key, std::string* element) {
  rocksdb::WriteBatch batch;
  ScopeRecordLock l(lock_mgr_, key);

  std::string meta_value;
  Status s = db_->Get(default_read_options_, handles_[0], key, &meta_value);
  if (s.ok()) {
    ParsedListsMetaValue parsed_lists_meta_value(&meta_value);
    if (parsed_lists_meta_value.IsStale()) {
      return Status::NotFound("Stale");
    } else if (parsed_lists_meta_value.count() == 0) {
      return Status::NotFound();
    } else {
      int32_t version = parsed_lists_meta_value.version();
      uint64_t last_node_index = parsed_lists_meta_value.right_index() - 1;
      ListsDataKey lists_data_key(key, version, last_node_index);
      s = db_->Get(default_read_options_, handles_[1], lists_data_key.Encode(), element);
      if (s.ok()) {
        batch.Delete(handles_[1], lists_data_key.Encode());
        parsed_lists_meta_value.ModifyCount(-1);
        parsed_lists_meta_value.ModifyRightIndex(-1);
        batch.Put(handles_[0], key, meta_value);
        return db_->Write(default_write_options_, &batch);
      } else {
        return s;
      }
    }
  }
  return s;
}

Status RedisLists::RPoplpush(const Slice& source,
                             const Slice& destination,
                             std::string* element) {
  element->clear();
  Status s;
  rocksdb::WriteBatch batch;
  MultiScopeRecordLock l(lock_mgr_, {source.ToString(), destination.ToString()});
  if (!source.compare(destination)) {
    std::string meta_value;
    s = db_->Get(default_read_options_, handles_[0], source, &meta_value);
    if (s.ok()) {
      ParsedListsMetaValue parsed_lists_meta_value(&meta_value);
      if (parsed_lists_meta_value.IsStale()) {
        return Status::NotFound("Stale");
      } else if (parsed_lists_meta_value.count() == 0) {
        return Status::NotFound();
      } else {
        std::string target;
        int32_t version = parsed_lists_meta_value.version();
        uint64_t last_node_index = parsed_lists_meta_value.right_index() - 1;
        ListsDataKey lists_data_key(source, version, last_node_index);
        s = db_->Get(default_read_options_, handles_[1], lists_data_key.Encode(), &target);
        if (s.ok()) {
          *element = target;
          if (parsed_lists_meta_value.count() == 1) {
            return Status::OK();
          } else {
            uint64_t target_index = parsed_lists_meta_value.left_index();
            ListsDataKey lists_target_key(source, version, target_index);
            batch.Delete(handles_[1], lists_data_key.Encode());
            batch.Put(handles_[1], lists_target_key.Encode(), target);
            parsed_lists_meta_value.ModifyRightIndex(-1);
            parsed_lists_meta_value.ModifyLeftIndex(1);
            batch.Put(handles_[0], source, meta_value);
            return db_->Write(default_write_options_, &batch);
          }
        } else {
          return s;
        }
      }
    } else {
      return s;
    }
  }

  int32_t version;
  std::string target;
  std::string source_meta_value;
  s = db_->Get(default_read_options_, handles_[0], source, &source_meta_value);
  if (s.ok()) {
    ParsedListsMetaValue parsed_lists_meta_value(&source_meta_value);
    if (parsed_lists_meta_value.IsStale()) {
      return Status::NotFound("Stale");
    } else if(parsed_lists_meta_value.count() == 0) {
      return Status::NotFound();
    } else {
      version = parsed_lists_meta_value.version();
      uint64_t last_node_index = parsed_lists_meta_value.right_index() - 1;
      ListsDataKey lists_data_key(source, version, last_node_index);
      s = db_->Get(default_read_options_, handles_[1], lists_data_key.Encode(), &target);
      if (s.ok()) {
        batch.Delete(handles_[1], lists_data_key.Encode());
        parsed_lists_meta_value.ModifyCount(-1);
        parsed_lists_meta_value.ModifyRightIndex(-1);
        batch.Put(handles_[0], source, source_meta_value);
      } else {
        return s;
      }
    }
  } else {
    return s;
  }

  std::string destination_meta_value;
  s = db_->Get(default_read_options_, handles_[0], destination, &destination_meta_value);
  if (s.ok()) {
    ParsedListsMetaValue parsed_lists_meta_value(&destination_meta_value);
    if (parsed_lists_meta_value.IsStale()) {
      version = parsed_lists_meta_value.InitialMetaValue();
    } else {
      version = parsed_lists_meta_value.version();
    }
    uint64_t target_index = parsed_lists_meta_value.left_index();
    ListsDataKey lists_data_key(destination, version, target_index);
    batch.Put(handles_[1], lists_data_key.Encode(), target);
    parsed_lists_meta_value.ModifyCount(1);
    parsed_lists_meta_value.ModifyLeftIndex(1);
    batch.Put(handles_[0], destination, destination_meta_value);
  } else if (s.IsNotFound()) {
    char str[8];
    EncodeFixed64(str, 1);
    ListsMetaValue lists_meta_value(Slice(str, sizeof(uint64_t)));
    version = lists_meta_value.UpdateVersion();
    uint64_t target_index = lists_meta_value.left_index();
    ListsDataKey lists_data_key(destination, version, target_index);
    batch.Put(handles_[1], lists_data_key.Encode(), target);
    lists_meta_value.ModifyLeftIndex(1);
    batch.Put(handles_[0], destination, lists_meta_value.Encode());
  } else {
    return s;
  }

  s = db_->Write(default_write_options_, &batch);
  if (s.ok()) {
    *element = target;
  }
  return s;
}

Status RedisLists::RPush(const Slice& key,
                         const std::vector<std::string>& values,
                         uint64_t* ret) {
  *ret = 0;
  rocksdb::WriteBatch batch;

  uint64_t index = 0;
  int32_t version = 0;
  std::string meta_value;
  Status s = db_->Get(default_read_options_, handles_[0], key, &meta_value);
  if (s.ok()) {
    ParsedListsMetaValue parsed_lists_meta_value(&meta_value);
    if (parsed_lists_meta_value.IsStale()) {
      version = parsed_lists_meta_value.InitialMetaValue();
    } else {
      version = parsed_lists_meta_value.version();
    }
    for (const auto& value : values) {
      index = parsed_lists_meta_value.right_index();
      parsed_lists_meta_value.ModifyRightIndex(1);
      parsed_lists_meta_value.ModifyCount(1);
      ListsDataKey lists_data_key(key, version, index);
      batch.Put(handles_[1], lists_data_key.Encode(), value);
    }
    batch.Put(handles_[0], key, meta_value);
    *ret = parsed_lists_meta_value.count();
  } else if (s.IsNotFound()) {
    char str[8];
    EncodeFixed64(str, values.size());
    ListsMetaValue lists_meta_value(Slice(str, sizeof(uint64_t)));
    version = lists_meta_value.UpdateVersion();
    for (auto value : values) {
      index = lists_meta_value.right_index();
      lists_meta_value.ModifyRightIndex(1);
      ListsDataKey lists_data_key(key, version, index);
      batch.Put(handles_[1], lists_data_key.Encode(), value);
    }
    batch.Put(handles_[0], key, lists_meta_value.Encode());
    *ret = lists_meta_value.right_index() - lists_meta_value.left_index() - 1;
  } else {
    return s;
  }
  return db_->Write(default_write_options_, &batch);
}

Status RedisLists::RPushx(const Slice& key, const Slice& value, uint64_t* len) {
  *len = 0;
  rocksdb::WriteBatch batch;

  ScopeRecordLock l(lock_mgr_, key);
  std::string meta_value;
  Status s = db_->Get(default_read_options_, handles_[0], key, &meta_value);
  if (s.ok()) {
    ParsedListsMetaValue parsed_lists_meta_value(&meta_value);
    if (parsed_lists_meta_value.IsStale()) {
      return Status::NotFound("Stale");
    } else if (parsed_lists_meta_value.count() == 0) {
      return Status::NotFound();
    } else {
      uint32_t version = parsed_lists_meta_value.version();
      uint64_t index = parsed_lists_meta_value.right_index();
      parsed_lists_meta_value.ModifyCount(1);
      parsed_lists_meta_value.ModifyRightIndex(1);
      ListsDataKey lists_data_key(key, version, index);
      batch.Put(handles_[0], key, meta_value);
      batch.Put(handles_[1], lists_data_key.Encode(), value);
      *len = parsed_lists_meta_value.count();
      return db_->Write(default_write_options_, &batch);
    }
  }
  return s;
}

Status RedisLists::Expire(const Slice& key, int32_t ttl) {
  std::string meta_value;
  ScopeRecordLock l(lock_mgr_, key);
  Status s = db_->Get(default_read_options_, handles_[0], key, &meta_value);
  if (s.ok()) {
    ParsedListsMetaValue parsed_lists_meta_value(&meta_value);
    if (parsed_lists_meta_value.IsStale()) {
      return Status::NotFound("Stale");
    }
    if (ttl > 0) {
      parsed_lists_meta_value.SetRelativeTimestamp(ttl);
      s = db_->Put(default_write_options_, handles_[0], key, meta_value);
    } else {
      parsed_lists_meta_value.InitialMetaValue();
      s = db_->Put(default_write_options_, handles_[0], key, meta_value);
    }
  }
  return s;
}

Status RedisLists::Del(const Slice& key) {
  std::string meta_value;
  ScopeRecordLock l(lock_mgr_, key);
  Status s = db_->Get(default_read_options_, handles_[0], key, &meta_value);
  if (s.ok()) {
    ParsedListsMetaValue parsed_lists_meta_value(&meta_value);
    if (parsed_lists_meta_value.IsStale()) {
      return Status::NotFound("Stale");
    } else {
      parsed_lists_meta_value.InitialMetaValue();
      s = db_->Put(default_write_options_, handles_[0], key, meta_value);
    }
  }
  return s;
}

bool RedisLists::Scan(const std::string& start_key,
                      const std::string& pattern,
                      std::vector<std::string>* keys,
                      int64_t* count,
                      std::string* next_key) {
  std::string meta_key, meta_value;
  bool is_finish = true;
  rocksdb::ReadOptions iterator_options;
  const rocksdb::Snapshot* snapshot;
  ScopeSnapshot ss(db_, &snapshot);
  iterator_options.snapshot = snapshot;
  iterator_options.fill_cache = false;

  rocksdb::Iterator* it = db_->NewIterator(iterator_options, handles_[0]);

  it->Seek(start_key);
  while (it->Valid() && (*count) > 0) {
    ParsedListsMetaValue parsed_lists_meta_value(it->value());
    if (parsed_lists_meta_value.IsStale()) {
      it->Next();
      continue;
    } else {
      meta_key = it->key().ToString();
      if (StringMatch(pattern.data(), pattern.size(),
                         meta_key.data(), meta_key.size(), 0)) {
        keys->push_back(meta_key);
      }
      (*count)--;
      it->Next();
    }
  }

  if (it->Valid()) {
    *next_key = it->key().ToString();
    is_finish = false;
  } else {
    *next_key = "";
  }
  delete it;
  return is_finish;
}

Status RedisLists::Expireat(const Slice& key, int32_t timestamp) {
  std::string meta_value;
  ScopeRecordLock l(lock_mgr_, key);
  Status s = db_->Get(default_read_options_, handles_[0], key, &meta_value);
  if (s.ok()) {
    ParsedListsMetaValue parsed_lists_meta_value(&meta_value);
    if (parsed_lists_meta_value.IsStale()) {
      return Status::NotFound("Stale");
    } else {
      parsed_lists_meta_value.set_timestamp(timestamp);
      return db_->Put(default_write_options_, handles_[0], key, meta_value);
    }
  }
  return s;
}

Status RedisLists::Persist(const Slice& key) {
  std::string meta_value;
  ScopeRecordLock l(lock_mgr_, key);
  Status s = db_->Get(default_read_options_, handles_[0], key, &meta_value);
  if (s.ok()) {
    ParsedListsMetaValue parsed_lists_meta_value(&meta_value);
    if (parsed_lists_meta_value.IsStale()) {
      return Status::NotFound("Stale");
    } else {
      int32_t timestamp = parsed_lists_meta_value.timestamp();
      if (timestamp == 0) {
        return Status::NotFound("Not have an associated timeout");
      } else {
        parsed_lists_meta_value.set_timestamp(0);
        return db_->Put(default_write_options_, handles_[0], key, meta_value);
      }
    }
  }
  return s;
}

Status RedisLists::TTL(const Slice& key, int64_t* timestamp) {
  std::string meta_value;
  Status s = db_->Get(default_read_options_, handles_[0], key, &meta_value);
  if (s.ok()) {
    ParsedListsMetaValue parsed_lists_meta_value(&meta_value);
    if (parsed_lists_meta_value.IsStale()) {
      *timestamp = -2;
      return Status::NotFound("Stale");
    } else {
      *timestamp = parsed_lists_meta_value.timestamp();
      if (*timestamp == 0) {
        *timestamp = -1;
      } else {
        int64_t curtime;
        rocksdb::Env::Default()->GetCurrentTime(&curtime);
        *timestamp = *timestamp - curtime > 0 ? *timestamp - curtime : -1;
      }
    }
  } else if (s.IsNotFound()) {
    *timestamp = -2;
  }
  return s;
}

void RedisLists::ScanDatabase() {

  rocksdb::ReadOptions iterator_options;
  const rocksdb::Snapshot* snapshot;
  ScopeSnapshot ss(db_, &snapshot);
  iterator_options.snapshot = snapshot;
  iterator_options.fill_cache = false;

  printf("\n***************List Meta Data***************\n");
  auto meta_iter = db_->NewIterator(iterator_options, handles_[0]);
  for (meta_iter->SeekToFirst();
       meta_iter->Valid();
       meta_iter->Next()) {
    ParsedListsMetaValue parsed_lists_meta_value(meta_iter->value());
    printf("[key : %-30s] [count : %-10lu] [left index : %-10lu] [right index : %-10lu] [timestamp : %-10d] [version : %d]\n",
           meta_iter->key().ToString().c_str(),
           parsed_lists_meta_value.count(),
           parsed_lists_meta_value.left_index(),
           parsed_lists_meta_value.right_index(),
           parsed_lists_meta_value.timestamp(),
           parsed_lists_meta_value.version());
  }
  delete meta_iter;

  printf("\n***************List Node Data***************\n");
  auto data_iter = db_->NewIterator(iterator_options, handles_[1]);
  for (data_iter->SeekToFirst();
       data_iter->Valid();
       data_iter->Next()) {
    ParsedListsDataKey parsed_lists_data_key(data_iter->key());
    printf("[key : %-30s] [index : %-10lu] [data : %-20s] [version : %d]\n",
           parsed_lists_data_key.key().ToString().c_str(),
           parsed_lists_data_key.index(),
           data_iter->value().ToString().c_str(),
           parsed_lists_data_key.version());
  }
  delete data_iter;
}

}   //  namespace blackwidow

