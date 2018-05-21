//  Copyright (c) 2017-present The blackwidow Authors.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.

#include "src/redis_zsets.h"

#include <limits>

#include "iostream"
#include "src/util.h"
#include "src/zsets_filter.h"
#include "src/scope_record_lock.h"
#include "src/scope_snapshot.h"

namespace blackwidow {

rocksdb::Comparator* ZSetsScoreKeyComparator() {
  static ZSetsScoreKeyComparatorImpl zsets_score_key_compare;
  return &zsets_score_key_compare;
}

RedisZSets::~RedisZSets() {
  for (auto handle : handles_) {
    delete handle;
  }
}

Status RedisZSets::Open(const rocksdb::Options& options,
    const std::string& db_path) {
  rocksdb::Options ops(options);
  Status s = rocksdb::DB::Open(ops, db_path, &db_);
  if (s.ok()) {
    rocksdb::ColumnFamilyHandle *dcf = nullptr, *scf = nullptr;
    s = db_->CreateColumnFamily(rocksdb::ColumnFamilyOptions(), "data_cf", &dcf);
    if (!s.ok()) {
      return s;
    }
    rocksdb::ColumnFamilyOptions score_cf_ops;
    score_cf_ops.comparator = ZSetsScoreKeyComparator();
    s = db_->CreateColumnFamily(score_cf_ops, "score_cf", &scf);
    if (!s.ok()) {
      return s;
    }
    delete scf;
    delete dcf;
    delete db_;
  }

  rocksdb::DBOptions db_ops(options);
  rocksdb::ColumnFamilyOptions meta_cf_ops(options);
  rocksdb::ColumnFamilyOptions data_cf_ops(options);
  rocksdb::ColumnFamilyOptions score_cf_ops(options);
  meta_cf_ops.compaction_filter_factory =
    std::make_shared<ZSetsMetaFilterFactory>();
  data_cf_ops.compaction_filter_factory =
    std::make_shared<ZSetsDataFilterFactory>(&db_, &handles_);
  score_cf_ops.compaction_filter_factory =
    std::make_shared<ZSetsScoreFilterFactory>(&db_, &handles_);
  score_cf_ops.comparator = ZSetsScoreKeyComparator();

  std::vector<rocksdb::ColumnFamilyDescriptor> column_families;
  column_families.push_back(rocksdb::ColumnFamilyDescriptor(
        rocksdb::kDefaultColumnFamilyName, meta_cf_ops));
  column_families.push_back(rocksdb::ColumnFamilyDescriptor(
        "data_cf", data_cf_ops));
  column_families.push_back(rocksdb::ColumnFamilyDescriptor(
        "score_cf", score_cf_ops));
  return rocksdb::DB::Open(db_ops, db_path, column_families, &handles_, &db_);
}

Status RedisZSets::ZAdd(const Slice& key,
                        const std::vector<BlackWidow::ScoreMember>& score_members,
                        int32_t* ret) {
  *ret = 0;
  std::unordered_set<std::string> unique;
  std::vector<BlackWidow::ScoreMember> filtered_score_members;
  for (const auto& sm : score_members) {
    if (unique.find(sm.member) == unique.end()) {
      unique.insert(sm.member);
      filtered_score_members.push_back(sm);
    }
  }

  char score_buf[8];
  int32_t version = 0;
  std::string meta_value;
  rocksdb::WriteBatch batch;
  ScopeRecordLock l(lock_mgr_, key);
  Status s = db_->Get(default_read_options_, handles_[0], key, &meta_value);
  if (s.ok()) {
    bool is_stale = false;
    ParsedZSetsMetaValue parsed_zsets_meta_value(&meta_value);
    if (parsed_zsets_meta_value.IsStale()) {
      is_stale = true;
      version = parsed_zsets_meta_value.InitialMetaValue();
    } else {
      is_stale = false;
      version = parsed_zsets_meta_value.version();
    }

    int32_t new_add = 0;
    int32_t old_size = parsed_zsets_meta_value.count();
    std::string data_value;
    for (const auto& sm : filtered_score_members) {
      bool not_found = true;
      ZSetsDataKey zsets_data_key(key, version, sm.member);
      if (!is_stale) {
        s = db_->Get(default_read_options_, handles_[1], zsets_data_key.Encode(), &data_value);
        if (s.ok()) {
          not_found = false;
          uint64_t tmp = DecodeFixed64(data_value.data());
          const void* ptr_tmp = reinterpret_cast<const void*>(&tmp);
          double old_score = *reinterpret_cast<const double*>(ptr_tmp);
          if (old_score == sm.score) {
            continue;
          } else {
            ZSetsScoreKey zsets_score_key(key, version, old_score, sm.member);
            batch.Delete(handles_[2], zsets_score_key.Encode());
          }
        } else if (!s.IsNotFound()) {
          return s;
        }
      }

      const void* ptr_score = reinterpret_cast<const void*>(&sm.score);
      EncodeFixed64(score_buf, *reinterpret_cast<const uint64_t*>(ptr_score));
      batch.Put(handles_[1], zsets_data_key.Encode(), Slice(score_buf, sizeof(uint64_t)));

      ZSetsScoreKey zsets_score_key(key, version, sm.score, sm.member);
      batch.Put(handles_[2], zsets_score_key.Encode(), Slice());
      if (not_found) {
        new_add++;
      }
    }
    parsed_zsets_meta_value.set_count(old_size + new_add);
    batch.Put(handles_[0], key, meta_value);
    *ret = old_size + new_add;
  } else if (s.IsNotFound()) {
    char buf[4];
    EncodeFixed32(buf, filtered_score_members.size());
    ZSetsMetaValue zsets_meta_value(Slice(buf, sizeof(int32_t)));
    version = zsets_meta_value.UpdateVersion();
    batch.Put(handles_[0], key, zsets_meta_value.Encode());
    for (const auto& sm : filtered_score_members) {
      ZSetsDataKey zsets_data_key(key, version, sm.member);
      const void* ptr_score = reinterpret_cast<const void*>(&sm.score);
      EncodeFixed64(score_buf, *reinterpret_cast<const uint64_t*>(ptr_score));
      batch.Put(handles_[1], zsets_data_key.Encode(), Slice(score_buf, sizeof(uint64_t)));

      ZSetsScoreKey zsets_score_key(key, version, sm.score, sm.member);
      batch.Put(handles_[2], zsets_score_key.Encode(), Slice());
    }
    *ret = filtered_score_members.size();
  } else {
    return s;
  }
  return db_->Write(default_write_options_, &batch);
}


Status RedisZSets::ZCard(const Slice& key, int32_t* card) {
  *card = 0;
  std::string meta_value;

  Status s = db_->Get(default_read_options_, key, &meta_value);
  if (s.ok()) {
    ParsedZSetsMetaValue parsed_zsets_meta_value(&meta_value);
    if (parsed_zsets_meta_value.IsStale()) {
      *card = 0;
      return Status::NotFound("Stale");
    } else if (parsed_zsets_meta_value.count() == 0) {
      *card = 0;
      return Status::NotFound();
    } else {
      *card = parsed_zsets_meta_value.count();
    }
  }
  return s;
}

Status RedisZSets::ZCount(const Slice& key,
                          double min,
                          double max,
                          int32_t* ret) {
  *ret = 0;
  rocksdb::ReadOptions read_options;
  const rocksdb::Snapshot* snapshot = nullptr;

  std::string meta_value;
  ScopeSnapshot ss(db_, &snapshot);
  read_options.snapshot = snapshot;

  Status s = db_->Get(read_options, key, &meta_value);
  if (s.ok()) {
    ParsedZSetsMetaValue parsed_zsets_meta_value(&meta_value);
    if (parsed_zsets_meta_value.IsStale()) {
      return Status::NotFound("Stale");
    } else if (parsed_zsets_meta_value.count() == 0) {
      return Status::NotFound();
    } else {
      int32_t version = parsed_zsets_meta_value.version();
      int32_t cnt = 0;
      int32_t cur_index = 0;
      int32_t stop_index = parsed_zsets_meta_value.count() - 1;
      BlackWidow::ScoreMember score_member;
      ZSetsScoreKey zsets_score_key(key, version, std::numeric_limits<double>::lowest(), Slice());
      rocksdb::Iterator* iter = db_->NewIterator(read_options, handles_[2]);
      for (iter->Seek(zsets_score_key.Encode());
           iter->Valid() && cur_index <= stop_index;
           iter->Next(), ++cur_index) {
          ParsedZSetsScoreKey parsed_zsets_score_key(iter->key());
          if (min <= parsed_zsets_score_key.score()
            && parsed_zsets_score_key.score() <= max) {
            cnt++;
          } else if (parsed_zsets_score_key.score() >= max) {
            break;
          }
      }
      delete iter;
      *ret = cnt;
    }
  }
  return s;
}

Status RedisZSets::ZIncrby(const Slice& key,
                           const Slice& member,
                           double increment,
                           double* ret) {
  *ret = 0;
  double score = 0; 
  char score_buf[8];
  int32_t version = 0;
  std::string meta_value;
  rocksdb::WriteBatch batch;
  ScopeRecordLock l(lock_mgr_, key);
  Status s = db_->Get(default_read_options_, handles_[0], key, &meta_value);
  if (s.ok()) {
    ParsedZSetsMetaValue parsed_zsets_meta_value(&meta_value);
    if (parsed_zsets_meta_value.IsStale()) {
      version = parsed_zsets_meta_value.InitialMetaValue();
    } else {
      version = parsed_zsets_meta_value.version();
    }
    std::string data_value;
    ZSetsDataKey zsets_data_key(key, version, member);
    s = db_->Get(default_read_options_, handles_[1], zsets_data_key.Encode(), &data_value);
    if (s.ok()) {
      uint64_t tmp = DecodeFixed64(data_value.data());
      const void* ptr_tmp = reinterpret_cast<const void*>(&tmp);
      double old_score = *reinterpret_cast<const double*>(ptr_tmp);
      score = old_score + increment;
      ZSetsScoreKey zsets_score_key(key, version, old_score, member);
      batch.Delete(handles_[2], zsets_score_key.Encode());
    } else if (s.IsNotFound()) {
      score = increment;
      parsed_zsets_meta_value.ModifyCount(1);
      batch.Put(handles_[0], key, meta_value);
    } else {
      return s;
    }
  } else if (s.IsNotFound()) {
    char buf[8];
    EncodeFixed32(buf, 1);
    ZSetsMetaValue zsets_meta_value(Slice(buf, sizeof(int32_t)));
    version = zsets_meta_value.UpdateVersion();
    batch.Put(handles_[0], key, zsets_meta_value.Encode());
    score = increment;
  } else {
    return s;
  }
  ZSetsDataKey zsets_data_key(key, version, member);
  const void* ptr_score = reinterpret_cast<const void*>(&score);
  EncodeFixed64(score_buf, *reinterpret_cast<const uint64_t*>(ptr_score));
  batch.Put(handles_[1], zsets_data_key.Encode(), Slice(score_buf, sizeof(uint64_t)));

  ZSetsScoreKey zsets_score_key(key, version, score, member);
  batch.Put(handles_[2], zsets_score_key.Encode(), Slice());
  *ret = score;
  return db_->Write(default_write_options_, &batch);
}

Status RedisZSets::ZRange(const Slice& key,
                          int32_t start,
                          int32_t stop,
                          std::vector<BlackWidow::ScoreMember>* score_members) {
  score_members->clear();
  rocksdb::ReadOptions read_options;
  const rocksdb::Snapshot* snapshot = nullptr;

  std::string meta_value;
  ScopeSnapshot ss(db_, &snapshot);
  read_options.snapshot = snapshot;

  Status s = db_->Get(read_options, key, &meta_value);
  if (s.ok()) {
    ParsedZSetsMetaValue parsed_zsets_meta_value(&meta_value);
    if (parsed_zsets_meta_value.IsStale()) {
      return Status::NotFound("Stale");
    } else if (parsed_zsets_meta_value.count() == 0) {
      return Status::NotFound();
    } else {
      int32_t count = parsed_zsets_meta_value.count();
      int32_t version = parsed_zsets_meta_value.version();
      int32_t start_index = start >= 0 ? start : count + start;
      int32_t stop_index  = stop  >= 0 ? stop  : count + stop;
      start_index = start_index <= 0 ? 0 : start_index;
      stop_index = stop_index >= count ? count - 1 : stop_index;
      if (start_index > stop_index
        || start_index >= count
        || stop_index < 0) {
        return s;
      }
      int32_t cur_index = 0;
      BlackWidow::ScoreMember score_member;
      ZSetsScoreKey zsets_score_key(key, version, std::numeric_limits<double>::lowest(), Slice());
      rocksdb::Iterator* iter = db_->NewIterator(read_options, handles_[2]);
      for (iter->Seek(zsets_score_key.Encode());
           iter->Valid() && cur_index <= stop_index;
           iter->Next(), ++cur_index) {
        if (cur_index >= start_index) {
          ParsedZSetsScoreKey parsed_zsets_score_key(iter->key());
          score_member.score = parsed_zsets_score_key.score();
          score_member.member = parsed_zsets_score_key.member().ToString();
          score_members->push_back(score_member);
        }
      }
      delete iter;
    }
  }
  return s;
}

Status RedisZSets::ZRangebyscore(const Slice& key,
                                 double min,
                                 double max,
                                 bool left_close,
                                 bool right_close,
                                 std::vector<BlackWidow::ScoreMember>* score_members) {
  score_members->clear();
  rocksdb::ReadOptions read_options;
  const rocksdb::Snapshot* snapshot = nullptr;

  std::string meta_value;
  ScopeSnapshot ss(db_, &snapshot);
  read_options.snapshot = snapshot;
  Status s = db_->Get(read_options, handles_[0], key, &meta_value);
  if (s.ok()) {
    ParsedZSetsMetaValue parsed_zsets_meta_value(&meta_value);
    if (parsed_zsets_meta_value.IsStale()) {
      return Status::NotFound("Stale");
    } else if (parsed_zsets_meta_value.count() == 0) {
      return Status::NotFound();
    } else {
      int32_t version = parsed_zsets_meta_value.version();
      int32_t index = 0;
      int32_t stop_index = parsed_zsets_meta_value.count() - 1;
      BlackWidow::ScoreMember score_member;
      ZSetsScoreKey zsets_score_key(key, version, std::numeric_limits<double>::lowest(), Slice());
      rocksdb::Iterator* iter = db_->NewIterator(read_options, handles_[2]);
      for (iter->Seek(zsets_score_key.Encode());
           iter->Valid() && index <= stop_index;
           iter->Next(), ++index) {
        bool left_pass = false;
        bool right_pass = false;
        ParsedZSetsScoreKey parsed_zsets_score_key(iter->key());
        if ((left_close && min <= parsed_zsets_score_key.score())
          || (!left_close && min < parsed_zsets_score_key.score())) {
          left_pass = true;
        }
        if ((right_close && parsed_zsets_score_key.score() <= max)
          || (!right_close && parsed_zsets_score_key.score() < max)) {
          right_pass = true;
        }
        if (left_pass && right_pass) {
          score_member.score = parsed_zsets_score_key.score();
          score_member.member = parsed_zsets_score_key.member().ToString();
          score_members->push_back(score_member);
        }
        if (!right_pass) {
          break;
        }
      }
      delete iter;
    }
  }
  return s;
}

Status RedisZSets::ZRank(const Slice& key,
                         const Slice& member,
                         int32_t* rank) {
  *rank = -1;
  rocksdb::ReadOptions read_options;
  const rocksdb::Snapshot* snapshot = nullptr;

  std::string meta_value;
  ScopeSnapshot ss(db_, &snapshot);
  read_options.snapshot = snapshot;
  Status s = db_->Get(read_options, handles_[0], key, &meta_value);
  if (s.ok()) {
    ParsedZSetsMetaValue  parsed_zsets_meta_value(&meta_value);
    if (parsed_zsets_meta_value.IsStale()) {
      return Status::NotFound("Stale");
    } else if(parsed_zsets_meta_value.count() == 0) {
      return Status::NotFound();
    } else {
      bool found = false;
      int32_t version = parsed_zsets_meta_value.version();
      int32_t index = 0;
      int32_t stop_index = parsed_zsets_meta_value.count() - 1;
      BlackWidow::ScoreMember score_member;
      ZSetsScoreKey zsets_score_key(key, version, std::numeric_limits<double>::lowest(), Slice());
      rocksdb::Iterator* iter = db_->NewIterator(read_options, handles_[2]);
      for (iter->Seek(zsets_score_key.Encode());
           iter->Valid() && index <= stop_index;
           iter->Next(), ++index) {
          ParsedZSetsScoreKey parsed_zsets_score_key(iter->key());
          if (!parsed_zsets_score_key.member().compare(member)) {
            found = true;
            break;
          }
      }
      delete iter;
      if (found) {
        *rank = index;
        return Status::OK();
      } else {
        return Status::NotFound();
      }
    }
  }
  return s;
}

Status RedisZSets::ZRem(const Slice& key,
                        std::vector<std::string> members,
                        int32_t* ret) {
  *ret = 0;
  std::unordered_set<std::string> unique;
  std::vector<std::string> filtered_members;
  for (const auto& member : members) {
    if (unique.find(member) == unique.end()) {
      unique.insert(member);
      filtered_members.push_back(member);
    }
  }

  std::string meta_value;
  rocksdb::WriteBatch batch;
  ScopeRecordLock l(lock_mgr_, key);
  Status s = db_->Get(default_read_options_, handles_[0], key, &meta_value);
  if (s.ok()) {
    ParsedZSetsMetaValue parsed_zsets_meta_value(&meta_value);
    if (parsed_zsets_meta_value.IsStale()) {
      return Status::NotFound("Stale");
    } else if (parsed_zsets_meta_value.count() == 0) {
      return Status::NotFound();
    } else {
      int32_t del_cnt = 0;
      std::string data_value;
      int32_t version = parsed_zsets_meta_value.version();
      for (const auto& member : filtered_members) {
        ZSetsDataKey zsets_data_key(key, version, member);
        s = db_->Get(default_read_options_, handles_[1], zsets_data_key.Encode(), &data_value);
        if (s.ok()) {
          del_cnt++;
          uint64_t tmp = DecodeFixed64(data_value.data());
          const void* ptr_tmp = reinterpret_cast<const void*>(&tmp);
          double score = *reinterpret_cast<const double*>(ptr_tmp);
          batch.Delete(handles_[1], zsets_data_key.Encode());

          ZSetsScoreKey zsets_score_key(key, version, score, member);
          batch.Delete(handles_[2], zsets_score_key.Encode());
        } else if (!s.IsNotFound()) {
          return s;
        }
      }
      *ret = del_cnt;
      parsed_zsets_meta_value.ModifyCount(-del_cnt);
      batch.Put(handles_[0], key, meta_value);
    }
  } else {
    return s;
  }
  return db_->Write(default_write_options_, &batch);
}

Status RedisZSets::ZRemrangebyrank(const Slice& key,
                                   int32_t start,
                                   int32_t stop,
                                   int32_t* ret) {
  *ret = 0;
  std::string meta_value;
  rocksdb::WriteBatch batch;
  ScopeRecordLock l(lock_mgr_, key);
  Status s = db_->Get(default_read_options_, handles_[0], key, &meta_value);
  if (s.ok()) {
    ParsedZSetsMetaValue parsed_zsets_meta_value(&meta_value);
    if (parsed_zsets_meta_value.IsStale()) {
      return Status::NotFound("Stale");
    } else if (parsed_zsets_meta_value.count() == 0) {
      return Status::NotFound();
    } else {
      std::string member;
      int32_t del_cnt = 0;
      int32_t cur_index = 0;
      int32_t count = parsed_zsets_meta_value.count();
      int32_t version = parsed_zsets_meta_value.version();
      int32_t start_index = start >= 0 ? start : count + start;
      int32_t stop_index  = stop  >= 0 ? stop  : count + stop;
      start_index = start_index <= 0 ? 0 : start_index;
      stop_index = stop_index >= count ? count - 1 : stop_index;
      ZSetsScoreKey zsets_score_key(key, version, std::numeric_limits<double>::lowest(), Slice());
      rocksdb::Iterator* iter = db_->NewIterator(default_read_options_, handles_[2]);
      for (iter->Seek(zsets_score_key.Encode());
           iter->Valid() && cur_index <= stop_index;
           iter->Next(), ++cur_index) {
        if (cur_index >= start_index) {
          ParsedZSetsScoreKey parsed_zsets_score_key(iter->key());
          ZSetsDataKey zsets_data_key(key, version, parsed_zsets_score_key.member());
          batch.Delete(handles_[1], zsets_data_key.Encode());
          batch.Delete(handles_[2], iter->key());
          del_cnt++;
        }
      }
      *ret = del_cnt;
      parsed_zsets_meta_value.ModifyCount(-del_cnt);
      batch.Put(handles_[0], key, meta_value);
    }
  } else {
    return s;
  }
  return db_->Write(default_write_options_, &batch);
}

Status RedisZSets::ZRemrangebyscore(const Slice& key,
                                    double min,
                                    double max,
                                    int32_t* ret) {
  *ret = 0;
  std::string meta_value;
  rocksdb::WriteBatch batch;
  ScopeRecordLock l(lock_mgr_, key);
  Status s = db_->Get(default_read_options_, handles_[0], key, &meta_value);
  if (s.ok()) {
    ParsedZSetsMetaValue parsed_zsets_meta_value(&meta_value);
    if (parsed_zsets_meta_value.IsStale()) {
      return Status::NotFound("Stale");
    } else if (parsed_zsets_meta_value.count() == 0) {
      return Status::NotFound();
    } else {
      std::string member;
      int32_t del_cnt = 0;
      int32_t cur_index = 0;
      int32_t stop_index = parsed_zsets_meta_value.count() - 1;
      int32_t version = parsed_zsets_meta_value.version();
      ZSetsScoreKey zsets_score_key(key, version, std::numeric_limits<double>::lowest(), Slice());
      rocksdb::Iterator* iter = db_->NewIterator(default_read_options_, handles_[2]);
      for (iter->Seek(zsets_score_key.Encode());
           iter->Valid() && cur_index <= stop_index;
           iter->Next(), ++cur_index) {
        ParsedZSetsScoreKey parsed_zsets_score_key(iter->key());
        if (min <= parsed_zsets_score_key.score()
          && parsed_zsets_score_key.score() <= max) {
          ZSetsDataKey zsets_data_key(key, version, parsed_zsets_score_key.member());
          batch.Delete(handles_[1], zsets_data_key.Encode());
          batch.Delete(handles_[2], iter->key());
          del_cnt++;
        } else if (parsed_zsets_score_key.score() > max) {
          break;
        }
      }
      *ret = del_cnt;
      parsed_zsets_meta_value.ModifyCount(-del_cnt);
      batch.Put(handles_[0], key, meta_value);
    }
  } else {
    return s;
  }
  return db_->Write(default_write_options_, &batch);
}

Status RedisZSets::ZRevrange(const Slice& key,
                             int32_t start,
                             int32_t stop,
                             std::vector<BlackWidow::ScoreMember>* score_members) {
  score_members->clear();
  rocksdb::ReadOptions read_options;
  const rocksdb::Snapshot* snapshot = nullptr;

  std::string meta_value;
  ScopeSnapshot ss(db_, &snapshot);
  read_options.snapshot = snapshot;

  Status s = db_->Get(read_options, key, &meta_value);
  if (s.ok()) {
    ParsedZSetsMetaValue parsed_zsets_meta_value(&meta_value);
    if (parsed_zsets_meta_value.IsStale()) {
      return Status::NotFound("Stale");
    } else if (parsed_zsets_meta_value.count() == 0) {
      return Status::NotFound();
    } else {
      int32_t count = parsed_zsets_meta_value.count();
      int32_t version = parsed_zsets_meta_value.version();
      int32_t start_index = start >= 0 ? start : count + start;
      int32_t stop_index  = stop  >= 0 ? stop  : count + stop;
      start_index = start_index <= 0 ? 0 : start_index;
      stop_index = stop_index >= count ? count - 1 : stop_index;
      if (start_index > stop_index
        || start_index >= count
        || stop_index < 0) {
        return s;
      }
      int32_t cur_index = 0;
      std::vector<BlackWidow::ScoreMember> tmp_sms;
      BlackWidow::ScoreMember score_member;
      ZSetsScoreKey zsets_score_key(key, version, std::numeric_limits<double>::lowest(), Slice());
      rocksdb::Iterator* iter = db_->NewIterator(read_options, handles_[2]);
      for (iter->Seek(zsets_score_key.Encode());
           iter->Valid() && cur_index <= stop_index;
           iter->Next(), ++cur_index) {
        if (cur_index >= start_index) {
          ParsedZSetsScoreKey parsed_zsets_score_key(iter->key());
          score_member.score = parsed_zsets_score_key.score();
          score_member.member = parsed_zsets_score_key.member().ToString();
          tmp_sms.push_back(score_member);
        }
      }
      delete iter;
      score_members->assign(tmp_sms.rbegin(), tmp_sms.rend());
    }
  }
  return s;
}

Status RedisZSets::ZRevrangebyscore(const Slice& key,
                                    double min,
                                    double max,
                                    bool left_close,
                                    bool right_close,
                                    std::vector<BlackWidow::ScoreMember>* score_members) {
  score_members->clear();
  rocksdb::ReadOptions read_options;
  const rocksdb::Snapshot* snapshot = nullptr;

  std::string meta_value;
  ScopeSnapshot ss(db_, &snapshot);
  read_options.snapshot = snapshot;
  Status s = db_->Get(read_options, handles_[0], key, &meta_value);
  if (s.ok()) {
    ParsedZSetsMetaValue parsed_zsets_meta_value(&meta_value);
    if (parsed_zsets_meta_value.IsStale()) {
      return Status::NotFound("Stale");
    } else if (parsed_zsets_meta_value.count() == 0) {
      return Status::NotFound();
    } else {
      int32_t version = parsed_zsets_meta_value.version();
      int32_t left = parsed_zsets_meta_value.count();
      BlackWidow::ScoreMember score_member;
      ZSetsScoreKey zsets_score_key(key, version, std::numeric_limits<double>::max(), Slice());
      rocksdb::Iterator* iter = db_->NewIterator(read_options, handles_[2]);
      for (iter->SeekForPrev(zsets_score_key.Encode());
           iter->Valid() && left > 0;
           iter->Prev(), --left) {
        bool left_pass = false;
        bool right_pass = false;
        ParsedZSetsScoreKey parsed_zsets_score_key(iter->key());
        if ((left_close && min <= parsed_zsets_score_key.score())
          || (!left_close && min < parsed_zsets_score_key.score())) {
          left_pass = true;
        }
        if ((right_close && parsed_zsets_score_key.score() <= max)
          || (!right_close && parsed_zsets_score_key.score() < max)) {
          right_pass = true;
        }
        if (left_pass && right_pass) {
          score_member.score = parsed_zsets_score_key.score();
          score_member.member = parsed_zsets_score_key.member().ToString();
          score_members->push_back(score_member);
        }
        if (!left_pass) {
          break;
        }
      }
      delete iter;
    }
  }
  return s;
}

Status RedisZSets::ZRevrank(const Slice& key,
                            const Slice& member,
                            int32_t* rank) {
  *rank = -1;
  rocksdb::ReadOptions read_options;
  const rocksdb::Snapshot* snapshot = nullptr;

  std::string meta_value;
  ScopeSnapshot ss(db_, &snapshot);
  read_options.snapshot = snapshot;

  Status s = db_->Get(read_options, key, &meta_value);
  if (s.ok()) {
    ParsedZSetsMetaValue parsed_zsets_meta_value(&meta_value);
    if (parsed_zsets_meta_value.IsStale()) {
      return Status::NotFound("Stale");
    } else if (parsed_zsets_meta_value.count() == 0) {
      return Status::NotFound();
    } else {
      bool found = false;
      int32_t rev_index = 0;
      int32_t left = parsed_zsets_meta_value.count();
      int32_t version = parsed_zsets_meta_value.version();
      ZSetsScoreKey zsets_score_key(key, version, std::numeric_limits<double>::max(), Slice());
      rocksdb::Iterator* iter = db_->NewIterator(read_options, handles_[2]);
      for (iter->SeekForPrev(zsets_score_key.Encode());
           iter->Valid() && left >= 0;
           iter->Prev(), --left, ++rev_index) {
        ParsedZSetsScoreKey parsed_zsets_score_key(iter->key());
        if (!parsed_zsets_score_key.member().compare(member)) {
          found = true;
          break;
        }
      }
      delete iter;
      if (found) {
        *rank = rev_index;
      } else {
        return Status::NotFound();
      }
    }
  }
  return s;
}

Status RedisZSets::ZScore(const Slice& key, const Slice& member, double* score) {

  *score = 0;
  rocksdb::ReadOptions read_options;
  const rocksdb::Snapshot* snapshot = nullptr;

  std::string meta_value;
  ScopeSnapshot ss(db_, &snapshot);
  read_options.snapshot = snapshot;

  Status s = db_->Get(read_options, key, &meta_value);
  if (s.ok()) {
    ParsedZSetsMetaValue parsed_zsets_meta_value(&meta_value);
    int32_t version = parsed_zsets_meta_value.version();
    if (parsed_zsets_meta_value.IsStale()) {
      return Status::NotFound("Stale");
    } else if (parsed_zsets_meta_value.count() == 0) {
      return Status::NotFound();
    } else {
      std::string data_value;
      ZSetsDataKey zsets_data_key(key, version, member);
      s = db_->Get(read_options, handles_[1], zsets_data_key.Encode(), &data_value);
      if (s.ok()) {
        uint64_t tmp = DecodeFixed64(data_value.data());
        const void* ptr_tmp = reinterpret_cast<const void*>(&tmp);
        *score = *reinterpret_cast<const double*>(ptr_tmp);
      } else {
        return s;
      }
    }
  } else if (!s.IsNotFound()) {
    return s;
  }
  return s;
}

Status RedisZSets::ZUnionstore(const Slice& destination,
                               const std::vector<std::string>& keys,
                               const std::vector<double>& weights,
                               const BlackWidow::AGGREGATE agg,
                               int32_t* ret) {
  *ret = 0;
  rocksdb::WriteBatch batch;
  rocksdb::ReadOptions read_options;
  const rocksdb::Snapshot* snapshot = nullptr;

  int32_t version;
  std::string meta_value;
  BlackWidow::ScoreMember sm;
  ScopeSnapshot ss(db_, &snapshot);
  read_options.snapshot = snapshot;
  ScopeRecordLock l(lock_mgr_, destination);
  std::map<std::string, double> member_score_map;

  Status s;
  for (size_t idx = 0; idx < keys.size(); ++idx) {
    s = db_->Get(read_options, handles_[0], keys[idx], &meta_value);
    if (s.ok()) {
      ParsedZSetsMetaValue parsed_zsets_meta_value(&meta_value);
      if (!parsed_zsets_meta_value.IsStale()
        && parsed_zsets_meta_value.count() != 0) {
        int32_t cur_index = 0;
        int32_t stop_index = parsed_zsets_meta_value.count() - 1;
        double weight = idx < weights.size() ? weights[idx] : 1;
        version = parsed_zsets_meta_value.version();
        ZSetsScoreKey zsets_score_key(keys[idx], version, std::numeric_limits<double>::lowest(), Slice());
        rocksdb::Iterator* iter = db_->NewIterator(read_options, handles_[2]);
        for (iter->Seek(zsets_score_key.Encode());
             iter->Valid() && cur_index <= stop_index;
             iter->Next(), ++cur_index) {
          ParsedZSetsScoreKey parsed_zsets_score_key(iter->key());
          sm.score = parsed_zsets_score_key.score();
          sm.member = parsed_zsets_score_key.member().ToString();
          if (member_score_map.find(sm.member) == member_score_map.end()) {
            member_score_map[sm.member] = weight * sm.score;
          } else {
            double score = member_score_map[sm.member];
            switch (agg) {
              case BlackWidow::SUM: score += weight * sm.score; break;
              case BlackWidow::MIN: score  = std::min(score, weight * sm.score); break;
              case BlackWidow::MAX: score  = std::max(score, weight * sm.score); break;
            }
            member_score_map[sm.member] = score;
          }
        }
        delete iter;
      }
    } else if (!s.IsNotFound()) {
      return s;
    }
  }

  s = db_->Get(read_options, handles_[0], destination, &meta_value);
  if (s.ok()) {
    ParsedZSetsMetaValue parsed_zsets_meta_value(&meta_value);
    version = parsed_zsets_meta_value.InitialMetaValue();
    parsed_zsets_meta_value.set_count(member_score_map.size());
    batch.Put(handles_[0], destination, meta_value);
  } else {
    char buf[4];
    EncodeFixed32(buf, member_score_map.size());
    ZSetsMetaValue zsets_meta_value(Slice(buf, sizeof(int32_t)));
    version = zsets_meta_value.UpdateVersion();
    batch.Put(handles_[0], destination, zsets_meta_value.Encode());
  }

  char score_buf[8];
  for (const auto& sm : member_score_map) {
    ZSetsDataKey zsets_data_key(destination, version, sm.first);

    const void* ptr_score = reinterpret_cast<const void*>(&sm.second);
    EncodeFixed64(score_buf, *reinterpret_cast<const uint64_t*>(ptr_score));
    batch.Put(handles_[1], zsets_data_key.Encode(), Slice(score_buf, sizeof(uint64_t)));

    ZSetsScoreKey zsets_score_key(destination, version, sm.second, sm.first);
    batch.Put(handles_[2], zsets_score_key.Encode(), Slice());
  }
  *ret = member_score_map.size();
  return db_->Write(default_write_options_, &batch);
}

Status RedisZSets::ZInterstore(const Slice& destination,
                               const std::vector<std::string>& keys,
                               const std::vector<double>& weights,
                               const BlackWidow::AGGREGATE agg,
                               int32_t* ret) {
  if (keys.size() <= 0) {
    return Status::Corruption("ZInterstore invalid parameter, no keys");
  }

  *ret = 0;
  rocksdb::WriteBatch batch;
  rocksdb::ReadOptions read_options;
  const rocksdb::Snapshot* snapshot = nullptr;
  ScopeSnapshot ss(db_, &snapshot);
  read_options.snapshot = snapshot;
  ScopeRecordLock l(lock_mgr_, destination);

  std::string meta_value;
  int32_t version = 0;
  bool have_invalid_zsets = false;
  BlackWidow::ScoreMember item;
  std::vector<BlackWidow::KeyVersion> vaild_zsets;
  std::vector<BlackWidow::ScoreMember> score_members;
  std::vector<BlackWidow::ScoreMember> final_score_members;
  Status s;

  int32_t cur_index = 0;
  int32_t stop_index = 0;
  for (size_t idx = 0; idx < keys.size(); ++idx) {
    s = db_->Get(read_options, handles_[0], keys[idx], &meta_value);
    if (s.ok()) {
      ParsedZSetsMetaValue parsed_zsets_meta_value(&meta_value);
      if (parsed_zsets_meta_value.IsStale()
        || parsed_zsets_meta_value.count() == 0) {
        have_invalid_zsets = true;
      } else {
        vaild_zsets.push_back({keys[idx], parsed_zsets_meta_value.version()});
        if (idx == 0) {
          stop_index = parsed_zsets_meta_value.count() - 1;
        }
      }
    } else if (s.IsNotFound()){
      have_invalid_zsets = true;
    } else {
      return s;
    }
  }

  if (!have_invalid_zsets) {
    ZSetsScoreKey zsets_score_key(vaild_zsets[0].key, vaild_zsets[0].version, std::numeric_limits<double>::lowest(), Slice());
    rocksdb::Iterator* iter = db_->NewIterator(read_options, handles_[2]);
    for (iter->Seek(zsets_score_key.Encode());
         iter->Valid() && cur_index <= stop_index;
         iter->Next(), ++cur_index) {
      ParsedZSetsScoreKey parsed_zsets_score_key(iter->key());
      double score = parsed_zsets_score_key.score();
      std::string member = parsed_zsets_score_key.member().ToString();
      score_members.push_back({score, member});
    }
    delete iter;

    std::string data_value;
    for (const auto& sm : score_members) {
      bool reliable = true;
      item.member = sm.member;
      item.score = sm.score * (weights.size() > 0 ? weights[0] : 1);
      for (size_t idx = 1; idx < vaild_zsets.size(); ++idx) {
        double weight = idx < weights.size() ? weights[idx] : 1;
        ZSetsDataKey zsets_data_key(vaild_zsets[idx].key, vaild_zsets[idx].version, item.member);
        s = db_->Get(read_options, handles_[1], zsets_data_key.Encode(), &data_value);
        if (s.ok()) {
          uint64_t tmp = DecodeFixed64(data_value.data());
          const void* ptr_tmp = reinterpret_cast<const void*>(&tmp);
          double score = *reinterpret_cast<const double*>(ptr_tmp);
          switch (agg) {
            case BlackWidow::SUM: item.score += weight * score; break;
            case BlackWidow::MIN: item.score  = std::min(item.score, weight * score); break;
            case BlackWidow::MAX: item.score  = std::max(item.score, weight * score); break;
          }
        } else if (s.IsNotFound()) {
          reliable = false;
          break;
        } else {
          return s;
        }
      }
      if (reliable) {
        final_score_members.push_back(item);
      }
    }
  }

  s = db_->Get(read_options, handles_[0], destination, &meta_value);
  if (s.ok()) {
    ParsedZSetsMetaValue parsed_zsets_meta_value(&meta_value);
    version = parsed_zsets_meta_value.InitialMetaValue();
    parsed_zsets_meta_value.set_count(final_score_members.size());
    batch.Put(handles_[0], destination, meta_value);
  } else {
    char buf[4];
    EncodeFixed32(buf, final_score_members.size());
    ZSetsMetaValue zsets_meta_value(Slice(buf, sizeof(int32_t)));
    version = zsets_meta_value.UpdateVersion();
    batch.Put(handles_[0], destination, zsets_meta_value.Encode());

  }
  char score_buf[8];
  for (const auto& sm : final_score_members) {
    ZSetsDataKey zsets_data_key(destination, version, sm.member);

    const void* ptr_score = reinterpret_cast<const void*>(&sm.score);
    EncodeFixed64(score_buf, *reinterpret_cast<const uint64_t*>(ptr_score));
    batch.Put(handles_[1], zsets_data_key.Encode(), Slice(score_buf, sizeof(uint64_t)));

    ZSetsScoreKey zsets_score_key(destination, version, sm.score, sm.member);
    batch.Put(handles_[2], zsets_score_key.Encode(), Slice());
  }
  *ret = final_score_members.size();
  return db_->Write(default_write_options_, &batch);
}

Status RedisZSets::ZRangebylex(const Slice& key,
                               const Slice& min,
                               const Slice& max,
                               bool left_close,
                               bool right_close,
                               std::vector<std::string>* members) {

  members->clear();
  rocksdb::ReadOptions read_options;
  const rocksdb::Snapshot* snapshot = nullptr;

  std::string meta_value;
  ScopeSnapshot ss(db_, &snapshot);
  read_options.snapshot = snapshot;

  bool left_no_limit = !min.compare("-");
  bool right_not_limit = !max.compare("+");
  
  Status s = db_->Get(read_options, handles_[0], key, &meta_value);
  if (s.ok()) { 
    ParsedZSetsMetaValue parsed_zsets_meta_value(&meta_value);
    if (parsed_zsets_meta_value.IsStale()
      || parsed_zsets_meta_value.count() == 0) {
      return Status::NotFound();
    } else {
      int32_t version = parsed_zsets_meta_value.version();
      int32_t cur_index = 0;
      int32_t stop_index = parsed_zsets_meta_value.count() - 1;
      ZSetsDataKey zsets_data_key(key, version, Slice());
      rocksdb::Iterator* iter = db_->NewIterator(read_options, handles_[1]);
      for (iter->Seek(zsets_data_key.Encode());
           iter->Valid() && cur_index <= stop_index;
           iter->Next(), ++cur_index) {
        bool left_pass = false;
        bool right_pass = false;
        ParsedZSetsDataKey parsed_zsets_data_key(iter->key());
        Slice member = parsed_zsets_data_key.field();
        if (left_no_limit
          || (left_close && min.compare(member) <= 0)
          || (!left_close && min.compare(member) < 0)) {
          left_pass = true;
        }
        if (right_not_limit
          || (right_close && max.compare(member) >= 0)
          || (!right_close && max.compare(member) > 0)) {
          right_pass = true;
        }
        if (left_pass && right_pass) {
          members->push_back(member.ToString());
        }
        if (!right_pass) {
          break;
        }
      }
      delete iter;
    }
  } 
  return s;
}

Status RedisZSets::ZLexcount(const Slice& key,
                             const Slice& min,
                             const Slice& max,
                             bool left_close,
                             bool right_close,
                             int32_t* ret) {
  std::vector<std::string> members;
  Status s = ZRangebylex(key, min, max, left_close, right_close, &members);
  *ret = members.size();
  return s;
}

Status RedisZSets::ZRemrangebylex(const Slice& key,
                                  const Slice& min,
                                  const Slice& max,
                                  bool left_close,
                                  bool right_close,
                                  int32_t* ret) {
  *ret = 0;
  rocksdb::WriteBatch batch;
  rocksdb::ReadOptions read_options;
  const rocksdb::Snapshot* snapshot = nullptr;

  ScopeSnapshot ss(db_, &snapshot);
  read_options.snapshot = snapshot;
  ScopeRecordLock l(lock_mgr_, key);

  bool left_no_limit = !min.compare("-");
  bool right_not_limit = !max.compare("+");
  
  int32_t del_cnt = 0;
  std::string meta_value;
  Status s = db_->Get(read_options, handles_[0], key, &meta_value);
  if (s.ok()) { 
    ParsedZSetsMetaValue parsed_zsets_meta_value(&meta_value);
    if (parsed_zsets_meta_value.IsStale()
      || parsed_zsets_meta_value.count() == 0) {
      return Status::NotFound();
    } else {
      int32_t version = parsed_zsets_meta_value.version();
      int32_t cur_index = 0;
      int32_t stop_index = parsed_zsets_meta_value.count() - 1;
      ZSetsDataKey zsets_data_key(key, version, Slice());
      rocksdb::Iterator* iter = db_->NewIterator(read_options, handles_[1]);
      for (iter->Seek(zsets_data_key.Encode());
           iter->Valid() && cur_index <= stop_index;
           iter->Next(), ++cur_index) {
        bool left_pass = false;
        bool right_pass = false;
        ParsedZSetsDataKey parsed_zsets_data_key(iter->key());
        Slice member = parsed_zsets_data_key.field();
        if (left_no_limit
          || (left_close && min.compare(member) <= 0)
          || (!left_close && min.compare(member) < 0)) {
          left_pass = true;
        }
        if (right_not_limit
          || (right_close && max.compare(member) >= 0)
          || (!right_close && max.compare(member) > 0)) {
          right_pass = true;
        }
        if (left_pass && right_pass) {
          batch.Delete(handles_[1], iter->key());

          uint64_t tmp = DecodeFixed64(iter->value().data());
          const void* ptr_tmp = reinterpret_cast<const void*>(&tmp);
          double score = *reinterpret_cast<const double*>(ptr_tmp);
          ZSetsScoreKey zsets_score_key(key, version, score, member);
          batch.Delete(handles_[2], zsets_score_key.Encode());
          del_cnt++;
        }
        if (!right_pass) {
          break;
        }
      }
      delete iter;
    }
    if (del_cnt > 0) {
      parsed_zsets_meta_value.ModifyCount(-del_cnt);
      batch.Put(handles_[0], key, meta_value);
      *ret = del_cnt;
    }
  } else {
    return s;
  }
  return db_->Write(default_write_options_, &batch);
}

Status RedisZSets::Expire(const Slice& key, int32_t ttl) {
  std::string meta_value;
  ScopeRecordLock l(lock_mgr_, key);
  Status s = db_->Get(default_read_options_, key, &meta_value);
  if (s.ok()) {
    ParsedZSetsMetaValue parsed_zsets_meta_value(&meta_value);
    if (parsed_zsets_meta_value.IsStale()) {
      return Status::NotFound();
    }
    if (ttl > 0) {
      parsed_zsets_meta_value.SetRelativeTimestamp(ttl);
    } else {
      parsed_zsets_meta_value.InitialMetaValue();
    }
    s = db_->Put(default_write_options_, handles_[0], key, meta_value);
  }
  return s;
}

Status RedisZSets::Del(const Slice& key) {
  std::string meta_value;
  ScopeRecordLock l(lock_mgr_, key);
  Status s = db_->Get(default_read_options_, key, &meta_value);
  if (s.ok()) {
    ParsedZSetsMetaValue parsed_zsets_meta_value(&meta_value);
    if (parsed_zsets_meta_value.IsStale()) {
      return Status::NotFound();
    } else {
      parsed_zsets_meta_value.InitialMetaValue();
      s = db_->Put(default_write_options_, handles_[0], key, meta_value);
    }
  }
  return s;
}

bool RedisZSets::Scan(const std::string& start_key,
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
    ParsedZSetsMetaValue parsed_zsets_meta_value(it->value());
    if (parsed_zsets_meta_value.IsStale()) {
      it->Next();
      continue;
    } else {
      meta_key = it->key().ToString();
      meta_value = it->value().ToString();
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

Status RedisZSets::Expireat(const Slice& key, int32_t timestamp) {
  std::string meta_value;
  ScopeRecordLock l(lock_mgr_, key);
  Status s = db_->Get(default_read_options_, handles_[0], key, &meta_value);
  if (s.ok()) {
    ParsedZSetsMetaValue parsed_zsets_meta_value(&meta_value);
    if (parsed_zsets_meta_value.IsStale()) {
      return Status::NotFound("Stale");
    } else {
      parsed_zsets_meta_value.set_timestamp(timestamp);
      return db_->Put(default_write_options_, handles_[0], key, meta_value);
    }
  }
  return s;
}
Status RedisZSets::Persist(const Slice& key) {
  std::string meta_value;
  ScopeRecordLock l(lock_mgr_, key);
  Status s = db_->Get(default_read_options_, handles_[0], key, &meta_value);
  if (s.ok()) {
    ParsedZSetsMetaValue parsed_zsets_meta_value(&meta_value);
    if (parsed_zsets_meta_value.IsStale()) {
      return Status::NotFound("Stale");
    } else {
      int32_t timestamp = parsed_zsets_meta_value.timestamp();
      if (timestamp == 0) {
        return Status::NotFound("Not have an associated timeout");
      } else {
        parsed_zsets_meta_value.set_timestamp(0);
        return db_->Put(default_write_options_, handles_[0], key, meta_value);
      }
    }
  }
  return s;
}

Status RedisZSets::TTL(const Slice& key, int64_t* timestamp) {
  std::string meta_value;
  Status s = db_->Get(default_read_options_, handles_[0], key, &meta_value);
  if (s.ok()) {
    ParsedZSetsMetaValue parsed_zsets_meta_value(&meta_value);
    if (parsed_zsets_meta_value.IsStale()) {
      *timestamp = -2;
      return Status::NotFound("Stale");
    } else {
      *timestamp = parsed_zsets_meta_value.timestamp();
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

Status RedisZSets::CompactRange(const rocksdb::Slice* begin,
    const rocksdb::Slice* end) {
  Status s = db_->CompactRange(default_compact_range_options_,
          handles_[0], begin, end);
  if (!s.ok()) {
    return s;
  }
  s = db_->CompactRange(default_compact_range_options_,
          handles_[1], begin, end);
  if (!s.ok()) {
    return s;
  }
  return db_->CompactRange(default_compact_range_options_,
          handles_[2], begin, end);
}

} // blackwidow

