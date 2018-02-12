//  Copyright (c) 2017-present The blackwidow Authors.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.

#include "src/redis_setes.h"

#include <memory>

#include "src/util.h"
#include "src/setes_filter.h"
#include "src/scope_record_lock.h"
#include "src/scope_snapshot.h"

namespace blackwidow {

RedisSetes::~RedisSetes() {
  for (auto handle : handles_) {
    delete handle;
  }
}

Status RedisSetes::Open(const rocksdb::Options& options,
    const std::string& db_path) {
  rocksdb::Options ops(options);
  Status s = rocksdb::DB::Open(ops, db_path, &db_);
  if (s.ok()) {
    // create column family
    rocksdb::ColumnFamilyHandle* cf;
    s = db_->CreateColumnFamily(rocksdb::ColumnFamilyOptions(),
        "member_cf", &cf);
    if (!s.ok()) {
      return s;
    }
    // close DB
    delete cf;
    delete db_;
  }

  // Open
  rocksdb::DBOptions db_ops(options);
  rocksdb::ColumnFamilyOptions meta_cf_ops(options);
  rocksdb::ColumnFamilyOptions member_cf_ops(options);
  meta_cf_ops.compaction_filter_factory =
      std::make_shared<SetesMetaFilterFactory>();
  member_cf_ops.compaction_filter_factory =
      std::make_shared<SetesMemberFilterFactory>(&db_, &handles_);
  std::vector<rocksdb::ColumnFamilyDescriptor> column_families;
  // Meta CF
  column_families.push_back(rocksdb::ColumnFamilyDescriptor(
      rocksdb::kDefaultColumnFamilyName, meta_cf_ops));
  // Member CF
  column_families.push_back(rocksdb::ColumnFamilyDescriptor(
      "member_cf", member_cf_ops));
  return rocksdb::DB::Open(db_ops, db_path, column_families, &handles_, &db_);
}

Status RedisSetes::CompactRange(const rocksdb::Slice* begin,
    const rocksdb::Slice* end) {
  Status s = db_->CompactRange(default_compact_range_options_,
      handles_[0], begin, end);
  if (!s.ok()) {
    return s;
  }
  return db_->CompactRange(default_compact_range_options_,
      handles_[1], begin, end);
}

}  //  namespace blackwidow
