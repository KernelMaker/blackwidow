//  Copyright (c) 2017-present The blackwidow Authors.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.

#ifndef SRC_REDIS_SETES_H_
#define SRC_REDIS_SETES_H_

#include <string>
#include <vector>

#include "src/redis.h"
#include "blackwidow/blackwidow.h"

namespace blackwidow {

class RedisSetes : public Redis {
  public:
    RedisSetes() = default;
    ~RedisSetes();

  // Common Commands
  virtual Status Open(const rocksdb::Options& options,
      const std::string& db_path) override;
  virtual Status CompactRange(const rocksdb::Slice* begin,
      const rocksdb::Slice* end) override;
  private:
    std::vector<rocksdb::ColumnFamilyHandle*> handles_;
};

}  //  namespace blackwidow
#endif  //  SRC_REDIS_SETES_H_
