//  Copyright (c) 2017-present The blackwidow Authors.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.

#include <gtest/gtest.h>
#include <thread>
#include <iostream>

#include "blackwidow/blackwidow.h"

class HashesTest : public ::testing::Test {
 public:
  HashesTest() {
    options.create_if_missing = true;
    s = db.Open(options, "./db");
  }
  virtual ~HashesTest() { }

  static void SetUpTestCase() { }
  static void TearDownTestCase() { }

  blackwidow::Options options;
  blackwidow::BlackWidow db;
  blackwidow::Status s;
};

// HSet
TEST_F(HashesTest, HSetTest) {
  int32_t ret;
  // If field is a new field in the hash and value was set.
  s = db.HSet("HSET_TEST_KEY", "HSET_TEST_FIELD", "HSET_TEST_VALUE", &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 1);

  // If field already exists in the hash and the value was updated.
  s = db.HSet("HSET_TEST_KEY", "HSET_TEST_FIELD", "HSET_TEST_NEW_VALUE", &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 0);
}

// HGet
TEST_F(HashesTest, HGetTest) {
  std::string value;
  s = db.HGet("HSET_TEST_KEY", "HSET_TEST_FIELD", &value);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(value, "HSET_TEST_NEW_VALUE");

  // If key does not exist.
  s = db.HGet("HSET_NOT_EXIST_KEY", "HSET_TEST_FIELD", &value);
  ASSERT_TRUE(s.IsNotFound());

  // If field is not present in the hash
  s = db.HGet("HSET_TEST_KEY", "HSET_NOT_EXIST_FIELD", &value);
  ASSERT_TRUE(s.IsNotFound());
}

// HExists
TEST_F(HashesTest, HExistsTest) {
  int32_t ret;
  s = db.HSet("HEXIST_KEY", "HEXIST_FIELD", "HEXIST_VALUE", &ret);
  ASSERT_TRUE(s.ok());

  s = db.HExists("HEXIST_KEY", "HEXIST_FIELD");
  ASSERT_TRUE(s.ok());

  // If key does not exist.
  s = db.HExists("HEXIST_NOT_EXIST_KEY", "HEXIST_FIELD");
  ASSERT_TRUE(s.IsNotFound());

  // If field is not present in the hash
  s = db.HExists("HEXIST_KEY", "HEXIST_NOT_EXIST_FIELD");
  ASSERT_TRUE(s.IsNotFound());
}

// HIncrby
TEST_F(HashesTest, HIncrby) {
  int32_t ret;
  int64_t value;

  // If the hash field contains a string that can not be
  // represented as integer
  s = db.HSet("HINCRBY_KEY", "HINCRBY_STR_FIELD", "HINCRBY_VALEU", &ret);
  ASSERT_TRUE(s.ok());
  s = db.HIncrby("HINCRBY_KEY", "HINCRBY_STR_FIELD", 100, &value);
  ASSERT_TRUE(s.IsInvalidArgument());

  // If field does not exist the value is set to 0 before the
  // operation is performed
  s = db.HIncrby("HINCRBY_KEY", "HINCRBY_NOT_EXIST_FIELD", 100, &value);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(value, 100);

  s = db.HSet("HINCRBY_KEY", "HINCRBY_NUM_FIELD", "100", &ret);
  ASSERT_TRUE(s.ok());

  // Positive test
  s = db.HIncrby("HINCRBY_KEY", "HINCRBY_NUM_FIELD", 100, &value);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(value, 200);

  // Negative test
  s = db.HIncrby("HINCRBY_KEY", "HINCRBY_NUM_FIELD", -100, &value);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(value, 100);

  // Larger than the maximum number 9223372036854775807
  s = db.HSet("HINCRBY_KEY", "HINCRBY_NUM_FIELD", "10", &ret);
  ASSERT_TRUE(s.ok());
  s = db.HIncrby("HINCRBY_KEY", "HINCRBY_NUM_FIELD", 9223372036854775807, &value);
  ASSERT_TRUE(s.IsInvalidArgument());

  // Less than the minimum number -9223372036854775808
  s = db.HSet("HINCRBY_KEY", "HINCRBY_NUM_FIELD", "-10", &ret);
  ASSERT_TRUE(s.ok());
  s = db.HIncrby("HINCRBY_KEY", "HINCRBY_NUM_FIELD", -9223372036854775807, &value);
  ASSERT_TRUE(s.IsInvalidArgument());
}

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

