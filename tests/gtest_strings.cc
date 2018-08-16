//  Copyright (c) 2017-present The blackwidow Authors.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.

#include <gtest/gtest.h>
#include <thread>
#include <iostream>

#include "blackwidow/blackwidow.h"

using namespace blackwidow;

class StringsTest : public ::testing::Test {
 public:
  StringsTest() {
    std::string path = "./db/strings";
    if (access(path.c_str(), F_OK)) {
      mkdir(path.c_str(), 0755);
    }
    options.create_if_missing = true;
    s = db.Open(options, path);
  }
  virtual ~StringsTest() { }

  static void SetUpTestCase() { }
  static void TearDownTestCase() { }

  blackwidow::Options options;
  blackwidow::BlackWidow db;
  blackwidow::Status s;
};

static bool make_expired(blackwidow::BlackWidow *const db,
                         const Slice& key) {
  std::map<blackwidow::DataType, rocksdb::Status> type_status;
  int ret = db->Expire(key, 1, &type_status);
  if (!ret || !type_status[blackwidow::DataType::kStrings].ok()) {
    return false;
  }
  std::this_thread::sleep_for(std::chrono::milliseconds(2000));
  return true;
}

static bool string_ttl(blackwidow::BlackWidow *const db,
                       const Slice& key, int32_t* ttl) {
  std::map<blackwidow::DataType, int64_t> type_ttl;
  std::map<blackwidow::DataType, Status> type_status;
  type_ttl = db->TTL(key, &type_status);
  for (const auto& item : type_status) {
    if (item.second != Status::OK()
      && item.second != Status::NotFound()) {
      return false;
    }
  }
  if (type_ttl.find(blackwidow::DataType::kStrings) == type_ttl.end()) {
    *ttl = -1;
    return false;
  } else {
    *ttl = type_ttl[blackwidow::DataType::kStrings];
    return true;
  }
}

// Append
TEST_F(StringsTest, AppendTest) {
  int32_t ret;
  std::string value;
  s = db.Append("APPEND_KEY", "HELLO", &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 5);

  s = db.Append("APPEND_KEY", " WORLD", &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 11);

  s = db.Get("APPEND_KEY", &value);
  ASSERT_STREQ(value.c_str(), "HELLO WORLD");
}

// BitCount
TEST_F(StringsTest, BitCountTest) {
  int32_t ret;

  // ***************** Group 1 Test *****************
  s = db.Set("GP1_BITCOUNT_KEY", "foobar");
  ASSERT_TRUE(s.ok());

  // Not have offset
  s = db.BitCount("GP1_BITCOUNT_KEY", 0, -1, &ret, false);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 26);

  // Have offset
  s = db.BitCount("GP1_BITCOUNT_KEY", 0, 0, &ret, true);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 4);
  s = db.BitCount("GP1_BITCOUNT_KEY", 1, 1, &ret, true);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 6);
}

// BitOp
TEST_F(StringsTest, BitOpTest) {
  int64_t ret;
  std::string value;
  s = db.Set("BITOP_KEY1", "FOOBAR");
  ASSERT_TRUE(s.ok());
  s = db.Set("BITOP_KEY2", "ABCDEF");
  ASSERT_TRUE(s.ok());
  s = db.Set("BITOP_KEY3", "BLACKWIDOW");
  ASSERT_TRUE(s.ok());
  std::vector<std::string> src_keys {"BITOP_KEY1", "BITOP_KEY2", "BITOP_KEY3"};

  // AND
  s = db.BitOp(blackwidow::BitOpType::kBitOpAnd,
               "BITOP_DESTKEY", src_keys, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 10);
  s = db.Get("BITOP_DESTKEY", &value);
  ASSERT_STREQ(value.c_str(), "@@A@AB\x00\x00\x00\x00");

  // OR
  s = db.BitOp(blackwidow::BitOpType::kBitOpOr,
               "BITOP_DESTKEY", src_keys, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 10);
  s = db.Get("BITOP_DESTKEY", &value);
  ASSERT_STREQ(value.c_str(), "GOOGOWIDOW");

  // XOR
  s = db.BitOp(blackwidow::BitOpType::kBitOpXor,
               "BITOP_DESTKEY", src_keys, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 10);
  s = db.Get("BITOP_DESTKEY", &value);
  ASSERT_STREQ(value.c_str(), "EAMEOCIDOW");

  // NOT
  std::vector<std::string> not_keys {"BITOP_KEY1"};
  s = db.BitOp(blackwidow::BitOpType::kBitOpNot,
               "BITOP_DESTKEY", not_keys, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 6);
  s = db.Get("BITOP_DESTKEY", &value);
  ASSERT_STREQ(value.c_str(), "\xb9\xb0\xb0\xbd\xbe\xad");
  // NOT operation more than two parameters
  s = db.BitOp(blackwidow::BitOpType::kBitOpNot,
               "BITOP_DESTKEY", src_keys, &ret);
  ASSERT_TRUE(s.IsInvalidArgument());
}

// Decrby
TEST_F(StringsTest, DecrbyTest) {
  int64_t ret;
  // If the key is not exist
  s = db.Decrby("DECRBY_KEY", 5, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, -5);

  // If the key contains a string that can not be represented as integer
  s = db.Set("DECRBY_KEY", "DECRBY_VALUE");
  ASSERT_TRUE(s.ok());
  s = db.Decrby("DECRBY_KEY", 5, &ret);
  ASSERT_TRUE(s.IsCorruption());

  // Less than the minimum number -9223372036854775808
  s = db.Set("DECRBY_KEY", "-2");
  ASSERT_TRUE(s.ok());
  s = db.Decrby("DECRBY_KEY", 9223372036854775807, &ret);
  ASSERT_TRUE(s.IsInvalidArgument());
}

// Get
TEST_F(StringsTest, GetTest) {
  std::string value;
  s = db.Set("GET_KEY", "GET_VALUE_1");
  ASSERT_TRUE(s.ok());

  s = db.Get("GET_KEY", &value);
  ASSERT_TRUE(s.ok());
  ASSERT_STREQ(value.c_str(), "GET_VALUE_1");

  s = db.Set("GET_KEY", "GET_VALUE_2");
  ASSERT_TRUE(s.ok());

  s = db.Get("GET_KEY", &value);
  ASSERT_TRUE(s.ok());
  ASSERT_STREQ(value.c_str(), "GET_VALUE_2");
}

// GetBit
TEST_F(StringsTest, GetBitTest) {
  int32_t ret;
  s = db.SetBit("GETBIT_KEY", 7, 1, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 0);

  s = db.GetBit("GETBIT_KEY", 0, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 0);

  s = db.GetBit("GETBIT_KEY", 7, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 1);

  // The offset is beyond the string length
  s = db.GetBit("GETBIT_KEY", 100, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 0);
}

// Getrange
TEST_F(StringsTest, GetrangeTest) {
  std::string value;
  s = db.Set("GETRANGE_KEY", "This is a string");
  ASSERT_TRUE(s.ok());
  s = db.Getrange("GETRANGE_KEY", 0, 3, &value);
  ASSERT_TRUE(s.ok());
  ASSERT_STREQ(value.c_str(), "This");

  s = db.Getrange("GETRANGE_KEY", -3, -1, &value);
  ASSERT_TRUE(s.ok());
  ASSERT_STREQ(value.c_str(), "ing");

  s = db.Getrange("GETRANGE_KEY", 0, -1, &value);
  ASSERT_TRUE(s.ok());
  ASSERT_STREQ(value.c_str(), "This is a string");

  s = db.Getrange("GETRANGE_KEY", 10, 100, &value);
  ASSERT_TRUE(s.ok());
  ASSERT_STREQ(value.c_str(), "string");

  // If the key is not exist
  s = db.Getrange("GETRANGE_NOT_EXIST_KEY", 0, -1, &value);
  ASSERT_TRUE(s.IsNotFound());
  ASSERT_STREQ(value.c_str(), "");
}

// GetSet
TEST_F(StringsTest, GetSetTest) {
  std::string value;
  // If the key did not exist
  s = db.GetSet("GETSET_KEY", "GETSET_VALUE", &value);
  ASSERT_TRUE(s.ok());
  ASSERT_STREQ(value.c_str(), "");

  s = db.GetSet("GETSET_KEY", "GETSET_VALUE", &value);
  ASSERT_TRUE(s.ok());
  ASSERT_STREQ(value.c_str(), "GETSET_VALUE");
}

// Incrby
TEST_F(StringsTest, IncrbyTest) {
  int64_t ret;
  // If the key is not exist
  s = db.Incrby("INCRBY_KEY", 5, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 5);

  // If the key contains a string that can not be represented as integer
  s = db.Set("INCRBY_KEY", "INCRBY_VALUE");
  ASSERT_TRUE(s.ok());
  s = db.Incrby("INCRBY_KEY", 5, &ret);
  ASSERT_TRUE(s.IsCorruption());

  s = db.Set("INCRBY_KEY", "1");
  ASSERT_TRUE(s.ok());
  // Less than the maximum number 9223372036854775807
  s = db.Incrby("INCRBY_KEY", 9223372036854775807, &ret);
  ASSERT_TRUE(s.IsInvalidArgument());
}

// Incrbyfloat
TEST_F(StringsTest, IncrbyfloatTest) {
  std::string value;
  s = db.Set("INCRBYFLOAT_KEY", "10.50");
  ASSERT_TRUE(s.ok());
  s = db.Incrbyfloat("INCRBYFLOAT_KEY", "0.1", &value);
  ASSERT_TRUE(s.ok());
  ASSERT_STREQ(value.c_str(), "10.6");
  s = db.Incrbyfloat("INCRBYFLOAT_KEY", "-5", &value);
  ASSERT_TRUE(s.ok());
  ASSERT_STREQ(value.c_str(), "5.6");

  // If the key contains a string that can not be represented as integer
  s = db.Set("INCRBYFLOAT_KEY", "INCRBY_VALUE");
  ASSERT_TRUE(s.ok());
  s = db.Incrbyfloat("INCRBYFLOAT_KEY", "5", &value);
  ASSERT_TRUE(s.IsCorruption());
}

// MGet
TEST_F(StringsTest, MGetTest) {
  std::vector<blackwidow::KeyValue> kvs {{"MGET_KEY1", "VALUE1"},
                                         {"MGET_KEY2", "VALUE2"},
                                         {"MGET_KEY3", "VALUE3"}};
  s = db.MSet(kvs);
  ASSERT_TRUE(s.ok());
  std::vector<std::string> values;
  std::vector<std::string> keys {"",
                                 "MGET_KEY1",
                                 "MGET_KEY2",
                                 "MGET_KEY3",
                                 "MGET_NOT_EXIST_KEY"};
  s = db.MGet(keys, &values);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(values.size(), 5);
  ASSERT_EQ(values[0], "");
  ASSERT_EQ(values[1], "VALUE1");
  ASSERT_EQ(values[2], "VALUE2");
  ASSERT_EQ(values[3], "VALUE3");
  ASSERT_EQ(values[4], "");
}

// MSet
TEST_F(StringsTest, MSetTest) {
  std::vector<blackwidow::KeyValue> kvs;
  kvs.push_back({"", "MSET_EMPTY_VALUE"});
  kvs.push_back({"MSET_TEST_KEY1", "MSET_TEST_VALUE1"});
  kvs.push_back({"MSET_TEST_KEY2", "MSET_TEST_VALUE2"});
  kvs.push_back({"MSET_TEST_KEY3", "MSET_TEST_VALUE3"});
  kvs.push_back({"MSET_TEST_KEY3", "MSET_TEST_VALUE3"});
  s = db.MSet(kvs);
  ASSERT_TRUE(s.ok());
}

// MSetnx
TEST_F(StringsTest, MSetnxTest) {
  int32_t ret;
  std::vector<blackwidow::KeyValue> kvs;
  kvs.push_back({"", "MSET_EMPTY_VALUE"});
  kvs.push_back({"MSET_TEST_KEY1", "MSET_TEST_VALUE1"});
  kvs.push_back({"MSET_TEST_KEY2", "MSET_TEST_VALUE2"});
  kvs.push_back({"MSET_TEST_KEY3", "MSET_TEST_VALUE3"});
  kvs.push_back({"MSET_TEST_KEY3", "MSET_TEST_VALUE3"});
  s = db.MSetnx(kvs, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 0);

  kvs.clear();
  kvs.push_back({"MSETNX_TEST_KEY1", "MSET_TEST_VALUE1"});
  kvs.push_back({"MSETNX_TEST_KEY2", "MSET_TEST_VALUE2"});
  kvs.push_back({"MSETNX_TEST_KEY3", "MSET_TEST_VALUE3"});
  kvs.push_back({"MSETNX_TEST_KEY3", "MSET_TEST_VALUE3"});
  s = db.MSetnx(kvs, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 1);
}

// Set
TEST_F(StringsTest, SetTest) {
  s = db.Set("SET_KEY", "SET_VALUE_1");
  ASSERT_TRUE(s.ok());

  std::string value;
  s = db.Get("SET_KEY", &value);
  ASSERT_STREQ(value.c_str(), "SET_VALUE_1");

  s = db.Set("SET_KEY", "SET_VALUE_2");
  ASSERT_TRUE(s.ok());

  s = db.Get("SET_KEY", &value);
  ASSERT_STREQ(value.c_str(), "SET_VALUE_2");
}

// SetBit
TEST_F(StringsTest, SetBitTest) {
  int32_t ret;
  // ***************** Group 1 Test *****************
  s = db.SetBit("GP1_SETBIT_KEY", 7, 1, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 0);

  s = db.SetBit("GP1_SETBIT_KEY", 7, 0, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 1);

  std::string value;
  s = db.Get("GP1_SETBIT_KEY", &value);
  ASSERT_TRUE(s.ok());
  ASSERT_STREQ(value.c_str(), "\x00");


  // ***************** Group 2 Test *****************
  s = db.SetBit("GP2_SETBIT_KEY", 10081, 1, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 0);

  s = db.GetBit("GP2_SETBIT_KEY", 10081, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 1);

  s = db.SetBit("GP2_SETBIT_KEY", 10081, 1, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 1);

  s = db.GetBit("GP2_SETBIT_KEY", 10081, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 1);


  // ***************** Group 3 Test *****************
  s = db.SetBit("GP3_SETBIT_KEY", 1, 1, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 0);

  s = db.GetBit("GP3_SETBIT_KEY", 1, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 1);

  s = db.SetBit("GP3_SETBIT_KEY", 1, 0, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 1);

  s = db.GetBit("GP3_SETBIT_KEY", 1, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 0);


  // ***************** Group 4 Test *****************
  s = db.SetBit("GP4_SETBIT_KEY", 1, 1, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 0);
  ASSERT_TRUE(make_expired(&db, "GP4_SETBIT_KEY"));

  s = db.SetBit("GP4_SETBIT_KEY", 1, 1, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 0);


  // ***************** Group 5 Test *****************
  // The offset argument is less than 0
  s = db.SetBit("GP5_SETBIT_KEY", -1, 0, &ret);
  ASSERT_TRUE(s.IsInvalidArgument());
}

// Setex
TEST_F(StringsTest, SetexTest) {
  std::string value;
  s = db.Setex("SETEX_KEY", "SETEX_VALUE", 1);
  ASSERT_TRUE(s.ok());

  // The key is not timeout
  s = db.Get("SETEX_KEY", &value);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(value, "SETEX_VALUE");

  // The key is timeout
  std::this_thread::sleep_for(std::chrono::milliseconds(2000));
  s = db.Get("SETEX_KEY", &value);
  ASSERT_TRUE(s.IsNotFound());

  // If the ttl equal 0
  s = db.Setex("SETEX_KEY", "SETEX_VALUE", 0);
  ASSERT_TRUE(s.IsInvalidArgument());

  // The ttl is negative
  s = db.Setex("SETEX_KEY", "SETEX_VALUE", -1);
  ASSERT_TRUE(s.IsInvalidArgument());
}

// Setnx
TEST_F(StringsTest, SetnxTest) {
  // If the key was set, return 1
  int32_t ret;
  s = db.Setnx("SETNX_KEY", "TEST_VALUE", &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 1);

  // If the key was not set, return 0
  s = db.Setnx("SETNX_KEY", "TEST_VALUE", &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 0);
}

// Setvx
TEST_F(StringsTest, SetvxTest) {

  int32_t ret, ttl;
  std::string value;
  // ***************** Group 1 Test *****************
  s = db.Set("GP1_SETVX_KEY", "GP1_SETVX_VALUE");
  ASSERT_TRUE(s.ok());

  s = db.Setvx("GP1_SETVX_KEY", "GP1_SETVX_VALUE", "GP1_SETVX_NEW_VALUE", &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 1);

  s = db.Get("GP1_SETVX_KEY", &value);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(value, "GP1_SETVX_NEW_VALUE");


  // ***************** Group 2 Test *****************
  s = db.Setvx("GP2_SETVX_KEY", "GP2_SETVX_VALUE", "GP2_SETVX_NEW_VALUE", &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 0);

  s = db.Get("GP2_SETVX_KEY", &value);
  ASSERT_TRUE(s.IsNotFound());
  ASSERT_EQ(value, "");


  // ***************** Group 3 Test *****************
  s = db.Set("GP3_SETVX_KEY", "GP3_SETVX_VALUE");
  ASSERT_TRUE(s.ok());

  s = db.Setvx("GP3_SETVX_KEY", "GP3_SETVX_OTHER_VALUE", "GP3_SETVX_NEW_VALUE", &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, -1);

  s = db.Get("GP3_SETVX_KEY", &value);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(value, "GP3_SETVX_VALUE");


  // ***************** Group 4 Test *****************
  s = db.Set("GP4_SETVX_KEY", "GP4_SETVX_VALUE");
  ASSERT_TRUE(s.ok());

  ASSERT_TRUE(make_expired(&db, "GP4_SETVX_KEY"));
  s = db.Setvx("GP4_SETVX_KEY", "GP4_SETVX_VALUE", "GP4_SETVX_NEW_VALUE", &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 0);

  s = db.Get("GP4_SETVX_KEY", &value);
  ASSERT_TRUE(s.IsNotFound());
  ASSERT_EQ(value, "");


  // ***************** Group 5 Test *****************
  s = db.Set("GP5_SETVX_KEY", "GP5_SETVX_VALUE");
  ASSERT_TRUE(s.ok());

  s = db.Setvx("GP5_SETVX_KEY", "GP5_SETVX_VALUE", "GP5_SETVX_NEW_VALUE", &ret, 10);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 1);

  s = db.Get("GP5_SETVX_KEY", &value);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(value, "GP5_SETVX_NEW_VALUE");

  ASSERT_TRUE(string_ttl(&db, "GP5_SETVX_KEY", &ttl));
  ASSERT_LE(0, ttl);
  ASSERT_GE(10, ttl);


  // ***************** Group 6 Test *****************
  s = db.Set("GP6_SETVX_KEY", "GP6_SETVX_VALUE");
  ASSERT_TRUE(s.ok());

  std::map<blackwidow::DataType, Status> type_status;
  ret = db.Expire("GP6_SETVX_KEY", 10, &type_status);
  ASSERT_EQ(ret, 1);

  sleep(1);
  ASSERT_TRUE(string_ttl(&db, "GP6_SETVX_KEY", &ttl));
  ASSERT_LT(0, ttl);
  ASSERT_GT(10, ttl);

  s = db.Setvx("GP6_SETVX_KEY", "GP6_SETVX_VALUE", "GP6_SETVX_NEW_VALUE", &ret, 20);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 1);

  s = db.Get("GP6_SETVX_KEY", &value);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(value, "GP6_SETVX_NEW_VALUE");

  sleep(1);
  ASSERT_TRUE(string_ttl(&db, "GP6_SETVX_KEY", &ttl));
  ASSERT_LE(10, ttl);
  ASSERT_GE(20, ttl);
}

// Delvx
TEST_F(StringsTest, DelvxTest) {

  int32_t ret, ttl;
  std::string value;
  // ***************** Group 1 Test *****************
  s = db.Set("GP1_DELVX_KEY", "GP1_DELVX_VALUE");
  ASSERT_TRUE(s.ok());

  s = db.Delvx("GP1_DELVX_KEY", "GP1_DELVX_VALUE", &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 1);

  s = db.Get("GP1_DELVX_KEY", &value);
  ASSERT_TRUE(s.IsNotFound());
  ASSERT_EQ(value, "");


  // ***************** Group 2 Test *****************
  s = db.Delvx("GP2_DELVX_KEY", "GP2_DELVX_VALUE", &ret);
  ASSERT_TRUE(s.IsNotFound());
  ASSERT_EQ(ret, 0);

  s = db.Get("GP2_DELVX_KEY", &value);
  ASSERT_TRUE(s.IsNotFound());
  ASSERT_EQ(value, "");


  // ***************** Group 3 Test *****************
  s = db.Set("GP3_DELVX_KEY", "GP3_DELVX_VALUE");
  ASSERT_TRUE(s.ok());

  s = db.Delvx("GP3_DELVX_KEY", "GP3_DELVX_OTHER_VALUE", &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, -1);

  s = db.Get("GP3_DELVX_KEY", &value);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(value, "GP3_DELVX_VALUE");


  // ***************** Group 4 Test *****************
  s = db.Set("GP4_DELVX_KEY", "GP4_DELVX_VALUE");
  ASSERT_TRUE(s.ok());

  ASSERT_TRUE(make_expired(&db, "GP4_DELVX_KEY"));
  s = db.Delvx("GP4_DELVX_KEY", "GP4_DELVX_VALUE", &ret);
  ASSERT_TRUE(s.IsNotFound());
  ASSERT_EQ(ret, 0);

  s = db.Get("GP4_DELVX_KEY", &value);
  ASSERT_TRUE(s.IsNotFound());
  ASSERT_EQ(value, "");
}

// Setrange
TEST_F(StringsTest, SetrangeTest) {
  std::string value;
  int32_t ret;
  s = db.Set("SETRANGE_KEY", "HELLO WORLD");
  ASSERT_TRUE(s.ok());
  s = db.Setrange("SETRANGE_KEY", 6, "REDIS", &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 11);
  s = db.Get("SETRANGE_KEY", &value);
  ASSERT_STREQ(value.c_str(), "HELLO REDIS");

  std::vector<std::string> keys {"SETRANGE_KEY"};
  std::map<blackwidow::DataType, Status> type_status;
  ret = db.Del(keys, &type_status);
  ASSERT_EQ(ret, 1);
  // If not exist, padded with zero-bytes to make offset fit
  s = db.Setrange("SETRANGE_KEY", 6, "REDIS", &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 11);
  s = db.Get("SETRANGE_KEY", &value);
  ASSERT_STREQ(value.c_str(), "\x00\x00\x00\x00\x00\x00REDIS");

  // If the offset less than 0
  s = db.Setrange("SETRANGE_KEY", -1, "REDIS", &ret);
  ASSERT_TRUE(s.IsInvalidArgument());
}

// Strlen
TEST_F(StringsTest, StrlenTest) {
  int32_t strlen;
  // The value is empty
  s = db.Set("STRLEN_EMPTY_KEY", "");
  ASSERT_TRUE(s.ok());
  s = db.Strlen("STRLEN_EMPTY_KEY", &strlen);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(strlen, 0);

  // The key is not exist
  s = db.Strlen("STRLEN_NOT_EXIST_KEY", &strlen);
  ASSERT_EQ(strlen, 0);

  s = db.Set("STRLEN_KEY", "STRLEN_VALUE");
  ASSERT_TRUE(s.ok());
  s = db.Strlen("STRLEN_KEY", &strlen);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(strlen, 12);
}

// BitPos
TEST_F(StringsTest, BitPosTest) {
  // bitpos key bit
  int64_t ret;
  s = db.Set("BITPOS_KEY", "\xff\xf0\x00");
  ASSERT_TRUE(s.ok());
  s = db.BitPos("BITPOS_KEY", 0, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 12);

  // bitpos key bit [start]
  s = db.Set("BITPOS_KEY", "\xff\x00\x00");
  ASSERT_TRUE(s.ok());
  s = db.BitPos("BITPOS_KEY", 1, 0, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 0);
  s = db.BitPos("BITPOS_KEY", 1, 2, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, -1);

  // bitpos key bit [start] [end]
  s = db.BitPos("BITPOS_KEY", 1, 0, 4, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 0);

  // bit value is not exists
  s = db.Set("BITPOS_KEY", "\x00\x00\x00");
  ASSERT_TRUE(s.ok());
  s = db.BitPos("BITPOS_KEY", 1, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, -1);

  s = db.Set("BITPOS_KEY", "\xff\xff\xff");
  ASSERT_TRUE(s.ok());
  s = db.BitPos("BITPOS_KEY", 0, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, -1);

  s = db.BitPos("BITPOS_KEY", 0, 0, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, -1);

  s = db.BitPos("BITPOS_KEY", 0, 0, -1, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, -1);

  // the offset is beyond the range
  s = db.BitPos("BITPOS_KEY", 0, 4, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, -1);
}

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
