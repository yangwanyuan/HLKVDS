//#pragma once
#ifndef TEST_NEW_BASE_H_
#define TEST_NEW_BASE_H_

#include "Kvdb_Impl.h"
#include "hlkvds/Options.h"
#include "hlkvds/Iterator.h"
#include "gtest/gtest.h"
#include <string>

using namespace hlkvds;

#define FILE_PATH "test_file0,test_file1,test_file2"

class TestNewBase : public ::testing::Test {
public:
    TestNewBase(std::string filename, int datastor_type);
    ~TestNewBase();

    KVDS* Create();
    KVDS* Open(Options &opts);
    KVDS* ReOpen(Options &opts);

    Status Insert(const char* key, uint32_t key_len, const char* data, uint16_t length, bool immediately = false);
    Status Get(const char* key, uint32_t key_len, std::string &data);
    Status Delete(const char* key, uint32_t key_len);
    Status InsertBatch(WriteBatch *batch);
    Iterator* NewIterator();
    
public:
    std::string path_;
    int dsType_;
    KVDS *db_;

};

#endif  //TEST_NEW_BASE_H_
