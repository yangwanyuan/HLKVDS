#include <string>
#include <iostream>
#include "test_new_base.h"
#include "Utils.h"

using namespace std;

class TestMultiVolume : public TestNewBase {
public:
    TestMultiVolume(): TestNewBase(FILE_PATH, 0) {}
};

TEST_F(TestMultiVolume, OpenDB) {
    KVDS *db = Create();
    EXPECT_TRUE( NULL != db);
    delete db;

    Options opts;
    opts.datastor_type = 0;
    db = Open(opts);
    EXPECT_TRUE( NULL != db);
    delete db;
}

TEST_F(TestMultiVolume, Insert) {
    KVDS *db = Create();

    string test_key = "test-key";
    int test_key_size = 8;
    string test_value = "test-value";
    int test_value_size = 10;

    Status s = Insert(test_key.c_str(), test_key_size, test_value.c_str(), test_value_size);
    
    EXPECT_TRUE(s.ok());
    string get_data;
    s=Get(test_key.c_str(), test_key_size, get_data);
    EXPECT_TRUE(s.ok());

    EXPECT_EQ(test_value,get_data);
    get_data.clear();
    delete db;
}

TEST_F(TestMultiVolume, Delete) {
    KVDS *db = Create();

    string test_key = "test-key";
    int test_key_size = 8;
    string test_value = "test-value";
    int test_value_size = 10;

    Status s = Insert(test_key.c_str(), test_key_size, test_value.c_str(), test_value_size);
    EXPECT_TRUE(s.ok());

    string get_data;
    s = Get(test_key.c_str(), test_key_size, get_data);
    EXPECT_TRUE(s.ok());
    EXPECT_EQ(test_value,get_data);
    get_data.clear();

    s = Delete(test_key.c_str(), test_key_size);
    EXPECT_TRUE(s.ok());

    s= Get(test_key.c_str(), test_key_size, get_data);
    EXPECT_TRUE(s.notfound());
    EXPECT_EQ("",get_data);
    get_data.clear();

    delete db;
    db = NULL;
}

TEST_F(TestMultiVolume, WriteBatch) {
    KVDS *db = Create();

    string test_key = "test-key";
    int test_key_size = 8;
    string test_value = "test-value";
    int test_value_size = 10;

    WriteBatch writeBatch;

    writeBatch.put(test_key.c_str(), test_key_size, test_value.c_str(),
                          test_value_size);
    string test_key2="testkey2";
    writeBatch.put(test_key2.c_str(), test_key_size, test_value.c_str(),
                    test_value_size);

    Status s = InsertBatch(&writeBatch);
    EXPECT_TRUE(s.ok());

    string get_data;
    s = Get(test_key.c_str(), test_key_size, get_data);
    EXPECT_TRUE(s.ok());
    EXPECT_EQ(test_value,get_data);

    get_data="";
    s = Get(test_key2.c_str(), test_key_size, get_data);
    EXPECT_TRUE(s.ok());
    EXPECT_EQ(test_value,get_data);

    delete db;
}

TEST_F(TestMultiVolume, Iterator) {
    KVDS *db = Create();
    
    string test_key = "test-key";
    int test_key_size = 8;
    string test_value = "test-value";
    int test_value_size = 10;

    Status s = Insert(test_key.c_str(), test_key_size, test_value.c_str(), test_value_size);
    EXPECT_TRUE(s.ok());

    Iterator *iter =NewIterator();
    EXPECT_TRUE(NULL != iter);
    iter->SeekToFirst();

    EXPECT_TRUE(iter->Valid());
    EXPECT_EQ(test_key, iter->Key());
    EXPECT_EQ(test_value, iter->Value());


    iter->SeekToLast();

    EXPECT_TRUE(iter->Valid());
    EXPECT_EQ(test_key, iter->Key());
    EXPECT_EQ(test_value, iter->Value());

    delete db;
}

TEST_F(TestMultiVolume, InsertLatencyFriendly) {
    KVDS *db = Create();
    delete db;

    Options opts;
    opts.aggregate_request = 0;
    db = Open(opts);

    string test_key = "test-key";
    int test_key_size = 8;
    string test_value = "test-value";
    int test_value_size = 10;

    Status s = Insert(test_key.c_str(), test_key_size, test_value.c_str(), test_value_size);

    EXPECT_TRUE(s.ok());
    string get_data;
    s=Get(test_key.c_str(), test_key_size, get_data);
    EXPECT_TRUE(s.ok());

    EXPECT_EQ(test_value,get_data);
    get_data.clear();
    delete db;
}

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();

}
