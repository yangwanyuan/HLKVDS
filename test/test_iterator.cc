#include "test_base.h"

class TestIterator : public TestBase {
public:
    int db_size = 100;
    KVDS *db = Create_DB(db_size);

    WriteBatch writeBatch;
    int count = 10;

    int test_key_size = 9;
    string test_key_base = "test-key";
    int test_value_size = 10;
    string test_value ="test-value";

    string *str_key;
    virtual void SetUp() {
        for (int i = 0; i < count; i++) {
            stringstream ss;
            string str = string("");
            ss << i;
            ss >> str;
            
            string test_key;
            test_key = test_key_base + str;

            str_key = new string(test_key);
            writeBatch.put(str_key->c_str(), test_key_size, test_value.c_str(),
                           test_value_size);

        }
        Status s = db->InsertBatch(&writeBatch);
        EXPECT_TRUE(s.ok());
    }
};

TEST_F(TestIterator, newIterator)
{
    Iterator *iter = db->NewIterator();
    if (iter == NULL) {
        EXPECT_TRUE(false);
    }
    delete iter;
}

TEST_F(TestIterator,seektofisrt)
{
    Iterator *iter = db->NewIterator();
    iter->SeekToFirst();
    if (iter->Valid()) {
        std::cout << "key:" << iter->Key() << ",value:" << iter->Value() << std::endl;
        EXPECT_EQ(test_value,iter->Value());
    } else {
        EXPECT_TRUE(false);
    }
    delete iter;
}


TEST_F(TestIterator,seektolast)
{
    Iterator *iter = db->NewIterator();
    iter->SeekToLast();
    if (iter->Valid()) {
        std::cout << "key:" << iter->Key() << ",value:" << iter->Value() << std::endl;
        EXPECT_EQ(test_value,iter->Value());
    } else {
        EXPECT_TRUE(false);
    }
    delete iter;

}

TEST_F(TestIterator,next)
{
    Iterator *iter = db->NewIterator();
    int key_num = 0;
    iter->SeekToFirst();
    while (iter->Valid()) {
        std::cout << "key:" << iter->Key() << ",value:" << iter->Value() << std::endl;
        iter->Next();
        key_num++;
    }
    EXPECT_EQ(key_num, count);

    delete iter;

}

TEST_F(TestIterator,prev)
{
    Iterator *iter = db->NewIterator();
    int key_num = 0;
    iter->SeekToLast();
    while (iter->Valid()) {
        std::cout << "key:" << iter->Key() << ",value:" << iter->Value() << std::endl;
        iter->Prev();
        key_num++;
    }
    EXPECT_EQ(key_num, count);

    delete iter;

}

TEST_F(TestIterator,seek)
{
    const char* key = "test-key3";
    Iterator *iter = db->NewIterator();
    iter->Seek(key);

    if (iter->Valid()) {
        string res = iter->Key();
        EXPECT_EQ(key,res.substr(0,res.length()));
        EXPECT_EQ(test_value,iter->Value());
    } else {
        EXPECT_TRUE(false);
    }
}



int main(int argc, char** argv) {
   ::testing::InitGoogleTest(&argc, argv);
   return RUN_ALL_TESTS();

}

