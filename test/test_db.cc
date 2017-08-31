#include <string>
#include <iostream>
#include "test_base.h"

class TestDb : public TestBase {
public:
    string path="/dev/loop2";
    double insert(KVDS *db) {
        //insert something
        string test_key = "test_key";
        int test_key_size = 8;
        string test_value = "test_value";
        int test_value_size = 10;

        KVTime tv_start;
        Status s = db->Insert(test_key.c_str(), test_key_size,
                              test_value.c_str(), test_value_size);
        EXPECT_TRUE(s.ok());
        KVTime tv_end;
        double diff_time = (tv_end - tv_start) / 1000.0;

        cout << "cost time: " << diff_time << "ms" << endl;
        return diff_time;
    }
};

TEST_F(TestDb, readinopen)
{
    KVDS *db= Create_DB(100);

    string test_key = "test-key";
    int test_key_size = 8;
    string test_value = "test-value";
    int test_value_size = 10;

    Status s=db->Insert(test_key.c_str(), test_key_size, test_value.c_str(), test_value_size);

    db->printDbStates();
    EXPECT_TRUE(s.ok());

    string get_data;
    s=db->Get(test_key.c_str(), test_key_size, get_data);
    EXPECT_TRUE(s.ok());
    EXPECT_EQ(test_value,get_data);

    delete db;

    //open db and read that key
    Options opts;
    opts.hashtable_size=100;
    KVDS *db2=KVDS::Open_KVDS(path.c_str(), opts);

    get_data="";
    s=db2->Get(test_key.c_str(), test_key_size, get_data);
    EXPECT_TRUE(s.ok());

    EXPECT_EQ(test_value,get_data);
    delete db2;
}

TEST_F(TestDb,uninitializeBlockDevice)
{

    KVDS *db = KVDS::Create_KVDS("/dev/loop3", opts);

    EXPECT_EQ(NULL,db);
}

TEST_F(TestDb,reopendb)
{
    KVDS *db = Create_DB(100);

    delete db;
    KVDS::Open_KVDS(path.c_str(), opts);

    KVDS* db2=KVDS::Open_KVDS(path.c_str(), opts);
    EXPECT_FALSE(NULL==db2);

    delete db2;
}

TEST_F(TestDb,usedbwithoutdeleting)
{
    KVDS *db = KVDS::Create_KVDS(path.c_str(), opts);

    insert(db);
    //delete db;
    //KVDS::Open_KVDS(path.c_str(), opts);
    //throw exception
    //Floating point exception (core dumped)
}

//this option has default value, but improper passed value still cause unhandled exception
TEST_F(TestDb,zerosegmentsize)
{
    opts.segment_size=0;
    KVDS *db = KVDS::Create_KVDS(path.c_str(), opts);

    EXPECT_TRUE(NULL==db);

    //unhadled exception
    //Floating point exception (core dumped)
}

//user should pass a value of hashtable size
TEST_F(TestDb,zerohashtablesize)
{
    opts.hashtable_size=0;
    string path="/dev/loop2";
    KVDS *db = KVDS::Create_KVDS(path.c_str(), opts);

    EXPECT_FALSE(NULL==db);

    //pass, should be failed
}

TEST_F(TestDb,zeroexpiretime)
{
    opts.expired_time=0;
    string path="/dev/loop2";
    KVDS *db = KVDS::Create_KVDS(path.c_str(), opts);

    EXPECT_FALSE(NULL==db);
    delete db;

    KVDS* db2=KVDS::Open_KVDS(path.c_str(), opts);
    EXPECT_FALSE(NULL==db2);

    insert(db2);
}

TEST_F(TestDb,expiretime)
{
    opts.expired_time=10000; // 10ms
    string path="/dev/loop2";
    KVDS *db = KVDS::Create_KVDS(path.c_str(), opts);

    EXPECT_FALSE(NULL==db);
    delete db;

    KVDS* db2=KVDS::Open_KVDS(path.c_str(), opts);
    EXPECT_FALSE(NULL==db2);

    double time=insert(db2);
    EXPECT_GT(time,10);
}

TEST_F(TestDb,seg_full_rate)
{
    //gc relative
}

TEST_F(TestDb,gc_upper_level)
{
    //gc relative
}

TEST_F(TestDb,gc_lower_level)
{
    //gc relative
}

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();

}

