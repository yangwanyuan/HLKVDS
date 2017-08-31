#include <string>
#include <iostream>

#include "Kvdb_Impl.h"
#include "hlkvds/Options.h"
#include "hlkvds/Write_batch.h"
#include "hlkvds/Iterator.h"

using namespace std;
using namespace hlkvds;

//#define FILENAME  "0000_db"
//#define FILENAME  "/dev/loop0"
#define FILENAME  "/dev/sdb2"

void Create_DB_Test(string filename) {

    int record_num = 10;
    int ht_size = record_num * 2;

    Options opts;
    opts.segment_size = 256 * 1024;
    opts.hashtable_size = ht_size;

    KVDS *db = KVDS::Create_KVDS(filename.c_str(), opts);

    delete db;
}

void Open_DB_Test(string filename) {

    Options opts;
    KVDS *db = KVDS::Open_KVDS(filename.c_str(), opts);

    //DB Insert 
    string test_key = "test-key";
    int test_key_size = 8;
    string test_value = "test-value";
    int test_value_size = 10;

    Status s = db->Insert(test_key.c_str(), test_key_size, test_value.c_str(),
                          test_value_size);
    if (!s.ok()) {
        std::cout << "Insert Failed" << std::endl;
    } else {
        std::cout << "Insert Success" << std::endl;
    }

    test_value = "test-value-new";
    test_value_size = 14;
    s = db->Insert(test_key.c_str(), test_key_size, test_value.c_str(),
                   test_value_size);
    if (!s.ok()) {
        std::cout << "Insert Failed" << std::endl;
    } else {
        std::cout << "Insert Success" << std::endl;
    }

    //DB Get
    string get_data;
    s = db->Get(test_key.c_str(), test_key_size, get_data);
    if (!s.ok()) {
        std::cout << "Get Failed" << std::endl;
    } else {
        std::cout << "Get Success: data is " << get_data << std::endl;
    }

    //DB Delete
    s = db->Delete(test_key.c_str(), test_key_size);
    if (!s.ok()) {
        std::cout << "Delete Failed" << std::endl;
    } else {
        std::cout << "Delete Success!" << std::endl;
    }

    ////DB Get
    //if (!db->Get(test_key.c_str(), test_key_size, get_data)){
    //  std::cout << "Get Failed" << std::endl;
    //}else{
    //  std::cout << "Get Success: data is " << get_data << std::endl; 
    //}

    WriteBatch batch;
    batch.put(test_key.c_str(), test_key_size, test_value.c_str(),
                test_value_size);

    string test_key2 = "test-key2";
    int test_key2_size = 9;
    string test_value2 = "test-value2";
    int test_value2_size = 11;
    batch.put(test_key2.c_str(), test_key2_size, test_value2.c_str(),
                test_value2_size);
    batch.put("keykeykey3",10,"value3",6);
    batch.put("keykeykey4",10,"value4",6);
    batch.put("keykeykey5",10,"value5",6);
    batch.put("keykeykey6",10,"value6",6);
    batch.put("keykeykey7",10,"value7",6);
    batch.put("keykeykey8",10,"value8",6);
    batch.put("keykeykey9",10,"value9",6);
    batch.del(test_key.c_str(), test_key_size);

    s = db->InsertBatch(&batch);
    if (!s.ok()) {
        std::cout << "Insert Batch Failed" << std::endl;
    }
    else {
        std::cout << "Insert Batch Success" << std::endl;
    }

    //DB Get
    //string get_data;
    s = db->Get(test_key.c_str(), test_key_size, get_data);
    if (!s.ok()) {
        std::cout << "Get Failed" << std::endl;
    } else {
        std::cout << "Get Success: data is " << get_data << std::endl;
    }
    s = db->Get(test_key2.c_str(), test_key2_size, get_data);
    if (!s.ok()) {
        std::cout << "Get Failed" << std::endl;
    } else {
        std::cout << "Get Success: data is " << get_data << std::endl;
    }

    Iterator* it = db->NewIterator();
    //test iterator if compile iterator
    if ( it != NULL) {
        cout << "Iterator the db: First to Last" << endl;
        for(it->SeekToFirst(); it->Valid(); it->Next()) {
            cout << it->Key() << ": " << it->Value() << endl;
        }
        sleep(3);
        cout << "Iterator the db: Last to First" << endl;
        for(it->SeekToLast(); it->Valid(); it->Prev()) {
            cout << it->Key() << ": " << it->Value() << endl;
        }
        delete it;
    }
    delete db;
}

int main(int argc, char** argv) {
    string filename = FILENAME;

    Create_DB_Test(filename);
    Open_DB_Test(filename);
    std::cout << "first process success" << std::endl;
    Create_DB_Test(filename);
    Open_DB_Test(filename);
    return 0;
}
