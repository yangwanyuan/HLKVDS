#include <iostream>
#include <string>
#include <sstream>
#include "Kvdb.h"


#define TEST_HT_SIZE 400
#define TEST_HT_MAX_DELETE_RATIO 0.9
#define TEST_HT_MAX_LOAD_FACTOR 0.8
#define TEST_SEGMENT_SIZE 256*1024

//#define TEST_DB_FILENAME "000_db"
//#define TEST_DB_FILENAME "/dev/loop0"
#define TEST_DB_FILENAME "/dev/sdc1"

void CreateExample()
{
    //kvdb::DB *db;

    if (!kvdb::DB::CreateDB(TEST_DB_FILENAME, 
                            TEST_HT_SIZE, 
                            //TEST_HT_MAX_DELETE_RATIO, 
                            //TEST_HT_MAX_LOAD_FACTOR,
                            TEST_SEGMENT_SIZE)){
        std::cout << "CreateDB Failed!" << std::endl;
        return;
    }

    //int key_len, value_len;
    //std::string key="key-test";
    //std::string value="value-test\n";
    //key_len =  key.size();
    //value_len = value.size(); 

    //db->Insert(key.c_str(), key_len, value.c_str(), value_len);

    //std::string get_data;
    //db->Get(key.c_str(), key_len, get_data);
    //std::cout << "Get Data : get_data = " << get_data << std::endl;

    //db->Delete(key.c_str(), key_len);

    //delete db;
}


int main(){
    CreateExample();
    return 0;
}
