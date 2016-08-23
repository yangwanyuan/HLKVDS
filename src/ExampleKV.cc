#include <iostream>
#include <string>
#include <sstream>
#include "Kvdb.h"


#define TEST_RECORD_NUM 1

//#define TEST_DB_FILENAME "000_db"
//#define TEST_DB_FILENAME "/dev/loop0"
#define TEST_DB_FILENAME "/dev/sdc1"

void Get(kvdb::DB *db)
{
    int key_len;
    for (int index = 0; index < TEST_RECORD_NUM; index++)
    {
        stringstream key_ss;
        key_ss << index;
        std::string key(key_ss.str());
        key_len = key.length();
        std::string get_data;
        db->Get(key.c_str(), key_len, get_data);
        std::cout << "Get Item Success. key = " << key << " ,value = " << get_data << std::endl;
    }

}
void Insert(kvdb::DB *db)
{
    int key_len;
    int value_len;
    for (int index = 0; index < TEST_RECORD_NUM; index++)
    {
        stringstream key_ss;
        key_ss << index;
        std::string key(key_ss.str());
        std::string value = "Begin" + key + "End";
        key_len = key.length();
        value_len = value.length();
        db->Insert(key.c_str(), key_len, value.c_str(), value_len);
        std::cout << "Insert Item Success. key = " << key << " ,value = " << value << std::endl;
    }


}

void Delete(kvdb::DB *db)
{
    int key_len;
    for (int index = 0; index < TEST_RECORD_NUM; index++)
    {
        stringstream key_ss;
        key_ss << index;
        std::string key(key_ss.str());
        key_len = key.length();
        //std::string get_data;
        db->Delete(key.c_str(), key_len);
        std::cout << "Delete Item Success. key = " << key << std::endl;
    }

}
void OpenExample()
{
    kvdb::DB *db;

    if (!kvdb::DB::OpenDB(TEST_DB_FILENAME, &db))
    {
        return;
    }

    //int key_len;
    //int value_len;

    Get(db);
    Insert(db);
    Get(db);
    Delete(db);
    Get(db);
    Insert(db);

    delete db;
}

int main(){
    OpenExample();
    return 0;
}
