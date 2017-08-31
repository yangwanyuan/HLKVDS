#include <iostream>
#include <string>
#include <sstream>
#include <unistd.h>
#include "hlkvds/Kvdb.h"

#define TEST_RECORD_NUM 1

void Get(hlkvds::DB *db) {
    int key_len;
    for (int index = 0; index < TEST_RECORD_NUM; index++) {
        stringstream key_ss;
        key_ss << index;
        std::string key(key_ss.str());
        key_len = key.length();
        std::string get_data;
        db->Get(key.c_str(), key_len, get_data);
        std::cout << "Get Item Success. key = " << key << " ,value = "
                << get_data << std::endl;
    }

}
void Insert(hlkvds::DB *db) {
    int key_len;
    int value_len;
    for (int index = 0; index < TEST_RECORD_NUM; index++) {
        stringstream key_ss;
        key_ss << index;
        std::string key(key_ss.str());
        std::string value = "Begin" + key + "End";
        key_len = key.length();
        value_len = value.length();
        db->Insert(key.c_str(), key_len, value.c_str(), value_len);
        std::cout << "Insert Item Success. key = " << key << " ,value = "
                << value << std::endl;
    }

}

void Delete(hlkvds::DB *db) {
    int key_len;
    for (int index = 0; index < TEST_RECORD_NUM; index++) {
        stringstream key_ss;
        key_ss << index;
        std::string key(key_ss.str());
        key_len = key.length();
        db->Delete(key.c_str(), key_len);
        std::cout << "Delete Item Success. key = " << key << std::endl;
    }

}
void OpenExample(string filename) {
    hlkvds::DB *db;

    if (!hlkvds::DB::OpenDB(filename, &db)) {
        return;
    }

    Get(db);
    Insert(db);
    //sleep(2);
    Get(db);
    Delete(db);
    //sleep(2);
    Get(db);
    Insert(db);
    //sleep(2);
    Get(db);

    delete db;
}

int main(int argc, char** argv) {
    string filename = argv[1];
    OpenExample(filename);
    return 0;
}
