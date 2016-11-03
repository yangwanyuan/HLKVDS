#include <string>
#include <iostream>

#include "Kvdb_Impl.h"

using namespace std;
using namespace kvdb;

//#define FILENAME  "0000_db"
//#define FILENAME  "/dev/loop0"
#define FILENAME  "/dev/sdb1"

void Create_DB_Test(string filename)
{
    
    int record_num = 10;
    int ht_size = record_num * 2;
    //double delete_ratio = 0.9;
    //double load_ratio = 0.8;

    int segment_size = 256*1024;
     
    //KvdbDS *db = KvdbDS::Create_KvdbDS(filename.c_str(), ht_size, delete_ratio, load_ratio, segment_size);
    KvdbDS *db = KvdbDS::Create_KvdbDS(filename.c_str(), ht_size, segment_size);
  

    delete db;
}

void Open_DB_Test(string filename)
{

    KvdbDS *db = KvdbDS::Open_KvdbDS(filename.c_str());

    //DB Insert 
    string test_key = "test-key";
    int test_key_size = 8;
    string test_value = "test-value";
    int test_value_size = 10;

    if (!db->Insert(test_key.c_str(), test_key_size, test_value.c_str(), test_value_size))
    {
        std::cout << "Insert Failed" << std::endl; 
    }
    else
    {
        std::cout << "Insert Success" << std::endl;
    }
    
    test_value = "test-value-new";
    test_value_size = 14;
    if (!db->Insert(test_key.c_str(), test_key_size, test_value.c_str(), test_value_size))
    {
        std::cout << "Insert Failed" << std::endl;
    }
    else
    {
        std::cout << "Insert Success" << std::endl;
    }

    //DB Get
    string get_data;
    if (!db->Get(test_key.c_str(), test_key_size, get_data))
    {
        std::cout << "Get Failed" << std::endl;
    }
    else
    {
        std::cout << "Get Success: data is " << get_data << std::endl; 
    }


    //DB Delete
    if (!db->Delete(test_key.c_str(), test_key_size))
    {
      std::cout << "Delete Failed" << std::endl;
    }
    else
    {
      std::cout << "Delete Success!" << std::endl;
    }

    ////DB Get
    //if (!db->Get(test_key.c_str(), test_key_size, get_data)){
    //  std::cout << "Get Failed" << std::endl;
    //}else{
    //  std::cout << "Get Success: data is " << get_data << std::endl; 
    //}

    delete db;
}

int main(int argc, char** argv){
    string filename= FILENAME;

    Create_DB_Test(filename);
    Open_DB_Test(filename);
    Create_DB_Test(filename);
    Open_DB_Test(filename);
    return 0;
}
