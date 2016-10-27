#include <iostream>
#include <string>
#include <sstream>
#include "Kvdb.h"


#define TEST_HT_SIZE 400
#define TEST_HT_MAX_DELETE_RATIO 0.9
#define TEST_HT_MAX_LOAD_FACTOR 0.8
#define TEST_SEGMENT_SIZE 256*1024

#define TEST_DB_FILENAME "/dev/sdb1"
//#define TEST_DB_FILENAME "/dev/sdb3"

void CreateExample()
{

    if (!kvdb::DB::CreateDB(TEST_DB_FILENAME, 
                            TEST_HT_SIZE, 
                            TEST_SEGMENT_SIZE))
    {
        std::cout << "CreateDB Failed!" << std::endl;
        return;
    }

}


int main(){
    CreateExample();
    return 0;
}
