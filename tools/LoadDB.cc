//  Copyright (c) 2017-present, Intel Corporation.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.

#include <string>
#include <iostream>

#include "Kvdb_Impl.h"
#include "hlkvds/Options.h"
#include "hlkvds/Write_batch.h"
#include "hlkvds/Iterator.h"

using namespace std;
using namespace hlkvds;


void Open_DB_Test(string filename) {

    Options opts;
    KVDS *db = KVDS::Open_KVDS(filename.c_str(), opts);

    Iterator* it = db->NewIterator();
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
    delete db;
}

int main(int argc, char** argv) {

    string filename = argv[1];

    Open_DB_Test(filename);
    std::cout << "load process success" << std::endl;
    //Open_DB_Test(filename);
    return 0;
}
