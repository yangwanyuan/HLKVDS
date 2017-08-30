//  Copyright (c) 2017-present, Intel Corporation.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.

#ifndef KV_DB_KVDB_H_
#define KV_DB_KVDB_H_

#include <iostream>
#include <string>

#include "hyperds/Options.h"
#include "hyperds/Status.h"
#include "hyperds/Write_batch.h"
#include "hyperds/Iterator.h"

using namespace std;

namespace kvdb {

class KvdbDS;

class DB {
public:
    static bool CreateDB(string filename, Options opts = Options());
    static bool OpenDB(string filename, DB** db, Options opts = Options());

    virtual ~DB();

    Status Insert(const char* key, uint32_t key_len, const char* data,
                uint16_t length);
    Status Delete(const char* key, uint32_t key_len);
    Status Get(const char* key, uint32_t key_len, string &data);

    Status InsertBatch(WriteBatch *batch);
    Iterator* NewIterator();

    void Do_GC();
    void printDbStates();

private:
    DB();
    DB(const DB &);
    DB& operator=(const DB &);

    string fileName_;
    KvdbDS *kvdb_;
    static DB *instance_;
};

} // namespace kvdb

#endif //KV_DB_KVDB_H_
