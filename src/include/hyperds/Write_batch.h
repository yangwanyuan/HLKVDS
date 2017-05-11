
//  Copyright (c) 2017-present, Intel Corporation.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.

#ifndef _KV_DB_WRITE_BATCH_H_
#define _KV_DB_WRITE_BATCH_H_

#include <list>
#include <string>

namespace kvdb {

class KvdbDS;
class KVSlice;

class WriteBatch {
public:
    WriteBatch();
    ~WriteBatch();

    void put(const char *key, uint32_t key_len, const char* data,
            uint16_t length);
    void del(const char *key, uint32_t key_len);
    void clear();

private:
    std::list<KVSlice *> batch_;
    friend class KvdbDS;
};

} // namespace kvdb

#endif //#define _KV_DB_WRITE_BATCH_H_
