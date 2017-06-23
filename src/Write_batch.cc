//  Copyright (c) 2017-present, Intel Corporation.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.

#include "Segment.h"
#include "hyperds/Write_batch.h"

namespace kvdb {
WriteBatch::WriteBatch() {}

WriteBatch::~WriteBatch() {
    clear();
}

void WriteBatch::put(const char *key, uint32_t key_len, const char* data,
                    uint16_t length) {
    KVSlice *slice = new KVSlice(key, key_len, data, length);
    batch_.push_back(slice);
}

void WriteBatch::del(const char *key, uint32_t key_len) {
    KVSlice *slice = new KVSlice(key, key_len, NULL, 0);
    batch_.push_back(slice);
}
void WriteBatch::clear() {
    while(!batch_.empty()) {
        KVSlice *slice = batch_.front();
        batch_.pop_front();
        delete slice;
    }
}

}
