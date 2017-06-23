//  Copyright (c) 2017-present, Intel Corporation.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.

#ifndef _KV_DB_KVDBITER_H_
#define _KV_DB_KVDBITER_H_

#include <string>
#include "hyperds/Iterator.h"

#ifdef WITH_ITERATOR
namespace kvdb {

class IndexManager;
class SegmentManager;
class BlockDevice;
class HashEntry;

class KvdbIter : public Iterator {
public:
    KvdbIter(IndexManager* im, SegmentManager* sm, BlockDevice* bdev);
    ~KvdbIter();

    virtual void SeekToFirst() override;
    virtual void SeekToLast() override;
    virtual void Seek(const char* key) override;
    virtual void Next() override;
    virtual void Prev() override;

    virtual std::string Key() override;
    virtual std::string Value() override;

    virtual bool Valid() const override;
    virtual Status status() const override;

private:
    IndexManager *idxMgr_;
    SegmentManager *segMgr_;
    BlockDevice* bdev_;
    bool valid_;
    HashEntry *hashEntry_;
    Status status_;
    //HashtableSlot *ht_;
    int htSize_;
    int hashTableCur_;
    int entryListCur_;
};
} 
#endif

#endif // #ifndef _KV_DB_KVDBITER_H_
