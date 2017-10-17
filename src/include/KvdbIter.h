#ifndef _HLKVDS_KVDBITER_H_
#define _HLKVDS_KVDBITER_H_

#include <string>
#include "hlkvds/Iterator.h"
#include "Db_Structure.h"

namespace hlkvds {

class IndexManager;
class SimpleDS_Impl;
class BlockDevice;
class HashEntry;

class KvdbIter : public Iterator {
public:
    KvdbIter(IndexManager* im, SimpleDS_Impl* ds, BlockDevice* bdev);
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
    SimpleDS_Impl* dataStor_;
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

#endif // #ifndef _HLKVDS_KVDBITER_H_
