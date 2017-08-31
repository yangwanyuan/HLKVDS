#ifndef _HLKVDS_KVDBITER_H_
#define _HLKVDS_KVDBITER_H_

#include <string>
#include "hlkvds/Iterator.h"
#include "Db_Structure.h"

#ifdef WITH_ITERATOR
namespace hlkvds {

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

#endif // #ifndef _HLKVDS_KVDBITER_H_
