#ifndef _HLKVDS_KVDB_IMPL_H_
#define _HLKVDS_KVDB_IMPL_H_

#include <list>
#include <queue>
#include <atomic>
#include <mutex>
#include <condition_variable>
#include <thread>

#include "Db_Structure.h"
#include "hlkvds/Options.h"
#include "hlkvds/Status.h"
#include "hlkvds/Write_batch.h"
#include "hlkvds/Iterator.h"
#include "BlockDevice.h"
#include "SuperBlockManager.h"
#include "IndexManager.h"
#include "SegmentManager.h"
#include "DataStor.h"
#include "GcManager.h"
#include "Segment.h"
#include "ReadCache.h"
#include "WorkQueue_.h"
#include "MetaStor.h"
#include "DataStor.h"

using namespace dslab;
namespace hlkvds {

class KVDS {
public:
    static KVDS* Create_KVDS(const char* filename, Options opts);
    static KVDS* Open_KVDS(const char* filename, Options opts);

    Status Insert(const char* key, uint32_t key_len, const char* data,
                  uint16_t length);
    Status Get(const char* key, uint32_t key_len, string &data);
    Status Delete(const char* key, uint32_t key_len);

    Status InsertBatch(WriteBatch *batch);

    Iterator* NewIterator();

    void Do_GC();
    void ClearReadCache() {
        bdev_->ClearReadCache();
    }
    void printDbStates();

    uint32_t getReqQueSize() {
        //return reqQue_.length();
        return 0;
    }
    uint32_t getSegWriteQueSize() {
        //return segWriteQue_.length();
        return 0;
    }
    uint32_t getSegReaperQueSize() {
        //return segReaperQue_.length();
        return 0;
    }

    virtual ~KVDS();

private:
    KVDS(const string& filename, Options opts);
    Status openDB();
    Status closeDB();
    void startThds();
    void stopThds();

private:
    SuperBlockManager* sbMgr_;
    IndexManager* idxMgr_;
    BlockDevice* bdev_;
    SegmentManager* segMgr_;

    ReadCache* rdCache_;// readcache, rmd160, slru/lru

    string fileName_;

    Options options_;

    MetaStor *metaStor_;
    SimpleDS_Impl *dataStor_;

};

} // namespace hlkvds

#endif  // #ifndef _HLKVDS_KVDB_IMPL_H_
