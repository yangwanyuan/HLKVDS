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
#include "GcManager.h"
#include "Segment.h"
#include "ReadCache.h"
#include "WorkQueue_.h"
#include "MetaStor.h"

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

    Status insertKey(KVSlice& slice);
    Status updateMeta(Request *req);

    Status readData(KVSlice& slice, string &data);

private:
    SuperBlockManager* sbMgr_;
    IndexManager* idxMgr_;
    BlockDevice* bdev_;
    SegmentManager* segMgr_;
    GcManager* gcMgr_;
    dslab::ReadCache* rdCache_;// readcache, rmd160, slru/lru
    string fileName_;

    SegForReq *seg_;
    std::mutex segMtx_;
    Options options_;

    MetaStor *metaStor_;

    // Request Merge thread
private:
    class ReqsMergeWQ : public WorkQueue_<Request> {
    public:
        explicit ReqsMergeWQ(KVDS *ds, int thd_num=1) : WorkQueue_<Request>(thd_num), ds_(ds) {}

    protected:
        void _process(Request* req) override {
            ds_->ReqMerge(req);
        }
    private:
        KVDS *ds_;
    };
    ReqsMergeWQ * reqWQ_;
    void ReqMerge(Request* req);

    // Seg Write to device thread
private:
    class SegmentWriteWQ : public WorkQueue_<SegForReq> {
    public:
        explicit SegmentWriteWQ(KVDS *ds, int thd_num=1) : WorkQueue_<SegForReq>(thd_num), ds_(ds) {}

    protected:
        void _process(SegForReq* seg) override {
            ds_->SegWrite(seg);
        }
    private:
        KVDS *ds_;
    };
    SegmentWriteWQ * segWteWQ_;
    void SegWrite(SegForReq* seg);

    // Seg Timeout thread
private:
    std::thread segTimeoutT_;
    std::atomic<bool> segTimeoutT_stop_;
    void SegTimeoutThdEntry();

    // Seg Reaper thread
private:
    class SegmentReaperWQ : public WorkQueue_<SegForReq> {
    public:
        explicit SegmentReaperWQ(KVDS *ds, int thd_num=1) : WorkQueue_<SegForReq>(thd_num), ds_(ds) {}

    protected:
        void _process(SegForReq* seg) override {
            ds_->SegReaper(seg);
        }
    private:
        KVDS *ds_;
    };
    SegmentReaperWQ *segRprWQ_;
    void SegReaper(SegForReq* seg);

    //GC thread
private:
    std::thread gcT_;
    std::atomic<bool> gcT_stop_;

    void GCThdEntry();
};

} // namespace hlkvds

#endif  // #ifndef _HLKVDS_KVDB_IMPL_H_
