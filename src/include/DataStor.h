#ifndef _HLKVDS_DATASTOR_H_
#define _HLKVDS_DATASTOR_H_

#include <string>
#include <mutex>

#include "hlkvds/Options.h"
#include "hlkvds/Status.h"
#include "hlkvds/Write_batch.h"
#include "Segment.h"
#include "WorkQueue_.h"

using namespace std;
using namespace dslab;

namespace hlkvds {

class KVSlice;
class HashEntry;

class SuperBlockManager;
class IndexManager;

class BlockDevice;
class SegmentManager;
class SegForReq;

class GcManager;

class DataStor {
public:
    //static DataStor* Create();
    virtual Status WriteData(KVSlice& slice) = 0;
    virtual Status WriteBatchData(WriteBatch *batch) =0;
    //virtual Status ReadData(HashEntry* entry, string &data) = 0;
    virtual Status ReadData(KVSlice &slice, string &data) = 0;
    virtual bool UpdateSST() = 0;
    virtual bool GetAllSSTs() = 0;
    virtual bool SetAllSSTs() = 0;

    DataStor() {}
    virtual ~DataStor() {}

};

class SimpleDS_Impl : public DataStor {
public:

    SimpleDS_Impl(Options &opts, BlockDevice* dev, SuperBlockManager* sb, SegmentManager* sm, IndexManager* idx);
    ~SimpleDS_Impl();
    Status WriteData(KVSlice& slice) override;
    Status WriteBatchData(WriteBatch *batch) override;
    //Status ReadData(HashEntry* entry, string &data) override;
    Status ReadData(KVSlice &slice, string &data) override;
    bool UpdateSST() override;
    bool GetAllSSTs() override;
    bool SetAllSSTs() override;

    void InitSegment();
    void startThds();
    void stopThds();
    void Do_GC();

private:
    Status updateMeta(Request *req);

//private:
public:
    Options &options_;
    BlockDevice* bdev_;
    SuperBlockManager *sbMgr_;
    SegmentManager* segMgr_;
    IndexManager *idxMgr_;

    GcManager* gcMgr_;
    SegForReq *seg_;
    mutex segMtx_;

    // Request Merge thread
private:
    class ReqsMergeWQ : public WorkQueue_<Request> {
    public:
        explicit ReqsMergeWQ(SimpleDS_Impl *ds, int thd_num=1) : WorkQueue_<Request>(thd_num), ds_(ds) {}

    protected:
        void _process(Request* req) override {
            ds_->ReqMerge(req);
        }
    private:
        SimpleDS_Impl *ds_;
    };
    ReqsMergeWQ * reqWQ_;
    void ReqMerge(Request* req);

    // Seg Write to device thread
private:
    class SegmentWriteWQ : public WorkQueue_<SegForReq> {
    public:
        explicit SegmentWriteWQ(SimpleDS_Impl *ds, int thd_num=1) : WorkQueue_<SegForReq>(thd_num), ds_(ds) {}

    protected:
        void _process(SegForReq* seg) override {
            ds_->SegWrite(seg);
        }
    private:
        SimpleDS_Impl *ds_;
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
        explicit SegmentReaperWQ(SimpleDS_Impl *ds, int thd_num=1) : WorkQueue_<SegForReq>(thd_num), ds_(ds) {}

    protected:
        void _process(SegForReq* seg) override {
            ds_->SegReaper(seg);
        }
    private:
        SimpleDS_Impl *ds_;
    };
    SegmentReaperWQ *segRprWQ_;
    void SegReaper(SegForReq* seg);

    //GC thread
private:
    std::thread gcT_;
    std::atomic<bool> gcT_stop_;

    void GCThdEntry();


};    
}// namespace hlkvds

#endif //#ifndef _HLKVDS_DATASTOR_H_
