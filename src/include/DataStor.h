#ifndef _HLKVDS_DATASTOR_H_
#define _HLKVDS_DATASTOR_H_

#include <string>
#include <mutex>
#include <thread>
#include <atomic>
#include <vector>

#include "hlkvds/Options.h"
#include "hlkvds/Status.h"
#include "hlkvds/Write_batch.h"
#include "Segment.h"
#include "WorkQueue.h"

namespace hlkvds {

class KVSlice;
class HashEntry;

class SuperBlockManager;
class IndexManager;

class BlockDevice;
class Volumes;
class SegForReq;

class DataStor {
public:
    virtual Status WriteData(KVSlice& slice) = 0;
    virtual Status WriteBatchData(WriteBatch *batch) =0;
    virtual Status ReadData(KVSlice &slice, std::string &data) = 0;
    virtual bool UpdateSST() = 0;
    virtual bool GetAllSSTs(char* buf, uint64_t length) = 0;
    virtual bool SetAllSSTs(char* buf, uint64_t length) = 0;

    DataStor() {}
    virtual ~DataStor() {}

};

class SimpleDS_Impl : public DataStor {
public:

    SimpleDS_Impl(Options &opts, std::vector<BlockDevice*> &dev_vec, SuperBlockManager* sb, IndexManager* idx);
    ~SimpleDS_Impl();
    Status WriteData(KVSlice& slice) override;
    Status WriteBatchData(WriteBatch *batch) override;
    Status ReadData(KVSlice &slice, std::string &data) override;

    bool UpdateSST() override;
    bool GetAllSSTs(char* buf, uint64_t length) override;
    bool SetAllSSTs(char* buf, uint64_t length) override;

    void InitMeta(uint64_t sst_offset, uint32_t segment_size, uint32_t number_segments, uint32_t cur_seg_id);
    void UpdateMetaToSB();

    void InitSegment();
    void StartThds();
    void StopThds();

    std::string GetKeyByHashEntry(HashEntry *entry);
    std::string GetValueByHashEntry(HashEntry *entry);

    uint32_t GetReqQueSize() {
        return (!reqWQ_)? 0 : reqWQ_->Size();
    }
    uint32_t GetSegWriteQueSize() {
        return (!segWteWQ_)? 0 : segWteWQ_->Size();
    }
    void Do_GC();

    // interface for SegmentManager
    //use in IndexManager
    void ModifyDeathEntry(HashEntry &entry);

    //use in MetaStor
    static uint32_t ComputeSegNum(uint64_t total_size, uint32_t seg_size);
    static uint64_t ComputeSegTableSizeOnDisk(uint32_t seg_num);

    uint64_t GetDataRegionSize();

    //use in Kvdb_Impl
    uint32_t GetTotalFreeSegs();
    uint32_t GetMaxValueLength();

private:
    Status updateMeta(Request *req);
    void createAllVolumes();
    void deleteAllVolumes();

//private:
public:
    Options &options_;
    std::vector<BlockDevice *> &bdVec_;
    SuperBlockManager *sbMgr_;
    IndexManager *idxMgr_;

    Volumes* vol_;
    SegForReq *seg_;
    std::mutex segMtx_;

    // Request Merge thread
private:
    class ReqsMergeWQ : public dslab::WorkQueue<Request> {
    public:
        explicit ReqsMergeWQ(SimpleDS_Impl *ds, int thd_num=1) : dslab::WorkQueue<Request>(thd_num), ds_(ds) {}

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
    class SegmentWriteWQ : public dslab::WorkQueue<SegForReq> {
    public:
        explicit SegmentWriteWQ(SimpleDS_Impl *ds, int thd_num=1) : dslab::WorkQueue<SegForReq>(thd_num), ds_(ds) {}

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

};    
}// namespace hlkvds

#endif //#ifndef _HLKVDS_DATASTOR_H_
