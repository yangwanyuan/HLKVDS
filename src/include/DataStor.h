#ifndef _HLKVDS_DATASTOR_H_
#define _HLKVDS_DATASTOR_H_

#include <string>
#include <mutex>
#include <thread>
#include <atomic>
#include <map>

#include "hlkvds/Options.h"
#include "hlkvds/Status.h"
#include "hlkvds/Write_batch.h"
#include "Segment.h"
#include "WorkQueue.h"

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
    virtual Status WriteData(KVSlice& slice) = 0;
    virtual Status WriteBatchData(WriteBatch *batch) =0;
    virtual Status ReadData(KVSlice &slice, string &data) = 0;
    virtual bool UpdateSST() = 0;
    virtual bool GetAllSSTs() = 0;
    virtual bool SetAllSSTs() = 0;

    DataStor() {}
    virtual ~DataStor() {}

};

class SimpleDS_Impl : public DataStor {
public:

    SimpleDS_Impl(Options &opts, BlockDevice* dev, SuperBlockManager* sb, IndexManager* idx);
    ~SimpleDS_Impl();
    Status WriteData(KVSlice& slice) override;
    Status WriteBatchData(WriteBatch *batch) override;
    Status ReadData(KVSlice &slice, string &data) override;

    bool UpdateSST() override;
    bool GetAllSSTs() override;
    bool SetAllSSTs() override;

    void InitSegment();
    void StartThds();
    void StopThds();

    uint32_t GetReqQueSize() {
        return (!reqWQ_)? 0 : reqWQ_->Size();
    }
    uint32_t GetSegWriteQueSize() {
        return (!segWteWQ_)? 0 : segWteWQ_->Size();
    }
    void Do_GC();

    // interface for SegmentManager
    //use in KvdbIter
    bool ComputeDataOffsetPhyFromEntry(HashEntry* entry, uint64_t& data_offset);
    bool ComputeKeyOffsetPhyFromEntry(HashEntry* entry, uint64_t& key_offset);

    //use in IndexManager
    void ModifyDeathEntry(HashEntry &entry);

    //use in MetaStor
    static uint32_t ComputeSegNum(uint64_t total_size, uint32_t seg_size);
    static uint64_t ComputeSegTableSizeOnDisk(uint32_t seg_num);

    bool InitSegmentForCreateDB(uint64_t start_offset, uint32_t segment_size,
                                uint32_t number_segments);

    bool LoadSegmentTableFromDevice(uint64_t start_offset,
                                    uint32_t segment_size, uint32_t num_seg,
                                    uint32_t current_seg);

    bool WriteSegmentTableToDevice();

    uint64_t GetDataRegionSize();

    //use in Kvdb_Impl
    uint32_t GetTotalFreeSegs();
    uint32_t GetMaxValueLength();

    //use in GcManagrr
    //uint32_t GetTotalFreeSegs();
    uint32_t GetTotalUsedSegs();

    void SortSegsByUtils(std::multimap<uint32_t, uint32_t> &cand_map, double utils);

    uint32_t GetSegmentSize();
    uint32_t GetNumberOfSeg();

    bool AllocForGC(uint32_t& seg_id);
    void FreeForFailed(uint32_t seg_id);
    void FreeForGC(uint32_t seg_id);
    void Use(uint32_t seg_id, uint32_t free_size);
    
    bool ComputeSegOffsetFromId(uint32_t seg_id, uint64_t& offset);

    //use in Segment
    //uint32_t GetSegmentSize();
    //bool ComputeSegOffsetFromId(uint32_t seg_id, uint64_t& offset);


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
    class ReqsMergeWQ : public WorkQueue<Request> {
    public:
        explicit ReqsMergeWQ(SimpleDS_Impl *ds, int thd_num=1) : WorkQueue<Request>(thd_num), ds_(ds) {}

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
    class SegmentWriteWQ : public WorkQueue<SegForReq> {
    public:
        explicit SegmentWriteWQ(SimpleDS_Impl *ds, int thd_num=1) : WorkQueue<SegForReq>(thd_num), ds_(ds) {}

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

    //GC thread
private:
    std::thread gcT_;
    std::atomic<bool> gcT_stop_;

    void GCThdEntry();


};    
}// namespace hlkvds

#endif //#ifndef _HLKVDS_DATASTOR_H_
