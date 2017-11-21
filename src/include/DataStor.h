#ifndef _HLKVDS_DATASTOR_H_
#define _HLKVDS_DATASTOR_H_

#include <string>
#include <mutex>
#include <thread>
#include <atomic>
#include <vector>
#include <map>

#include "hlkvds/Options.h"
#include "hlkvds/Status.h"
#include "hlkvds/Write_batch.h"
#include "Segment.h"
#include "WorkQueue.h"

#define DEVICE_PATH_LEN_LIMT 20

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
    virtual void printDeviceTopology() = 0;
    virtual int GetDataStorType() = 0;

    virtual Status WriteData(KVSlice& slice) = 0;
    virtual Status WriteBatchData(WriteBatch *batch) =0;
    virtual Status ReadData(KVSlice &slice, std::string &data) = 0;

    virtual bool GetAllSSTs(char* buf, uint64_t length) = 0;
    virtual bool SetAllSSTs(char* buf, uint64_t length) = 0;

    DataStor() {}
    virtual ~DataStor() {}

};

class SimpleDS_Impl : public DataStor {
public:
    class SimpleDS_SB_Reserved_Header {
    public :
        uint32_t segment_size;
        uint32_t volume_num;
    public:
        SimpleDS_SB_Reserved_Header() : segment_size(0), volume_num(0) {}
        SimpleDS_SB_Reserved_Header(uint32_t seg_size, uint32_t vol_num)
            : segment_size(seg_size), volume_num(vol_num) {}
    };

    class SimpleDS_SB_Reserved_Volume {
    public:
        char dev_path[DEVICE_PATH_LEN_LIMT];
        uint32_t segment_num;
        uint32_t cur_seg_id;
    public:
        SimpleDS_SB_Reserved_Volume() : segment_num(0), cur_seg_id(0) {
            memset(dev_path, 0, DEVICE_PATH_LEN_LIMT);
        }
        SimpleDS_SB_Reserved_Volume(std::string path, uint32_t seg_num, uint32_t cur_id)
            : segment_num(seg_num), cur_seg_id(cur_id) {
            memset(dev_path, 0, DEVICE_PATH_LEN_LIMT);
            memcpy((void*)dev_path, (const void*)path.c_str(), path.size());
        }
    };

public:
    SimpleDS_Impl(Options &opts, std::vector<BlockDevice*> &dev_vec, SuperBlockManager* sb, IndexManager* idx);
    ~SimpleDS_Impl();

    void printDeviceTopology() override;
    int GetDataStorType() override { return 0; }

    Status WriteData(KVSlice& slice) override;
    Status WriteBatchData(WriteBatch *batch) override;
    Status ReadData(KVSlice &slice, std::string &data) override;

    bool GetAllSSTs(char* buf, uint64_t length) override;
    bool SetAllSSTs(char* buf, uint64_t length) override;

    bool SetSBReservedContent(char* buf, uint64_t length);
    bool GetSBReservedContent(char* buf, uint64_t length);

    void CreateAllVolumes(uint64_t sst_offset, uint32_t segment_size);
    bool OpenAllVolumes();

    void InitSegment();
    void StartThds();
    void StopThds();

    std::string GetKeyByHashEntry(HashEntry *entry);
    std::string GetValueByHashEntry(HashEntry *entry);

    uint32_t GetReqQueSize();
    uint32_t GetSegWriteQueSize();

    void Do_GC();

    // interface for SegmentManager
    //use in IndexManager
    void ModifyDeathEntry(HashEntry &entry);

    //use in MetaStor
    uint32_t GetTotalSegNum() {
        return segTotalNum_;
    }

    uint64_t GetSSTsLengthOnDisk() {
        return sstLengthOnDisk_;
    }

    //use in Kvdb_Impl
    uint32_t GetTotalFreeSegs();
    uint32_t GetMaxValueLength() const {
        return maxValueLen_;
    }

    static uint64_t ComputeSSTsLengthOnDiskBySegNum(uint32_t seg_num);
    static uint32_t ComputeSegNumForPureVolume(uint64_t capacity, uint32_t seg_size);
    static uint32_t ComputeSegNumForMetaVolume(uint64_t capacity, uint64_t sst_offset, uint32_t total_buddy_seg_num, uint32_t seg_size);

private:
    Status updateMeta(Request *req);
    void deleteAllVolumes();

    void initSBReservedContentForCreate();
    void updateAllVolSBRes();

    bool verifyTopology();

    int pickVol();
    int getVolIdFromEntry(HashEntry *entry);

private:
    Options &options_;
    std::vector<BlockDevice *> &bdVec_;
    SuperBlockManager *sbMgr_;
    IndexManager *idxMgr_;

    std::map<int, Volumes *> volMap_;
    SegForReq *seg_;
    std::mutex segMtx_;

    uint32_t segSize_;
    uint32_t maxValueLen_;

    uint32_t volNum_;
    uint32_t segTotalNum_;
    uint64_t sstLengthOnDisk_;

    int pickVolId_;
    std::mutex volIdMtx_;

    SimpleDS_SB_Reserved_Header sbResHeader_;
    std::vector<SimpleDS_SB_Reserved_Volume> sbResVolVec_;

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
