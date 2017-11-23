#ifndef _HLKVDS_MULTIVOLUMEDS_H_
#define _HLKVDS_MULTIVOLUMEDS_H_

#include <string>
#include <mutex>
#include <thread>
#include <atomic>
#include <vector>
#include <map>

#include "hlkvds/Options.h"
#include "hlkvds/Status.h"
#include "hlkvds/Write_batch.h"
#include "Utils.h"
#include "Segment.h"
#include "WorkQueue.h"

#define DEVICE_PATH_LEN_LIMT 20

namespace hlkvds {

class DataStor;

class KVSlice;
class HashEntry;

class SuperBlockManager;
class IndexManager;

class BlockDevice;
class Volumes;
class SegForReq;

class MultiVolumeDS : public DataStor {
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
    MultiVolumeDS(Options &opts, std::vector<BlockDevice*> &dev_vec, SuperBlockManager* sb, IndexManager* idx);
    ~MultiVolumeDS();

    // Called by Kvdb_Impl
    int GetDataStorType() override { return 0; }
    void CreateAllSegments() override;
    void StartThds() override;
    void StopThds() override;

    void printDeviceTopologyInfo() override;
    void printDynamicInfo() override;

    Status WriteData(KVSlice& slice) override;
    Status WriteBatchData(WriteBatch *batch) override;
    Status ReadData(KVSlice &slice, std::string &data) override;

    void ManualGC() override;

    // Called by MetaStor
    bool GetSBReservedContent(char* buf, uint64_t length);
    bool SetSBReservedContent(char* buf, uint64_t length);

    bool GetAllSSTs(char* buf, uint64_t length) override;
    bool SetAllSSTs(char* buf, uint64_t length) override;

    void CreateAllVolumes(uint64_t sst_offset, uint32_t segment_size) override;
    bool OpenAllVolumes() override;

    uint32_t GetTotalSegNum() override {
        return segTotalNum_;
    }

    uint64_t GetSSTsLengthOnDisk() override {
        return sstLengthOnDisk_;
    }

    // Called by IndexManager
    void ModifyDeathEntry(HashEntry &entry) override;

    // Called by Iterator
    std::string GetKeyByHashEntry(HashEntry *entry);
    std::string GetValueByHashEntry(HashEntry *entry);

private:
    uint64_t calcSSTsLengthOnDiskBySegNum(uint32_t seg_num);
    uint32_t calcSegNumForPureVolume(uint64_t capacity, uint32_t seg_size);
    uint32_t calcSegNumForMetaVolume(uint64_t capacity, uint64_t sst_offset, uint32_t total_buddy_seg_num, uint32_t seg_size);

    uint32_t getTotalFreeSegs();
    uint32_t getReqQueSize();
    uint32_t getSegWriteQueSize();

    Status updateMeta(Request *req);
    void deleteAllVolumes();
    void deleteAllSegments();

    void initSBReservedContentForCreate();
    void updateAllVolSBRes();

    bool verifyTopology();

    int pickVol();
    int getVolIdFromEntry(HashEntry *entry);

    int calcShardId(KVSlice& slice);

private:
    Options &options_;
    std::vector<BlockDevice *> &bdVec_;
    SuperBlockManager *sbMgr_;
    IndexManager *idxMgr_;

    std::map<int, Volumes *> volMap_;

    int shardsNum_;
    std::mutex segMapMtx_;
    std::map<int, SegForReq *> segMap_;
    std::vector<std::mutex*> segMtxVec_;

    KVTime* lastTime_;

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
        explicit ReqsMergeWQ(MultiVolumeDS *ds, int thd_num=1) : dslab::WorkQueue<Request>(thd_num), ds_(ds) {}

    protected:
        void _process(Request* req) override {
            ds_->ReqMerge(req);
        }
    private:
        MultiVolumeDS *ds_;
    };
    std::vector<ReqsMergeWQ *> reqWQVec_;
    void ReqMerge(Request* req);

    // Seg Write to device thread
private:
    class SegmentWriteWQ : public dslab::WorkQueue<SegForReq> {
    public:
        explicit SegmentWriteWQ(MultiVolumeDS *ds, int thd_num=1) : dslab::WorkQueue<SegForReq>(thd_num), ds_(ds) {}

    protected:
        void _process(SegForReq* seg) override {
            ds_->SegWrite(seg);
        }
    private:
        MultiVolumeDS *ds_;
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

#endif //#ifndef _HLKVDS_MULTIVOLUMEDS_H_
