#ifndef _HLKVDS_TIER_H_
#define _HLKVDS_TIER_H_

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
#include "DataStor.h"

namespace hlkvds {

class DataStor;

class KVSlice;
class HashEntry;

class SuperBlockManager;
class IndexManager;

class BlockDevice;
class Volume;
class SegForReq;

class Tier {
public:
    Tier();
    ~Tier();
};

class FastTier : public Tier {
public:
    class MultiTierDS_SB_Reserved_FastTier {
    public:
        uint32_t segment_size;
        char dev_path[hlkvds::DevPathLenLimt];
        uint32_t segment_num;
        uint32_t cur_seg_id;
    public:
        MultiTierDS_SB_Reserved_FastTier()
                : segment_size(0), segment_num(0), cur_seg_id(0) {
            memset(dev_path, 0 , hlkvds::DevPathLenLimt);
        }
        MultiTierDS_SB_Reserved_FastTier(uint32_t seg_size, std::string path, uint32_t seg_num, uint32_t cur_id)
                : segment_size(seg_size), segment_num(seg_num), cur_seg_id(cur_id) {
            memset(dev_path, 0 , hlkvds::DevPathLenLimt);
            memcpy((void*)dev_path, (const void*)path.c_str(), path.size());
        }
    };

public:
    FastTier(Options& opts, SuperBlockManager* sb, IndexManager* idx);
    ~FastTier();
    
    void CreateAllSegments();
    void StartThds();
    void StopThds();

    void printDeviceTopologyInfo();
    void printDynamicInfo();

    Status WriteData(KVSlice& slice);
    Status WriteBatchData(WriteBatch *batch);
    Status ReadData(KVSlice &slice, std::string &data);

    void ManualGC();

    bool GetSBReservedContent(char* buf, uint64_t length);
    bool SetSBReservedContent(char* buf, uint64_t length);

    bool GetSST(char* buf, uint64_t length);
    bool SetSST(char* buf, uint64_t length);

    //bool CreateVolume(BlockDevice *bdev, uint64_t start_offset, uint32_t seg_size, uint32_t seg_num, uint32_t cur_id);
    bool CreateVolume(std::vector<BlockDevice*> &bd_vec, int fast_tier_device_num,uint64_t sst_offset, uint32_t medium_tier_total_seg_num);
    bool OpenVolume(std::vector<BlockDevice*> &bd_vec, int fast_tier_device_num);

    uint32_t GetSegSize() { return segSize_; }
    uint32_t GetTotalFreeSegs();
    uint32_t GetTotalUsedSegs();

    uint64_t GetSSTLength();

    std::string GetDevicePath();
    uint32_t GetSegNum() { return segNum_; }
    uint32_t GetCurSegId();

    uint32_t getReqQueSize();
    uint32_t getSegWriteQueSize();


    // Called by IndexManager
    void ModifyDeathEntry(HashEntry &entry);

    // Called by Iterator
    std::string GetKeyByHashEntry(HashEntry *entry);
    std::string GetValueByHashEntry(HashEntry *entry);

    uint32_t GetSbReservedSize() { return sizeof(MultiTierDS_SB_Reserved_FastTier); }
    uint64_t GetSSTLengthOnDisk() { return sstLengthOnDisk_; }
    uint32_t GetMaxValueLen() { return maxValueLen_; }

private:
    Options &options_;
    BlockDevice *bdev_;
    SuperBlockManager *sbMgr_;
    IndexManager *idxMgr_;

    MultiTierDS_SB_Reserved_FastTier sbResFastTier_;

    int shardsNum_;
    std::mutex segMapMtx_;
    std::map<int, SegForReq *> segMap_;
    std::vector<std::mutex *> segMtxVec_;

    uint32_t maxValueLen_;
    uint64_t sstLengthOnDisk_;

    uint32_t segSize_;
    uint32_t segNum_;

    Volume *vol_;

private:
    Status updateMeta(Request *req);

    void deleteAllSegments();
    void initSBReservedContentForCreate();

    bool verifyTopology(std::vector<BlockDevice*> &bd_vec, int fast_tier_device_num);

    int calcShardId(KVSlice& slice);
    uint64_t calcSSTsLengthOnDiskBySegNum(uint32_t seg_num);
    uint32_t calcSegNumForFastTierVolume(uint64_t capacity, uint64_t sst_offset, uint32_t fst_tier_seg_size, uint32_t sec_tier_seg_num);


    // Request Merge WorkQueue
protected:
    class ReqsMergeWQ : public dslab::WorkQueue<Request> {
    public:
        explicit ReqsMergeWQ(FastTier *ft, int thd_num=1) : dslab::WorkQueue<Request>(thd_num), ft_(ft) {}
    protected:
        void _process(Request* req) override {
            ft_->ReqMerge(req);
        }
    private:
        FastTier *ft_;
    };
    std::vector<ReqsMergeWQ *> reqWQVec_;
    void ReqMerge(Request* req);

    // Seg Write to device WorkQueue
protected:
    class SegmentWriteWQ : public dslab::WorkQueue<SegForReq> {
    public:
        explicit SegmentWriteWQ(FastTier *ft, int thd_num=1) : dslab::WorkQueue<SegForReq>(thd_num), ft_(ft) {}

    protected:
        void _process(SegForReq* seg) override {
            ft_->SegWrite(seg);
        }
    private:
        FastTier *ft_;
    };
    SegmentWriteWQ * segWteWQ_;
    void SegWrite(SegForReq* req);

    // Seg Timeout thread
protected:
    std::thread segTimeoutT_;
    std::atomic<bool> segTimeoutT_stop_;
    void SegTimeoutThdEntry();

};

class MediumTier : public Tier {
public:
    class MultiTierDS_SB_Reserved_MediumTier_Header {
    public:
        uint32_t segment_size;
        uint32_t volume_num;
    public:
        MultiTierDS_SB_Reserved_MediumTier_Header()
                : segment_size(0), volume_num(0) {
        }
        MultiTierDS_SB_Reserved_MediumTier_Header(uint32_t seg_size, uint32_t vol_num)
                : segment_size(seg_size), volume_num(vol_num) {
        }
    };

    class MultiTierDS_SB_Reserved_MediumTier_Volume {
    public:
        char dev_path[hlkvds::DevPathLenLimt];
        uint32_t segment_num;
        uint32_t cur_seg_id;
    public:
        MultiTierDS_SB_Reserved_MediumTier_Volume() : segment_num(0), cur_seg_id(0) {
            memset(dev_path, 0, hlkvds::DevPathLenLimt);
        }
        MultiTierDS_SB_Reserved_MediumTier_Volume(std::string path, uint32_t seg_num, uint32_t cur_id)
                : segment_num(seg_num), cur_seg_id(cur_id) {
            memset(dev_path, 0, hlkvds::DevPathLenLimt);
            memcpy((void*)dev_path, (const void*)path.c_str(), path.size());
        }
    };

public:
    MediumTier(Options& opts, SuperBlockManager* sb, IndexManager* idx);
    ~MediumTier();

    void CreateAllSegments();
    void StartThds();
    void StopThds();

    void printDeviceTopologyInfo();
    void printDynamicInfo();

    Status WriteData(KVSlice& slice);
    Status WriteBatchData(WriteBatch *batch);
    Status ReadData(KVSlice &slice, std::string &data);

    void ManualGC();

    bool GetSBReservedContent(char* buf, uint64_t length);
    bool SetSBReservedContent(char* buf, uint64_t length);

    bool GetSST(char* buf, uint64_t length);
    bool SetSST(char* buf, uint64_t length);

    bool CreateVolume(std::vector<BlockDevice*> &bd_vec, int fast_tier_device_num);
    //bool OpenVolume(std::vector<BlockDevice*> &bd_vec, int bd_cursor, uint32_t bd_num, uint32_t seg_size, std::vector<DS_MultiTier_Impl::MultiTierDS_SB_Reserved_Volume> &sb_res_vol_vec);
    //bool OpenVolume(std::vector<BlockDevice*> &bd_vec, int bd_cursor, uint32_t bd_num, uint32_t seg_size);
    bool OpenVolume(std::vector<BlockDevice*> &bd_vec, int fast_tier_device_num);

    uint32_t GetSegSize() { return segSize_; }
    uint32_t GetSegTotalNum() { return segTotalNum_; }
    uint32_t GetTotalFreeSegs();
    uint32_t GetTotalUsedSegs();

    uint64_t GetSSTLength();

    std::string GetDevicePath(uint32_t index);
    uint32_t GetSegNum(uint32_t index);
    uint32_t GetCurSegId(uint32_t index);

    uint32_t GetVolNum() { return volNum_; }

    // Called by IndexManager
    void ModifyDeathEntry(HashEntry &entry);

    // Called by Iterator
    std::string GetKeyByHashEntry(HashEntry *entry);
    std::string GetValueByHashEntry(HashEntry *entry);

    uint32_t GetSbReservedSize() { return sizeof(MultiTierDS_SB_Reserved_MediumTier_Header) + sizeof(MultiTierDS_SB_Reserved_MediumTier_Volume) * volNum_; }

private:
    Options &options_;
    SuperBlockManager *sbMgr_;
    IndexManager *idxMgr_;

    MultiTierDS_SB_Reserved_MediumTier_Header sbResMediumTier_;
    std::vector<MultiTierDS_SB_Reserved_MediumTier_Volume> sbResMediumTierVolVec_;

    uint32_t segSize_;
    uint32_t segTotalNum_;
    uint32_t volNum_;
    std::map<int, Volume *> volMap_;

    int pickVolId_;
    std::mutex volIdMtx_;

private:
    void deleteAllVolume();

    void initSBReservedContentForCreate();
    void updateAllVolSBRes();

    bool verifyTopology(std::vector<BlockDevice*> &bd_vec, int fast_tier_device_num);

    uint32_t calcSegNumForVolume(uint64_t capacity, uint32_t seg_size);
};

}// namespace hlkvds

#endif //#ifndef _HLKVDS_TIER_H_
