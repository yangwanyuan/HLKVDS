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
    FastTier(Options& opts, SuperBlockManager* sb, IndexManager* idx);
    ~FastTier();
    
    void CreateAllSegments();
    void StartThds();
    void StopThds();

    Status WriteData(KVSlice& slice);
    Status WriteBatchData(WriteBatch *batch);
    Status ReadData(KVSlice &slice, std::string &data);

    void ManualGC();

    bool GetSST(char* buf, uint64_t length);
    bool SetSST(char* buf, uint64_t length);

    bool CreateVolume(BlockDevice *bdev, uint64_t start_offset, uint32_t seg_size, uint32_t seg_num, uint32_t cur_id);
    bool OpenVolume(BlockDevice *bdev, uint64_t start_offset, uint32_t seg_size, uint32_t seg_num, uint32_t cur_id);

    uint32_t GetSegSize() { return segSize_; }
    uint32_t GetSegNum() { return segNum_; }
    uint32_t GetTotalFreeSegs();
    uint32_t GetTotalUsedSegs();

    uint32_t GetCurSegId();
    uint64_t GetSSTLength();
    
    std::string GetDevicePath();

    uint32_t getReqQueSize();
    uint32_t getSegWriteQueSize();

    // Called by IndexManager
    void ModifyDeathEntry(HashEntry &entry);

    // Called by Iterator
    std::string GetKeyByHashEntry(HashEntry *entry);
    std::string GetValueByHashEntry(HashEntry *entry);

private:
    Options &options_;
    BlockDevice *bdev_;
    SuperBlockManager *sbMgr_;
    IndexManager *idxMgr_;

    int shardsNum_;
    std::mutex segMapMtx_;
    std::map<int, SegForReq *> segMap_;
    std::vector<std::mutex *> segMtxVec_;

    uint32_t maxValueLen_;

    uint32_t segSize_;
    uint32_t segNum_;

    Volume *vol_;

private:
    Status updateMeta(Request *req);

    void deleteAllSegments();

    int calcShardId(KVSlice& slice);

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

//class WarmTier : public Tier {
//public:
//    WarmTier();
//    ~WarmTier();
//
//    void CreateAllSegments();
//    void StartThds();
//    void StopThds();
//
//    bool GetSSTs(char* buf, uint64_t length);
//    bool SetSSTs(char* buf, uint64_t length);
//
//    bool CreateVolumes();
//    bool OpenVolumes();
//
//    uint32_t GetSegNum();
//
//    void ModifyDeathEntry(HashEntry &entry);
//
//private:
//    Options &options_;
//    BlockDevice *bdev_;
//    SuperBlockManager *sbMgr_;
//    IndexManager *idxMgr_;
//
//    uint32_t segSize_;
//    uint32_t segNum_;
//    uint32_t volNum_;
//    std::map<int, Volume *> volMap_;
//
//    int pickVolId_;
//    std::mutex volIdMtx_;
//
//private:
//    void deleteAllVolumes();
//
//};

}// namespace hlkvds

#endif //#ifndef _HLKVDS_TIER_H_
