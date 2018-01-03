#ifndef _HLKVDS_DS_MULTITIER_IMPL_H_
#define _HLKVDS_DS_MULTITIER_IMPL_H_

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

namespace hlkvds {

class DataStor;

class KVSlice;
class HashEntry;

class SuperBlockManager;
class IndexManager;

class BlockDevice;
class Volume;
class SegForReq;

class FastTier;
class MediumTier;

class DS_MultiTier_Impl : public DataStor {
public:
    class MultiTierDS_SB_Reserved_Header {
    public:
        uint32_t sb_reserved_fast_tier_size;
        uint32_t sb_reserved_medium_tier_size;
    public:
        MultiTierDS_SB_Reserved_Header()
                : sb_reserved_fast_tier_size(0),
                  sb_reserved_medium_tier_size(0) {
        }
        MultiTierDS_SB_Reserved_Header(uint32_t ft_size, uint32_t mt_size)
                : sb_reserved_fast_tier_size(ft_size),
                  sb_reserved_medium_tier_size(mt_size) {
        }
    };

public:
    DS_MultiTier_Impl(Options &opts, std::vector<BlockDevice*> &dev_vec, SuperBlockManager* sb, IndexManager* idx);
    ~DS_MultiTier_Impl();

    // Called by Kvdb_Impl
    int GetDataStorType() override { return 1; }
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

    bool CreateAllVolumes(uint64_t sst_offset) override;
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
    std::string GetKeyByHashEntry(HashEntry *entry) override;
    std::string GetValueByHashEntry(HashEntry *entry) override;


private:
    Options &options_;
    std::vector<BlockDevice *> &bdVec_;
    SuperBlockManager *sbMgr_;
    IndexManager *idxMgr_;

    MultiTierDS_SB_Reserved_Header sbResHeader_;
    //MultiTierDS_SB_Reserved_Header sbResHeader_;
    //std::vector<MultiTierDS_SB_Reserved_Volume> sbResVolVec_;

    FastTier *ft_;
    MediumTier *mt_;

    uint32_t segTotalNum_;
    uint64_t sstLengthOnDisk_;

    KVTime* lastTime_;

    uint32_t maxValueLen_;

private:
    uint32_t getTotalFreeSegs();
    uint32_t getReqQueSize();
    uint32_t getSegWriteQueSize();

    void deleteAllVolumes();

    void initSBReservedContentForCreate();
    void updateAllVolSBRes();

    bool verifyTopology();

    uint64_t calcSSTsLengthOnDiskBySegNum(uint32_t seg_num);
    uint32_t calcSegNumForSecTierVolume(uint64_t capacity, uint32_t med_tier_seg_size);
    uint32_t calcSegNumForFstTierVolume(uint64_t capacity, uint64_t sst_offset, uint32_t fst_tier_seg_size, uint32_t med_tier_seg_num);

};    

}// namespace hlkvds
#endif //#ifndef _HLKVDS_DS_MULTITIER_IMPL_H_
