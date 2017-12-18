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

//class FastTier;
//class WarmTier;

class DS_MultiTier_Impl : public DataStor {
public:
    class MultiTierDS_SB_Reserved_Header {
    public :
        uint32_t fst_tier_seg_size;
        char fst_tier_dev_path[hlkvds::DevPathLenLimt];
        uint32_t fst_tier_seg_num;
        uint32_t fst_tier_cur_seg_id;

        uint32_t sec_tier_seg_size;
        uint32_t sec_tier_vol_num;
    public:
        MultiTierDS_SB_Reserved_Header()
                : fst_tier_seg_size(0), fst_tier_seg_num(0),
                fst_tier_cur_seg_id(0), sec_tier_seg_size(0),
                sec_tier_vol_num(0) {
            memset(fst_tier_dev_path, 0, hlkvds::DevPathLenLimt);
        }
        MultiTierDS_SB_Reserved_Header(uint32_t ft_seg_size, std::string ft_path,
                                    uint32_t ft_seg_num, uint32_t ft_cur_id,
                                    uint32_t st_seg_size, uint32_t st_vol_num)
                : fst_tier_seg_size(ft_seg_size), fst_tier_seg_num(ft_seg_num),
                fst_tier_cur_seg_id(ft_cur_id), sec_tier_seg_size(st_seg_size),
                sec_tier_vol_num(st_vol_num) {
            memset(fst_tier_dev_path, 0, hlkvds::DevPathLenLimt);
            memcpy((void*)fst_tier_dev_path, (const void*)ft_path.c_str(), ft_path.size());
        }
    };

    class MultiTierDS_SB_Reserved_Volume {
    public:
        char dev_path[hlkvds::DevPathLenLimt];
        uint32_t segment_num;
        uint32_t cur_seg_id;
    public:
        MultiTierDS_SB_Reserved_Volume() : segment_num(0), cur_seg_id(0) {
            memset(dev_path, 0, hlkvds::DevPathLenLimt);
        }
        MultiTierDS_SB_Reserved_Volume(std::string path, uint32_t seg_num, uint32_t cur_id)
            : segment_num(seg_num), cur_seg_id(cur_id) {
            memset(dev_path, 0, hlkvds::DevPathLenLimt);
            memcpy((void*)dev_path, (const void*)path.c_str(), path.size());
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
    std::vector<MultiTierDS_SB_Reserved_Volume> sbResVolVec_;

    uint32_t segTotalNum_;
    uint64_t sstLengthOnDisk_;

    int shardsNum_;
    std::mutex segMapMtx_;
    std::map<int, SegForReq *> segMap_;
    std::vector<std::mutex *> segMtxVec_;

    KVTime* lastTime_;

    uint32_t maxValueLen_;

    uint32_t fstSegSize_;
    Volume* fstTier_;
    uint32_t fstTierSegNum_;

    uint32_t secSegSize_;
    uint32_t secTierVolNum_;
    std::map<int, Volume *> secTierVolMap_;
    uint32_t secTierSegTotalNum_;

    int pickVolId_;
    std::mutex volIdMtx_;

private:
    void initSBReservedContentForCreate();
    void updateAllVolSBRes();

    bool verifyTopology();

    uint64_t calcSSTsLengthOnDiskBySegNum(uint32_t seg_num);
    uint32_t calcSegNumForSecTierVolume(uint64_t capacity, uint32_t sec_tier_seg_size);
    uint32_t calcSegNumForFstTierVolume(uint64_t capacity, uint64_t sst_offset, uint32_t fst_tier_seg_size, uint32_t sec_tier_seg_num);

};    

}// namespace hlkvds

#endif //#ifndef _HLKVDS_DS_MULTITIER_IMPL_H_
