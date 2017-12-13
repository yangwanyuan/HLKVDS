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

class DS_MultiTier_Impl : public DataStor {
public:
    class MultiTierDS_SB_Reserved_Header {
    public :
        uint32_t tier1_segment_size;
        char tier1_dev_path[hlkvds::DevPathLenLimt];
        uint32_t tier1_segment_num;
        uint32_t tier1_cur_seg_id;

        uint32_t tier2_segment_size;
        uint32_t tier2_volume_num;
    public:
        MultiTierDS_SB_Reserved_Header()
                : tier1_segment_size(0), tier1_segment_num(0),
                tier1_cur_seg_id(0), tier2_segment_size(0),
                tier2_volume_num(0) {
            memset(tier1_dev_path, 0, hlkvds::DevPathLenLimt);
        }
        MultiTierDS_SB_Reserved_Header(uint32_t t1_seg_size, std::string t1_path,
                                    uint32_t t1_seg_num, uint32_t t1_cur_id,
                                    uint32_t t2_seg_size, uint32_t t2_vol_num)
                : tier1_segment_size(t1_seg_size), tier1_segment_num(t1_seg_num),
                tier1_cur_seg_id(t1_cur_id), tier2_segment_size(t2_seg_size),
                tier2_volume_num(t2_vol_num) {
            memset(tier1_dev_path, 0, hlkvds::DevPathLenLimt);
            memcpy((void*)tier1_dev_path, (const void*)t1_path.c_str(), t1_path.size());
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

};    

}// namespace hlkvds

#endif //#ifndef _HLKVDS_DS_MULTITIER_IMPL_H_
