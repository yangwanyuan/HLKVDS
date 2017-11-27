#ifndef _HLKVDS_VOLUMES_H_
#define _HLKVDS_VOLUMES_H_

#include <stdint.h>
#include <thread>
#include <atomic>  
#include <map>

#include "hlkvds/Options.h"


namespace hlkvds {

class BlockDevice;
class SegmentManager;
class GcManager;
class IndexManager;

class KVSlice;
class HashEntry;

class SegmentOnDisk {
public:
    uint64_t time_stamp;
    uint32_t checksum;
    uint32_t number_keys;
public:
    SegmentOnDisk();
    ~SegmentOnDisk();
    SegmentOnDisk(const SegmentOnDisk& toBeCopied);
    SegmentOnDisk& operator=(const SegmentOnDisk& toBeCopied);

    SegmentOnDisk(uint32_t num);
    void Update();
    void SetKeyNum(uint32_t num) {
        number_keys = num;
    }
};

class Volumes {
public:
    Volumes(BlockDevice* dev, IndexManager* im, Options& opts,
            int vol_id, uint64_t start_off, uint32_t segment_size, uint32_t segment_num,
            uint32_t cur_seg_id);
    ~Volumes();

    void StartThds();
    void StopThds();

    bool GetSST(char* buf, uint64_t length);
    bool SetSST(char* buf, uint64_t length);

    bool Read(char* data, size_t count, off_t offset);
    bool Write(char* data, size_t count, off_t offset);
    
    uint32_t GetTotalFreeSegs();
    uint32_t GetTotalUsedSegs();
    
    void SortSegsByUtils(std::multimap<uint32_t, uint32_t> &cand_map, double utils);
    
    
    bool Alloc(uint32_t& seg_id);
    bool AllocForGC(uint32_t& seg_id);
    void FreeForFailed(uint32_t seg_id);
    void FreeForGC(uint32_t seg_id);
    void Use(uint32_t seg_id, uint32_t free_size);
    
    bool ForeGC();
    void FullGC();
    void BackGC();

    uint32_t GetCurSegId();
    uint64_t GetSSTLength();
    std::string GetDevicePath();

    int GetId() const { return volId_; }

//move from SegmentManager
public:

    static inline size_t SizeOfSegOnDisk() {
        return sizeof(SegmentOnDisk);
    }

    uint32_t GetNumberOfSeg() {
        return segNum_;
    }

    uint32_t GetSegmentSize() {
        return segSize_;
    }

    inline bool CalcSegOffsetFromId(uint32_t seg_id, uint64_t& offset) {
        if (seg_id >= segNum_) {
            return false;
        }
        offset = ((uint64_t) seg_id << segSizeBit_);
        return true;
    }

    inline bool CalcSegIdFromOffset(uint64_t offset, uint32_t& seg_id) {
        seg_id = offset >> segSizeBit_;
        if (seg_id >= segNum_) {
            return false;
        }
        return true;
    }


    bool CalcSegOffsetFromOffset(uint64_t offset, uint64_t& seg_offset);
    bool CalcDataOffsetPhyFromEntry(HashEntry* entry, uint64_t& data_offset);
    bool CalcKeyOffsetPhyFromEntry(HashEntry* entry, uint64_t& key_offset);

    void ModifyDeathEntry(HashEntry &entry);

private:
    BlockDevice* bdev_;
    SegmentManager* segMgr_;
    GcManager* gcMgr_;

    IndexManager *idxMgr_;
    Options& options_;
    int volId_;
    uint64_t startOff_;
    uint32_t segSize_;
    uint32_t segNum_;
    uint32_t segSizeBit_;

    std::thread gcT_;
    std::atomic<bool> gcT_stop_; 

    void GCThdEntry();
};

} //namespace hlkvds

#endif //#ifndef _HLKVDS_VOLUMES_H_
