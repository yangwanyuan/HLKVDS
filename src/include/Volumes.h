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

class SuperBlockManager;
class IndexManager;

class KVSlice;
class HashEntry;

class Volumes {
public:
    Volumes(BlockDevice* dev, SuperBlockManager* sbm, IndexManager* im, Options& opts, uint64_t start_off, uint32_t segment_size, uint32_t segment_num, uint32_t cur_seg_id);
    ~Volumes();

    void StartThds();
    void StopThds();

    bool GetSST(char* buf, uint64_t length);
    bool SetSST(char* buf, uint64_t length);
    void InitMeta(uint32_t segment_size, uint32_t number_segments, uint32_t cur_seg_id);

    bool Read(char* data, size_t count, off_t offset);
    bool Write(char* data, size_t count, off_t offset);
    
    void UpdateMetaToSB();
    bool ComputeDataOffsetPhyFromEntry(HashEntry* entry, uint64_t& data_offset);
    bool ComputeKeyOffsetPhyFromEntry(HashEntry* entry, uint64_t& key_offset);
    void ModifyDeathEntry(HashEntry &entry);
    
    uint64_t GetDataRegionSize();
    uint32_t GetTotalFreeSegs();
    uint32_t GetTotalUsedSegs();
    
    void SortSegsByUtils(std::multimap<uint32_t, uint32_t> &cand_map, double utils);
    
    uint32_t GetSegmentSize();
    uint32_t GetNumberOfSeg();
    
    bool Alloc(uint32_t& seg_id);
    bool AllocForGC(uint32_t& seg_id);
    void FreeForFailed(uint32_t seg_id);
    void FreeForGC(uint32_t seg_id);
    void Use(uint32_t seg_id, uint32_t free_size);
    
    bool ComputeSegOffsetFromId(uint32_t seg_id, uint64_t& offset);
 
    bool ForeGC();
    void FullGC();
    void BackGC();

    static uint32_t ComputeSegNum(uint64_t total_size, uint32_t seg_size);
    static uint64_t ComputeSegTableSizeOnDisk(uint64_t seg_num);

private:
    BlockDevice* bdev_;
    SegmentManager* segMgr_;
    GcManager* gcMgr_;

    SuperBlockManager *sbMgr_;
    IndexManager *idxMgr_;
    Options& options_;
    uint64_t startOff_;
    uint32_t segSize_;
    uint32_t segNum_;
    uint32_t curSegId_;

    std::thread gcT_;
    std::atomic<bool> gcT_stop_; 

    void GCThdEntry();
};

} //namespace hlkvds

#endif //#ifndef _HLKVDS_VOLUMES_H_
