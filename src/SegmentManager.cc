#include "SegmentManager.h"

namespace kvdb{

    bool SegmentManager::InitSegmentForCreateDB(uint64_t device_capacity, uint64_t meta_size, uint64_t segment_size)
    {
        if ((segment_size != ((segment_size>>12)<<12)) || (device_capacity < meta_size))
            return false;
        num_seg = (device_capacity - meta_size) % segment_size;
        current_seg = 0;
        
        //SegmentOnDisk seg_ondisk(now_time);
        return true;
    }

    bool SegmentManager::LoadSegmentTableFromDevice()
    {
        return true;
    }

    int SegmentManager::FindNextSegId()
    {
        return 0;
    }

    ssize_t SegmentManager::ComputeSegOffsetFromId(uint64_t seg_id)
    {
        return 0;
    }

    uint64_t ComputeSegIdFromOffset(ssize_t offset)
    {
        return 0;
    }

    SegmentManager::SegmentManager(BlockDevice* bdev) : 
        num_seg(0), current_seg(0), m_bdev(bdev)
    {
        return;
    }

    SegmentManager::~SegmentManager()
    {
        //delete m_timestamp;
    }
} //end namespace kvdb
