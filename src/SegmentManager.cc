#include "SegmentManager.h"

namespace kvdb{

    SegmentOnDisk::SegmentOnDisk():
        checksum(0), number_keys(0)
    {
        time_stamp = KVTime::GetNow();
    }

    SegmentOnDisk::~SegmentOnDisk(){}

    SegmentOnDisk::SegmentOnDisk(const SegmentOnDisk& toBeCopied)
    {
        time_stamp = toBeCopied.time_stamp;
        checksum = toBeCopied.checksum;
        number_keys = toBeCopied.number_keys;
    }

    SegmentOnDisk& SegmentOnDisk::operator=(const SegmentOnDisk& toBeCopied)
    {
        time_stamp = toBeCopied.time_stamp;
        checksum = toBeCopied.checksum;
        number_keys = toBeCopied.number_keys;
        return *this;
    }
    
    SegmentOnDisk::SegmentOnDisk(uint32_t num):
        checksum(0), number_keys(num)
    {
        time_stamp = KVTime::GetNow();
    }

    void SegmentOnDisk::Update()
    {
        time_stamp = KVTime::GetNow();
    }

    bool SegmentManager::InitSegmentForCreateDB(uint64_t device_capacity, uint64_t meta_size, uint32_t segment_size)
    {
        if ((segment_size != ((segment_size>>12)<<12)) || (device_capacity < meta_size))
        {
            return false;
        }

        startOffset_ = meta_size;
        segSize_ = segment_size;
        segNum_ = (device_capacity - meta_size) / segment_size;
        curSegId_ = 0;

        
        //init segment table
        SegmentStat seg_stat;
        for (uint32_t seg_index = 0; seg_index < segNum_; seg_index++)
        {
           segTable_.push_back(seg_stat);
        }

        //write all segments to device
        SegmentOnDisk seg_ondisk;
        uint64_t offset = startOffset_;
        for (uint32_t seg_index = 0; seg_index < segNum_; seg_index++)
        {
            //write seg to device
            if ( bdev_->pWrite(&seg_ondisk, sizeof(SegmentOnDisk), offset) != sizeof(SegmentOnDisk))
            {
                __ERROR("can not write segment to device!");
                return false;
            }
            offset += segSize_;
        }
        
        //SegmentOnDisk seg_ondisk(now_time);
        zeros_ = new char[segSize_];
        memset(zeros_, 0 , segSize_);
        return true;
    }

    bool SegmentManager::LoadSegmentTableFromDevice(uint64_t meta_size, uint32_t segment_size, uint32_t num_seg, uint32_t current_seg)
    {
        startOffset_ = meta_size;
        segSize_ = segment_size;
        segNum_ = num_seg;
        curSegId_ = current_seg;

        uint64_t offset = startOffset_;
        for (uint32_t seg_index = 0; seg_index < segNum_; seg_index++)
        {
            SegmentOnDisk seg_ondisk;
            //write seg to device
            if ( bdev_->pRead(&seg_ondisk, sizeof(SegmentOnDisk), offset) != sizeof(SegmentOnDisk))
            {
                __ERROR("can not write segment to device!");
                return false;
            }
            offset += segSize_;
            SegmentStat seg_stat;
            if (seg_ondisk.number_keys != 0)
            {
                seg_stat.state = in_use;
            }
            else
            {
                seg_stat.state = un_use;
            }
            segTable_.push_back(seg_stat);
        }
        
        zeros_ = new char[segSize_];
        memset(zeros_, 0 , segSize_);
        return true;
    }

    bool SegmentManager::GetEmptySegId(uint32_t& seg_id)
    {
        //if (isFull_)
        //{
        //    return false;
        //}

        //seg_id = curSegId_; 

        //for first use !!
        if (segTable_[curSegId_].state == un_use)
        {
            seg_id = curSegId_;
            return true;
        }

        uint32_t seg_index = curSegId_ + 1; 
        
        while(seg_index != curSegId_)
        {
            if (segTable_[seg_index].state == un_use)
            {
                seg_id = seg_index;
                return true;
            }
            seg_index++;
            if ( seg_index == segNum_)
            {
                seg_index = 0;
            }
        }
        return false;
    }

    bool SegmentManager::ComputeSegOffsetFromId(uint32_t seg_id, uint64_t& offset)
    {
        if (seg_id >= segNum_)
        {
            return false;
        }
        offset = startOffset_ + seg_id * segSize_;   
        return true;
    }

    bool SegmentManager::ComputeSegIdFromOffset(uint64_t offset, uint32_t& seg_id)
    {
        uint64_t end_offset = startOffset_ + ((uint64_t)segSize_) * ((uint64_t)(segNum_ - 1));
        if (offset < startOffset_ || offset > end_offset)
        {
            return false;
        }
        seg_id = (offset - startOffset_ )/ segSize_;
        return true;
    }

    bool SegmentManager::ComputeSegOffsetFromOffset(uint64_t offset, uint64_t& seg_offset)
    {
        uint32_t seg_id = 0;
        if (!ComputeSegIdFromOffset(offset, seg_id))
        {
            return false;
        }
        if (!ComputeSegOffsetFromId(seg_id, seg_offset))
        {
            return false;
        }
        return true;
    }

    bool SegmentManager::ComputeDataOffsetPhyFromEntry(HashEntry* entry, uint64_t& data_offset)
    {
        uint64_t seg_offset = 0;
        uint64_t header_offset = entry->GetHeaderOffsetPhy();
        if (!ComputeSegOffsetFromOffset(header_offset, seg_offset))
        {
            return false;
        }
        data_offset = seg_offset + entry->GetDataOffsetInSeg();
        return true;
    }

    void SegmentManager::Update(uint32_t seg_id)
    {
        curSegId_ = seg_id;
        segTable_[curSegId_].state = in_use;

        //segTable_[seg_id].state = in_use;

        //uint64_t seg_index = seg_id + 1; 
        //
        //while(seg_index != seg_id)
        //{
        //    if (segTable_[seg_index].state == un_use)
        //    {
        //        curSegId_ = seg_index;
        //        return;
        //    }
        //    seg_index++;
        //    if ( seg_index == segNum_)
        //    {
        //        seg_index = 0;
        //    }
        //}
        //
        //isFull_ = true;
        return;
    }

    SegmentManager::SegmentManager(BlockDevice* bdev) : 
        startOffset_(0), segSize_(0), segNum_(0), curSegId_(0), isFull_(false), bdev_(bdev), mtx_(Mutex())
    {
        return;
    }

    SegmentManager::~SegmentManager()
    {
        delete[] zeros_;
        segTable_.clear();
    }
} //end namespace kvdb
