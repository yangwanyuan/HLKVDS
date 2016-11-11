#include "SegmentManager.h"
#include <math.h>

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

    uint64_t SegmentManager::computeSegTableSize(uint32_t seg_num)
    {
        uint64_t segtable_size = sizeof(time_t) + sizeof(SegmentStat) * seg_num;
        uint64_t segtable_size_pages = segtable_size / getpagesize();
        return (segtable_size_pages + 1) * getpagesize();
    }

    uint32_t SegmentManager::computeSegNum(uint64_t total_size, uint32_t seg_size)
    {
        uint32_t seg_num  = total_size / seg_size;
        uint32_t seg_size_bit = log2(seg_size);
        uint64_t seg_table_size = computeSegTableSize(seg_num);
        while ( seg_table_size + ((uint64_t)seg_num << seg_size_bit) > total_size)
        {
            seg_num--;
            seg_table_size = computeSegTableSize(seg_num);
        }
        return seg_num;
    }

    bool SegmentManager::InitSegmentForCreateDB(uint64_t device_capacity, uint64_t start_offset, uint32_t segment_size)
    {
        uint32_t align_bit = log2(ALIGNED_SIZE);
        if ((segment_size != ((segment_size>>align_bit)<<align_bit)) || (device_capacity < start_offset))
        {
            return false;
        }


        segTableOff_ = start_offset;
        segSize_ = segment_size;
        segSizeBit_ = log2(segSize_);
        segNum_ = computeSegNum(device_capacity - start_offset, segSize_);
        curSegId_ = 0;
        dataStartOff_ = start_offset + computeSegTableSize(segNum_);
        dataEndOff_ = dataStartOff_ + ((uint64_t)segNum_ << segSizeBit_);

        
        //init segment table
        SegmentStat seg_stat;
        for (uint32_t seg_index = 0; seg_index < segNum_; seg_index++)
        {
           segTable_.push_back(seg_stat);
        }

        SegmentStat *segs_stat = new SegmentStat[segNum_];
        uint64_t length = sizeof(SegmentStat) * (uint64_t)segNum_;
        uint64_t offset = segTableOff_;
        if( bdev_->pWrite(segs_stat, length, offset) != (ssize_t)length)
        {
            __ERROR("can not write segment to device!");
            delete[] segs_stat;
            return false;
        }
        delete[] segs_stat;
        return true;
    }

    bool SegmentManager::LoadSegmentTableFromDevice(uint64_t start_offset, uint32_t segment_size, uint32_t num_seg, uint32_t current_seg)
    {
        segTableOff_ = start_offset;
        segSize_ = segment_size;
        segSizeBit_ = log2(segSize_);
        segNum_ = num_seg;
        curSegId_ = current_seg;
        dataStartOff_ = start_offset + computeSegTableSize(segNum_);
        dataEndOff_ = dataStartOff_ + ((uint64_t)segNum_ << segSizeBit_);

        uint64_t offset = segTableOff_;
        SegmentStat* segs_stat = new SegmentStat[segNum_];
        uint64_t length = sizeof(SegmentStat) * (uint64_t)segNum_;
        if (bdev_->pRead(segs_stat, length, offset) != (ssize_t)length)
        {
                __ERROR("can not write segment to device!");
                delete[] segs_stat;
                return false;
        }
        for(uint32_t seg_index = 0; seg_index < segNum_; seg_index++)
        {
            SegmentStat seg_stat;
            seg_stat = segs_stat[seg_index];
            segTable_.push_back(seg_stat);
        }
        delete[] segs_stat;
        return true;
    }

    bool SegmentManager::WriteSegmentTableToDevice(uint64_t offset)
    {
        SegmentStat* segs_stat = new SegmentStat[segNum_];
        uint64_t length = sizeof(SegmentStat) * (uint64_t)segNum_;
        for(uint32_t seg_index = 0; seg_index < segNum_; seg_index++)
        {
            segs_stat[seg_index] = segTable_[seg_index];
        }
        if (bdev_->pWrite(segs_stat, length, offset) != (ssize_t)length)
        {
                __ERROR("can not write segment to device!");
                delete[] segs_stat;
                return false;
        }
        delete[] segs_stat;
        return true;
    }

    bool SegmentManager::AllocSeg(uint32_t& seg_id)
    {
        //for first use !!
        std::lock_guard<std::mutex> l(mtx_);
        if (segTable_[curSegId_].state == SegUseStat::FREE)
        {
            seg_id = curSegId_;
            segTable_[curSegId_].state = SegUseStat::USED;
            return true;
        }

        uint32_t seg_index = curSegId_ + 1; 
        
        while(seg_index != curSegId_)
        {
            if (segTable_[seg_index].state == SegUseStat::FREE)
            {
                seg_id = seg_index;
                // set seg used
                curSegId_ = seg_id;
                segTable_[curSegId_].state = SegUseStat::USED;

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

    void SegmentManager::FreeSeg(uint32_t seg_id)
    {
        std::lock_guard<std::mutex> l(mtx_);
        segTable_[seg_id].state = SegUseStat::FREE;

    }

    SegmentManager::SegmentManager(BlockDevice* bdev) : 
        dataStartOff_(0), dataEndOff_(0), segSize_(0), segSizeBit_(0), segNum_(0), curSegId_(0), isFull_(false), bdev_(bdev){}

    SegmentManager::~SegmentManager()
    {
        segTable_.clear();
    }
} //end namespace kvdb
