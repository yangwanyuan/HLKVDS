#include "SegmentManager.h"
#include <math.h>
#include <map>

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

    uint64_t SegmentManager::ComputeSegTableSizeOnDisk(uint32_t seg_num)
    {
        uint64_t segtable_size = sizeof(time_t) + sizeof(SegmentStat) * seg_num;
        uint64_t segtable_size_pages = segtable_size / getpagesize();
        return (segtable_size_pages + 1) * getpagesize();
    }

    uint32_t SegmentManager::ComputeSegNum(uint64_t total_size, uint32_t seg_size)
    {
        uint32_t seg_num  = total_size / seg_size;
        uint32_t seg_size_bit = log2(seg_size);
        uint64_t seg_table_size = SegmentManager::ComputeSegTableSizeOnDisk(seg_num);
        while ( seg_table_size + ((uint64_t)seg_num << seg_size_bit) > total_size)
        {
            seg_num--;
            seg_table_size = SegmentManager::ComputeSegTableSizeOnDisk(seg_num);
        }
        return seg_num;
    }

    bool SegmentManager::InitSegmentForCreateDB(uint64_t start_offset, uint32_t segment_size, uint32_t number_segments)
    {
        uint32_t align_bit = log2(ALIGNED_SIZE);
        if ( segment_size != (segment_size>>align_bit)<<align_bit )
        {
            return false;
        }

        startOff_ = start_offset;
        segSize_ = segment_size;
        segSizeBit_ = log2(segSize_);
        segNum_ = number_segments;
        curSegId_ = 0;
        dataStartOff_ = start_offset + SegmentManager::ComputeSegTableSizeOnDisk(segNum_);
        dataEndOff_ = dataStartOff_ + ((uint64_t)segNum_ << segSizeBit_);

        
        //init segment table
        SegmentStat seg_stat;
        for (uint32_t seg_index = 0; seg_index < segNum_; seg_index++)
        {
           segTable_.push_back(seg_stat);
        }

        return true;
    }

    bool SegmentManager::LoadSegmentTableFromDevice(uint64_t start_offset, uint32_t segment_size, uint32_t num_seg, uint32_t current_seg)
    {
        startOff_ = start_offset;
        segSize_ = segment_size;
        segSizeBit_ = log2(segSize_);
        segNum_ = num_seg;
        curSegId_ = current_seg;
        dataStartOff_ = start_offset + SegmentManager::ComputeSegTableSizeOnDisk(segNum_);
        dataEndOff_ = dataStartOff_ + ((uint64_t)segNum_ << segSizeBit_);

        uint64_t offset = startOff_;
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
            // If state is reserved, need reset segmentstat
            if (segs_stat[seg_index].state != SegUseStat::RESERVED)
            {
                seg_stat = segs_stat[seg_index];
            }
            segTable_.push_back(seg_stat);
        }
        delete[] segs_stat;
        return true;
    }

    bool SegmentManager::WriteSegmentTableToDevice()
    {
        SegmentStat* segs_stat = new SegmentStat[segNum_];
        uint64_t length = sizeof(SegmentStat) * (uint64_t)segNum_;
        for(uint32_t seg_index = 0; seg_index < segNum_; seg_index++)
        {
            // If state is reserved, need reset segmentstat
            if (segTable_[seg_index].state != SegUseStat::RESERVED)
            {
                segs_stat[seg_index] = segTable_[seg_index];
                //__INFO("seg_id = %d, stat = %d, free_size = %d, death_size = %d", seg_index, segTable_[seg_index].state, segTable_[seg_index].free_size, segTable_[seg_index].death_size);
            }
            else
            {
                __WARN("Segment Maybe not write to device! seg_id = %d", seg_index);
            }
        }
        if (bdev_->pWrite(segs_stat, length, startOff_) != (ssize_t)length)
        {
                __ERROR("can not write segment to device!");
                delete[] segs_stat;
                return false;
        }
        delete[] segs_stat;
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

    bool SegmentManager::Alloc(uint32_t& seg_id)
    {
        //for first use !!
        std::lock_guard<std::mutex> l(mtx_);
        if (segTable_[curSegId_].state == SegUseStat::FREE)
        {
            seg_id = curSegId_;
            segTable_[curSegId_].state = SegUseStat::RESERVED;
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
                segTable_[curSegId_].state = SegUseStat::RESERVED;

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

    void SegmentManager::Free(uint32_t seg_id)
    {
        std::lock_guard<std::mutex> l(mtx_);
        segTable_[seg_id].state = SegUseStat::FREE;
    }

    void SegmentManager::Use(uint32_t seg_id, uint32_t free_size)
    {
        std::lock_guard<std::mutex> l(mtx_);
        segTable_[seg_id].state = SegUseStat::USED;
        segTable_[seg_id].free_size = free_size;
    }

    void SegmentManager::ModifyDeathEntry(HashEntry &entry)
    {
        uint32_t seg_id;
        uint64_t offset = entry.GetHeaderOffsetPhy();
        ComputeSegIdFromOffset(offset, seg_id);

        uint16_t data_size = entry.GetDataSize();
        uint32_t death_size = (uint32_t)data_size + (uint32_t)IndexManager::SizeOfDataHeader();

        std::lock_guard<std::mutex> l(mtx_);
        segTable_[seg_id].death_size += death_size;
    }

    bool SegmentManager::FindGCSegs(std::vector<uint32_t> &gc_list)
    {
        std::multimap<uint32_t, uint32_t> cand_map;
        uint32_t used_size;
        std::unique_lock<std::mutex> lck(mtx_, std::defer_lock);
        for(uint32_t index = 0; index < segNum_; index++)
        {
            lck.lock();
            if (segTable_[index].state == SegUseStat::USED)
            {
                used_size = segSize_ - ( segTable_[index].free_size + segTable_[index].death_size + SegmentManager::SizeOfSegOnDisk());
                cand_map.insert( std::pair<uint32_t, uint32_t> (used_size, index) );
            }
            lck.unlock();
        }

        for (std::multimap<uint32_t, uint32_t>::iterator iter = cand_map.begin(); iter != cand_map.end(); iter++)
        {
            __INFO("cand_map index=%d, used_size = %d", iter->second, iter->first);
        }
        used_size = 0;
        for (std::multimap<uint32_t, uint32_t>::iterator iter = cand_map.begin(); iter != cand_map.end(); iter++)
        {
            __INFO("seg index=%d, used_size = %d need do GC", iter->second, iter->first);
            gc_list.push_back(iter->second);
            used_size += iter->first;
            if (used_size > segSize_)
            {
                break;
            }
        }
        __INFO("total gc seg_num = %d", (int)gc_list.size());

        if (used_size < segSize_)
        {
            return false;
        }
        return true;

    }

    SegmentManager::SegmentManager(BlockDevice* bdev) : 
        dataStartOff_(0), dataEndOff_(0), segSize_(0), segSizeBit_(0), segNum_(0), curSegId_(0), isFull_(false), bdev_(bdev){}

    SegmentManager::~SegmentManager()
    {
        segTable_.clear();
    }
} //end namespace kvdb
