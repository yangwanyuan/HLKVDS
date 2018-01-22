#include "SegmentManager.h"
#include "IndexManager.h"
#include "Volume.h"

namespace hlkvds {

bool SegmentManager::Get(char* buf, uint64_t length) {
    uint64_t stat_size = SegmentManager::SizeOfSegmentStat();
    uint64_t stat_table_size = stat_size * segNum_;
    if (length != stat_table_size) {
        return false;
    }
    char *buf_ptr = buf;
    for (uint32_t seg_idx = 0; seg_idx < segNum_; seg_idx++) {
        if (segTable_[seg_idx].state != SegUseStat::RESERVED) {
            memcpy((void *)buf_ptr, (const void *)&segTable_[seg_idx], stat_size);
        } else {
            __WARN("Segment Maybe not write to device! seg_id = %d", seg_idx);
        }
        buf_ptr += stat_size;
    }

    return true;
}

bool SegmentManager::Set(char* buf, uint64_t length) {
    uint64_t stat_size = SegmentManager::SizeOfSegmentStat();
    uint64_t stat_table_size = stat_size * segNum_;
    if (length != stat_table_size) {
        return false;
    }
    char* buf_ptr = buf;
    for (uint32_t seg_idx = 0; seg_idx < segNum_; seg_idx++) {
        SegmentStat seg_stat;
        memcpy((void *)&seg_stat, (const void *)buf_ptr, stat_size);
        if (seg_stat.state == SegUseStat::USED) {
            usedCounter_++;
            freedCounter_--;
        }
        segTable_.push_back(seg_stat);
        buf_ptr += stat_size;
    }

    return true;
}

bool SegmentManager::Alloc(uint32_t& seg_id) {
    std::unique_lock <std::mutex> l(mtx_);
    if (freedCounter_ <= SEG_RESERVED_FOR_GC) {
        return false;
    }

    l.unlock();
    return AllocForGC(seg_id);
}

bool SegmentManager::AllocForGC(uint32_t& seg_id) {
    //for first use !!
    std::lock_guard <std::mutex> l(mtx_);
    if (segTable_[curSegId_].state == SegUseStat::FREE) {
        seg_id = curSegId_;
        segTable_[curSegId_].state = SegUseStat::RESERVED;

        reservedCounter_++;
        freedCounter_--;
        return true;
    }

    uint32_t seg_index = curSegId_ + 1;

    while (seg_index != curSegId_) {
        if (seg_index == segNum_) {
            seg_index = 0;
        }
        if (segTable_[seg_index].state == SegUseStat::FREE) {
            seg_id = seg_index;
            // set seg used
            curSegId_ = seg_id;
            segTable_[curSegId_].state = SegUseStat::RESERVED;

            reservedCounter_++;
            freedCounter_--;
            return true;
        }
        seg_index++;
    }
    return false;
}

void SegmentManager::FreeForFailed(uint32_t seg_id) {
    std::lock_guard <std::mutex> l(mtx_);
    segTable_[seg_id].state = SegUseStat::FREE;
    segTable_[seg_id].free_size = 0;
    segTable_[seg_id].death_size = 0;

    reservedCounter_--;
    freedCounter_++;
    __DEBUG("Free Segment seg_id = %d", seg_id);
}

void SegmentManager::FreeForGC(uint32_t seg_id) {
    std::lock_guard <std::mutex> l(mtx_);
    segTable_[seg_id].state = SegUseStat::FREE;
    segTable_[seg_id].free_size = 0;
    segTable_[seg_id].death_size = 0;

    usedCounter_--;
    freedCounter_++;
    __DEBUG("Free Segment For GC, seg_id = %d", seg_id);
}

void SegmentManager::Use(uint32_t seg_id, uint32_t free_size) {
    std::lock_guard <std::mutex> l(mtx_);
    segTable_[seg_id].state = SegUseStat::USED;
    segTable_[seg_id].free_size = free_size;

    reservedCounter_--;
    usedCounter_++;
    __DEBUG("Used Segment seg_id = %d, free_size = %d", seg_id, free_size);
}

//void SegmentManager::Reserved(uint32_t seg_id)
//{
//    std::lock_guard<std::mutex> l(mtx_);
//    segTable_[seg_id].state = SegUseStat::RESERVED;
//}

void SegmentManager::AddDeathSize(uint32_t seg_id, uint32_t death_size) {
    std::lock_guard <std::mutex> l(mtx_);
    segTable_[seg_id].death_size += death_size;
}

uint32_t SegmentManager::GetTotalFreeSegs() {
    std::lock_guard <std::mutex> l(mtx_);
    return freedCounter_;
}

uint32_t SegmentManager::GetTotalUsedSegs() {
    std::lock_guard <std::mutex> l(mtx_);
    return usedCounter_;
}

void SegmentManager::SortSegsByUtils( std::multimap<uint32_t, uint32_t> &cand_map, double utils) {
    std::lock_guard <std::mutex> lck(mtx_);
    uint32_t used_size;
    uint32_t thld = (uint32_t)(segSize_ * utils);

    for (uint32_t index = 0; index < segNum_; index++) {
        if (segTable_[index].state == SegUseStat::USED) {
            used_size = segSize_ - (segTable_[index].free_size
                    + segTable_[index].death_size
                    + (uint32_t) Volume::SizeOfSegOnDisk());
            if (used_size < thld) {
                cand_map.insert(std::pair<uint32_t, uint32_t>(used_size, index));
            }
        }
    } __DEBUG("There is tatal %lu segments utils under %f", cand_map.size(), utils);
}

void SegmentManager::SortSegsByTS(std::multimap<uint32_t, uint32_t> &cand_map, uint32_t max_seg_num)
{
    std::unique_lock<std::mutex> lck(mtx_, std::defer_lock);
    uint32_t used_size;

    lck.lock();
    std::multimap<uint32_t, uint32_t> used_map;
    for (uint32_t index = 0; index < segNum_; index++) {
        if (segTable_[index].state == SegUseStat::USED) {
            used_size = segSize_ - (segTable_[index].free_size
                    + segTable_[index].death_size
                    + (uint32_t) Volume::SizeOfSegOnDisk());
            used_map.insert(std::pair<uint32_t, uint32_t>(used_size, index));
        }
    }
    lck.unlock();

    for(std::multimap<uint32_t, uint32_t>::iterator iter = used_map.begin(); iter != used_map.end(); iter++) {
        if (max_seg_num == 0) {
            break;
        }
        cand_map.insert(*iter);
        max_seg_num --;
    }
}

SegmentManager::SegmentManager(Options &opt, uint32_t segment_size, uint32_t segment_num, uint32_t cur_seg_id, uint32_t seg_size_bit)
    : segSize_(segment_size), segSizeBit_(seg_size_bit), segNum_(segment_num),
        curSegId_(cur_seg_id), usedCounter_(0), freedCounter_(segNum_),
        reservedCounter_(0), options_(opt) {
}

SegmentManager::~SegmentManager() {
    segTable_.clear();
}

} //end namespace hlkvds
