#include <math.h>

#include "SegmentManager.h"
#include "SuperBlockManager.h"
#include "IndexManager.h"
#include "Volumes.h"

namespace hlkvds {

SegmentOnDisk::SegmentOnDisk() :
    checksum(0), number_keys(0) {
    time_stamp = KVTime::GetNow();
}

SegmentOnDisk::~SegmentOnDisk() {
}

SegmentOnDisk::SegmentOnDisk(const SegmentOnDisk& toBeCopied) {
    time_stamp = toBeCopied.time_stamp;
    checksum = toBeCopied.checksum;
    number_keys = toBeCopied.number_keys;
}

SegmentOnDisk& SegmentOnDisk::operator=(const SegmentOnDisk& toBeCopied) {
    time_stamp = toBeCopied.time_stamp;
    checksum = toBeCopied.checksum;
    number_keys = toBeCopied.number_keys;
    return *this;
}

SegmentOnDisk::SegmentOnDisk(uint32_t num) :
    checksum(0), number_keys(num) {
    time_stamp = KVTime::GetNow();
}

void SegmentOnDisk::Update() {
    time_stamp = KVTime::GetNow();
}

bool SegmentManager::Get(char* buf, uint64_t length) {
    uint64_t stat_size = sizeof(SegmentStat);
    uint64_t stat_table_size = Volumes::ComputeSegTableSizeOnDisk(segNum_);
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
    uint64_t stat_size = sizeof(SegmentStat);
    uint64_t stat_table_size = Volumes::ComputeSegTableSizeOnDisk(segNum_);
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

void SegmentManager::InitMeta(uint32_t segment_size, uint32_t number_segments, uint32_t cur_seg_id) {
    segSize_ = segment_size;
    segNum_ = number_segments;
    curSegId_ = cur_seg_id;

    segSizeBit_ = log2(segSize_);
    maxValueLen_ = segSize_ - SegmentManager::SizeOfSegOnDisk() - IndexManager::SizeOfHashEntryOnDisk();

    freedCounter_ = segNum_;
    usedCounter_ = 0;
    reservedCounter_ = 0;
}

void SegmentManager::UpdateMetaToSB() {
    sbMgr_->SetCurSegId(curSegId_);
}

bool SegmentManager::ComputeSegOffsetFromOffset(uint64_t offset,
                                                uint64_t& seg_offset) {
    uint32_t seg_id = 0;
    if (!ComputeSegIdFromOffset(offset, seg_id)) {
        return false;
    }
    if (!ComputeSegOffsetFromId(seg_id, seg_offset)) {
        return false;
    }
    return true;
}

bool SegmentManager::ComputeDataOffsetPhyFromEntry(HashEntry* entry,
                                                   uint64_t& data_offset) {
    uint64_t seg_offset = 0;
    uint64_t header_offset = entry->GetHeaderOffsetPhy();
    if (!ComputeSegOffsetFromOffset(header_offset, seg_offset)) {
        return false;
    }
    data_offset = seg_offset + entry->GetDataOffsetInSeg();
    return true;
}

bool SegmentManager::ComputeKeyOffsetPhyFromEntry(HashEntry* entry,
                                                    uint64_t& key_offset) {
    uint64_t seg_offset = 0;
    uint64_t header_offset = entry->GetHeaderOffsetPhy();
    if (!ComputeSegOffsetFromOffset(header_offset, seg_offset)) {
        return false;
    }
    uint32_t data_size = entry->GetDataSize();
    if ( data_size == ALIGNED_SIZE) {
        key_offset = seg_offset + entry->GetNextHeadOffsetInSeg() - entry->GetKeySize();
    } else {
        key_offset = seg_offset + entry->GetNextHeadOffsetInSeg() - entry->GetKeySize() - data_size;
    }
    return true;
}

bool SegmentManager::Alloc(uint32_t& seg_id) {
    std::unique_lock < std::mutex > l(mtx_);
    if (freedCounter_ <= SEG_RESERVED_FOR_GC) {
        return false;
    }

    l.unlock();
    return AllocForGC(seg_id);
}

bool SegmentManager::AllocForGC(uint32_t& seg_id) {
    //for first use !!
    std::lock_guard < std::mutex > l(mtx_);
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
    std::lock_guard < std::mutex > l(mtx_);
    segTable_[seg_id].state = SegUseStat::FREE;
    segTable_[seg_id].free_size = 0;
    segTable_[seg_id].death_size = 0;

    reservedCounter_--;
    freedCounter_++;
    __DEBUG("Free Segment seg_id = %d", seg_id);
}

void SegmentManager::FreeForGC(uint32_t seg_id) {
    std::lock_guard < std::mutex > l(mtx_);
    segTable_[seg_id].state = SegUseStat::FREE;
    segTable_[seg_id].free_size = 0;
    segTable_[seg_id].death_size = 0;

    usedCounter_--;
    freedCounter_++;
    __DEBUG("Free Segment For GC, seg_id = %d", seg_id);
}

void SegmentManager::Use(uint32_t seg_id, uint32_t free_size) {
    std::lock_guard < std::mutex > l(mtx_);
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

void SegmentManager::ModifyDeathEntry(HashEntry &entry) {
    uint32_t seg_id;
    uint64_t offset = entry.GetHeaderOffsetPhy();
    if (!ComputeSegIdFromOffset(offset, seg_id)) {
        __ERROR("Compute Seg Id Wrong!!! offset = %ld", offset);
    }

    uint16_t data_size = entry.GetDataSize();
    uint32_t death_size = (uint32_t) data_size
            + (uint32_t) IndexManager::SizeOfDataHeader();

    std::lock_guard < std::mutex > l(mtx_);
    segTable_[seg_id].death_size += death_size;
}

uint32_t SegmentManager::GetTotalFreeSegs() {
    std::lock_guard < std::mutex > l(mtx_);
    return freedCounter_;
}

uint32_t SegmentManager::GetTotalUsedSegs() {
    std::lock_guard < std::mutex > l(mtx_);
    return usedCounter_;
}

void SegmentManager::SortSegsByUtils(
                                     std::multimap<uint32_t, uint32_t> &cand_map,
                                     double utils) {
    std::lock_guard < std::mutex > lck(mtx_);
    uint32_t used_size;
    uint32_t thld = (uint32_t)(segSize_ * utils);

    for (uint32_t index = 0; index < segNum_; index++) {
        if (segTable_[index].state == SegUseStat::USED) {
            used_size = segSize_ - (segTable_[index].free_size
                    + segTable_[index].death_size
                    + (uint32_t) SegmentManager::SizeOfSegOnDisk());
            if (used_size < thld) {
                cand_map.insert(std::pair<uint32_t, uint32_t>(used_size, index));
            }
        }
    } __DEBUG("There is tatal %lu segments utils under %f", cand_map.size(), utils);
}

SegmentManager::SegmentManager(SuperBlockManager* sbm, Options &opt)
    : segSize_(0), segSizeBit_(0), segNum_(0), curSegId_(0),
    usedCounter_(0), freedCounter_(0), reservedCounter_(0),
    maxValueLen_(0), sbMgr_(sbm), options_(opt) {
}

SegmentManager::~SegmentManager() {
    segTable_.clear();
}

} //end namespace hlkvds
