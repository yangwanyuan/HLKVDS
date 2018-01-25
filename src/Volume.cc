#include <math.h>

#include "Volume.h"
#include "BlockDevice.h"
#include "SegmentManager.h"
#include "GcManager.h"
#include "IndexManager.h"

using namespace std;
namespace hlkvds {         

Volume::Volume(BlockDevice* dev, IndexManager* im,
                Options& opts, int vol_id, uint64_t start_off,
                uint32_t segment_size, uint32_t segment_num,
                uint32_t cur_seg_id)
    : bdev_(dev), segMgr_(NULL), gcMgr_(NULL), idxMgr_(im),
        options_(opts), volId_(vol_id), startOff_(start_off), segSize_(segment_size),
        segNum_(segment_num), segSizeBit_(0) {

    segSizeBit_ = log2(segSize_);

    segMgr_ = new SegmentManager(options_, segSize_, segNum_, cur_seg_id, segSizeBit_);
    gcMgr_ = new GcManager(idxMgr_, this, options_);
}

Volume::~Volume() {
    delete segMgr_;
    delete gcMgr_;
}

void Volume::StartThds() {
    gcT_stop_.store(false);
    gcT_ = std::thread(&Volume::GCThdEntry, this);
} 

void Volume::StopThds() {
    gcT_stop_.store(true);
    gcT_.join();
}

void Volume::GCThdEntry() {
    __DEBUG("GC thread start!!");
    while (!gcT_stop_) {
        gcMgr_->BackGC();
        usleep(1000000);
    } __DEBUG("GC thread stop!!");
}

bool Volume::Alloc(uint32_t& seg_id) {
    return segMgr_->Alloc(seg_id);
}

bool Volume::GetSST(char* buf, uint64_t length) {
    return segMgr_->Get(buf, length);
}

bool Volume::SetSST(char* buf, uint64_t length) {
    return segMgr_->Set(buf, length);
}

bool Volume::Read(char* data, size_t count, off_t offset) {
    uint64_t phy_offset = offset + startOff_;
    if (bdev_->pRead(data, count, phy_offset) != (ssize_t)count) {
        __ERROR("Read data error!!!");
        return false;
    }
    return true;
}

bool Volume::Write(char* data, size_t count, off_t offset) {
    uint64_t phy_offset = offset + startOff_;
    if (bdev_->pWrite(data, count, phy_offset) != (ssize_t)count) {
        __ERROR("Write data error!!!");
        return false;
    }
    return true;
}

uint32_t Volume::GetCurSegId() {
    return segMgr_->GetNowSegId();
}

string Volume::GetDevicePath() {
    return bdev_->GetDevicePath();
}

uint64_t Volume::GetSSTLength() {
    return SegmentManager::SizeOfSegmentStat() * segNum_;
}
////////////////////////////////////////////////////

uint32_t Volume::GetTotalFreeSegs() {
    return segMgr_->GetTotalFreeSegs();
}

uint32_t Volume::GetTotalUsedSegs() {
    return segMgr_->GetTotalUsedSegs();
}

void Volume::SortSegsByUtils(std::multimap<uint32_t, uint32_t> &cand_map, double utils) {
    return segMgr_->SortSegsByUtils(cand_map, utils);
}

void Volume::SortSegsByTS(std::multimap<uint32_t, uint32_t> &cand_map, uint32_t max_seg_num) {
    return segMgr_->SortSegsByTS(cand_map, max_seg_num);
}

bool Volume::AllocForGC(uint32_t& seg_id) {
    return segMgr_->AllocForGC(seg_id);
}
void Volume::FreeForFailed(uint32_t seg_id) {
    return segMgr_->FreeForFailed(seg_id);
}
void Volume::FreeForGC(uint32_t seg_id) {
    return segMgr_->FreeForGC(seg_id);
}
void Volume::Use(uint32_t seg_id, uint32_t free_size) {
    return segMgr_->Use(seg_id, free_size);
}

//////////////////////////////////////////////////

bool Volume::ForeGC() {
    return gcMgr_->ForeGC();
}

void Volume::FullGC() {
    return gcMgr_->FullGC();
}

void Volume::BackGC() {
    return gcMgr_->BackGC();
}

/////////////////////////////
// move from SegmentManager

bool Volume::CalcSegOffsetFromOffset(uint64_t offset,
                                         uint64_t& seg_offset) {
    uint32_t seg_id = 0;
    if (!CalcSegIdFromOffset(offset, seg_id)) {
        return false;
    }
    if (!CalcSegOffsetFromId(seg_id, seg_offset)) {
        return false;
    }
    return true;
}

bool Volume::CalcDataOffsetPhyFromEntry(HashEntry* entry,
                                            uint64_t& data_offset) {
    uint64_t seg_offset = 0;
    uint64_t header_offset = entry->GetHeaderOffset();
    if (!CalcSegOffsetFromOffset(header_offset, seg_offset)) {
        return false;
    }
    data_offset = seg_offset + entry->GetDataOffsetInSeg();
    return true;
}

bool Volume::CalcKeyOffsetPhyFromEntry(HashEntry* entry,
                                           uint64_t& key_offset) {
    uint64_t seg_offset = 0;
    uint64_t header_offset = entry->GetHeaderOffset();
    if (!CalcSegOffsetFromOffset(header_offset, seg_offset)) {
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

void Volume::ModifyDeathEntry(HashEntry &entry) {
    uint32_t seg_id;
    uint64_t offset = entry.GetHeaderOffset();
    if (!CalcSegIdFromOffset(offset, seg_id)) {
        __ERROR("Calculate Seg Id Wrong!!! offset = %ld", offset);
    }

    uint16_t data_size = entry.GetDataSize();
    uint32_t death_size = (uint32_t) data_size
            + (uint32_t) IndexManager::SizeOfDataHeader();

    segMgr_->AddDeathSize(seg_id, death_size);
}

}// namespace hlkvds
