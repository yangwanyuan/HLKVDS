#include <math.h>

#include "Volumes.h"
#include "BlockDevice.h"
#include "SegmentManager.h"
#include "GcManager.h"
#include "IndexManager.h"

using namespace std;
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
    
Volumes::Volumes(BlockDevice* dev, IndexManager* im,
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

Volumes::~Volumes() {
    delete segMgr_;
    delete gcMgr_;
}

void Volumes::StartThds() {
    gcT_stop_.store(false);
    gcT_ = std::thread(&Volumes::GCThdEntry, this);
} 

void Volumes::StopThds() {
    gcT_stop_.store(true);
    gcT_.join();
}

void Volumes::GCThdEntry() {
    __DEBUG("GC thread start!!");
    while (!gcT_stop_) {
        gcMgr_->BackGC();
        usleep(1000000);
    } __DEBUG("GC thread stop!!");
}

bool Volumes::Alloc(uint32_t& seg_id) {
    return segMgr_->Alloc(seg_id);
}

bool Volumes::GetSST(char* buf, uint64_t length) {
    return segMgr_->Get(buf, length);
}

bool Volumes::SetSST(char* buf, uint64_t length) {
    return segMgr_->Set(buf, length);
}

bool Volumes::Read(char* data, size_t count, off_t offset) {
    uint64_t phy_offset = offset + startOff_;
    if (bdev_->pRead(data, count, phy_offset) != (ssize_t)count) {
        __ERROR("Read data error!!!");
        return false;
    }
    return true;
}

bool Volumes::Write(char* data, size_t count, off_t offset) {
    uint64_t phy_offset = offset + startOff_;
    if (bdev_->pWrite(data, count, phy_offset) != (ssize_t)count) {
        __ERROR("Write data error!!!");
        return false;
    }
    return true;
}

uint32_t Volumes::GetCurSegId() {
    return segMgr_->GetNowSegId();
}

string Volumes::GetDevicePath() {
    return bdev_->GetDevicePath();
}

uint64_t Volumes::GetSSTLength() {
    return SegmentManager::SizeOfSegmentStat() * segNum_;
}
////////////////////////////////////////////////////

uint32_t Volumes::GetTotalFreeSegs() {
    return segMgr_->GetTotalFreeSegs();
}

uint32_t Volumes::GetTotalUsedSegs() {
    return segMgr_->GetTotalUsedSegs();
}

void Volumes::SortSegsByUtils(std::multimap<uint32_t, uint32_t> &cand_map, double utils) {
    return segMgr_->SortSegsByUtils(cand_map, utils);
}

bool Volumes::AllocForGC(uint32_t& seg_id) {
    return segMgr_->AllocForGC(seg_id);
}
void Volumes::FreeForFailed(uint32_t seg_id) {
    return segMgr_->FreeForFailed(seg_id);
}
void Volumes::FreeForGC(uint32_t seg_id) {
    return segMgr_->FreeForGC(seg_id);
}
void Volumes::Use(uint32_t seg_id, uint32_t free_size) {
    return segMgr_->Use(seg_id, free_size);
}

//////////////////////////////////////////////////


bool Volumes::ForeGC() {
    return gcMgr_->ForeGC();
}

void Volumes::FullGC() {
    return gcMgr_->FullGC();
}

void Volumes::BackGC() {
    return gcMgr_->BackGC();
}

/////////////////////////////
// move from SegmentManager

bool Volumes::ComputeSegOffsetFromOffset(uint64_t offset,
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

bool Volumes::ComputeDataOffsetPhyFromEntry(HashEntry* entry,
                                            uint64_t& data_offset) {
    uint64_t seg_offset = 0;
    uint64_t header_offset = entry->GetHeaderOffset();
    if (!ComputeSegOffsetFromOffset(header_offset, seg_offset)) {
        return false;
    }
    data_offset = seg_offset + entry->GetDataOffsetInSeg();
    return true;
}

bool Volumes::ComputeKeyOffsetPhyFromEntry(HashEntry* entry,
                                           uint64_t& key_offset) {
    uint64_t seg_offset = 0;
    uint64_t header_offset = entry->GetHeaderOffset();
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

void Volumes::ModifyDeathEntry(HashEntry &entry) {
    uint32_t seg_id;
    uint64_t offset = entry.GetHeaderOffset();
    if (!ComputeSegIdFromOffset(offset, seg_id)) {
        __ERROR("Compute Seg Id Wrong!!! offset = %ld", offset);
    }

    uint16_t data_size = entry.GetDataSize();
    uint32_t death_size = (uint32_t) data_size
            + (uint32_t) IndexManager::SizeOfDataHeader();

    segMgr_->AddDeathSize(seg_id, death_size);
}

}// namespace hlkvds
