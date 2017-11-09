#include <math.h>

#include "Volumes.h"
#include "BlockDevice.h"
#include "SegmentManager.h"
#include "GcManager.h"

#include "SuperBlockManager.h"
#include "IndexManager.h"

namespace hlkvds {         
    
Volumes::Volumes(BlockDevice* dev, SuperBlockManager* sbm, IndexManager* im, Options& opts, uint64_t start_off)
    : bdev_(dev), segMgr_(NULL), gcMgr_(NULL), sbMgr_(sbm), idxMgr_(im), options_(opts), startOff_(start_off) {
    segMgr_ = new SegmentManager(sbMgr_, options_);
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

void Volumes::InitMeta(uint32_t segment_size, uint32_t number_segments, uint32_t cur_seg_id) {
    return segMgr_->InitMeta(segment_size, number_segments, cur_seg_id);
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

void Volumes::UpdateMetaToSB() {
    return segMgr_->UpdateMetaToSB();
}


uint32_t Volumes::ComputeSegNum(uint64_t total_size, uint32_t seg_size) {
    uint32_t seg_num = total_size / seg_size;
    uint32_t seg_size_bit = log2(seg_size);
    uint64_t seg_table_size = Volumes::ComputeSegTableSizeOnDisk(seg_num);
    while (seg_table_size + ((uint64_t) seg_num << seg_size_bit) > total_size) {
        seg_num--;
        seg_table_size = Volumes::ComputeSegTableSizeOnDisk(seg_num);
    }
    return seg_num;
}

uint64_t Volumes::ComputeSegTableSizeOnDisk(uint64_t seg_num) {
    uint64_t segtable_size = sizeof(SegmentStat) * seg_num;
    uint64_t segtable_size_pages = segtable_size / getpagesize();
    return (segtable_size_pages + 1) * getpagesize();
}
////////////////////////////////////////////////////

bool Volumes::ComputeDataOffsetPhyFromEntry(HashEntry* entry, uint64_t& data_offset) {
    return segMgr_->ComputeDataOffsetPhyFromEntry(entry, data_offset);
}

bool Volumes::ComputeKeyOffsetPhyFromEntry(HashEntry* entry, uint64_t& key_offset) {
    return segMgr_->ComputeKeyOffsetPhyFromEntry(entry, key_offset);
}

void Volumes::ModifyDeathEntry(HashEntry &entry) {
    segMgr_->ModifyDeathEntry(entry);
}

uint64_t Volumes::GetDataRegionSize() {
    return segMgr_->GetDataRegionSize();
}


uint32_t Volumes::GetTotalFreeSegs() {
    return segMgr_->GetTotalFreeSegs();
}

uint32_t Volumes::GetTotalUsedSegs() {
    return segMgr_->GetTotalUsedSegs();
}

void Volumes::SortSegsByUtils(std::multimap<uint32_t, uint32_t> &cand_map, double utils) {
    return segMgr_->SortSegsByUtils(cand_map, utils);
}

uint32_t Volumes::GetSegmentSize() {
    return segMgr_->GetSegmentSize();
}
uint32_t Volumes::GetNumberOfSeg() {
    return segMgr_->GetNumberOfSeg();
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

bool Volumes::ComputeSegOffsetFromId(uint32_t seg_id, uint64_t& offset) {
    return segMgr_->ComputeSegOffsetFromId(seg_id, offset);
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

}// namespace hlkvds
