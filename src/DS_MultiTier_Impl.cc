#include <math.h>
#include "DataStor.h"
#include "DS_MultiTier_Impl.h"
#include "Db_Structure.h"
#include "BlockDevice.h"
#include "SuperBlockManager.h"
#include "IndexManager.h"
#include "Volume.h"
#include "Tier.h"
#include "SegmentManager.h"

using namespace std;

namespace hlkvds {

DS_MultiTier_Impl::DS_MultiTier_Impl(Options& opts, vector<BlockDevice*> &dev_vec,
                            SuperBlockManager* sb, IndexManager* idx) :
        options_(opts), bdVec_(dev_vec), sbMgr_(sb), idxMgr_(idx),
        segTotalNum_(0), sstLengthOnDisk_(0), maxValueLen_(0) {
    ft_ = new FastTier(options_, sbMgr_, idxMgr_);
    mt_ = new MediumTier(options_, sbMgr_, idxMgr_);
    lastTime_ = new KVTime();
}

DS_MultiTier_Impl::~DS_MultiTier_Impl() {
    deleteAllVolumes();
    delete lastTime_;
}

void DS_MultiTier_Impl::CreateAllSegments() {
    ft_->CreateAllSegments();
}

void DS_MultiTier_Impl::StartThds() {
    ft_->StartThds();
    mt_->StartThds();
}

void DS_MultiTier_Impl::StopThds() {
    mt_->StopThds();
    ft_->StopThds();
}

void DS_MultiTier_Impl::printDeviceTopologyInfo() {
    __INFO("\nDB Device Topology Information:");
    ft_->printDeviceTopologyInfo();
    mt_->printDeviceTopologyInfo();
}

void DS_MultiTier_Impl::printDynamicInfo() {
    __INFO("\n\t DS_MultiTier_Impl Dynamic information: \n"
            "\t Request Queue Size          : %d\n"
            "\t Segment Write Queue Size    : %d\n",
            getReqQueSize(), getSegWriteQueSize());
}

Status DS_MultiTier_Impl::WriteData(KVSlice& slice) {
    return ft_->WriteData(slice);
}

Status DS_MultiTier_Impl::WriteBatchData(WriteBatch *batch) {
    return ft_->WriteBatchData(batch);
}

Status DS_MultiTier_Impl::ReadData(KVSlice &slice, std::string &data) {
    return ft_->ReadData(slice, data);
}

void DS_MultiTier_Impl::ManualGC() {
    ft_->ManualGC();
}

bool DS_MultiTier_Impl::GetSBReservedContent(char* buf, uint64_t length) {
    if (length != SuperBlockManager::ReservedRegionLength()) {
        return false;
    }
    memset((void*)buf, 0, length);

    char* buf_ptr = buf;
    //Get sbResHeader_
    uint64_t header_size = sizeof(MultiTierDS_SB_Reserved_Header);
    memcpy((void*)buf_ptr, (const void*)&sbResHeader_, header_size);
    buf_ptr += header_size;
    __DEBUG("SB_RESERVED_HEADER: sb_reserved_fast_tier_size = %d, sb_reserved_medium_tier_size = %d ",
            sbResHeader_.sb_reserved_fast_tier_size, sbResHeader_.sb_reserved_medium_tier_size);

    //Get FastTier
    if (!ft_->GetSBReservedContent(buf_ptr, sbResHeader_.sb_reserved_fast_tier_size)) {
        return false;
    }
    buf_ptr += sbResHeader_.sb_reserved_fast_tier_size;

    //Get MediumTier
    if (!mt_->GetSBReservedContent(buf_ptr, sbResHeader_.sb_reserved_medium_tier_size)) {
        return false;
    }

    return true;
}

bool DS_MultiTier_Impl::SetSBReservedContent(char* buf, uint64_t length) {
    if (length != SuperBlockManager::ReservedRegionLength()) {
        return false;
    }

    char* buf_ptr = buf;
    //Set sbResHeader_
    uint64_t header_size = sizeof(MultiTierDS_SB_Reserved_Header);
    memcpy((void*)&sbResHeader_, (const void*)buf_ptr, header_size);
    buf_ptr += header_size;
    __DEBUG("SB_RESERVED_HEADER: sb_reserved_fast_tier_size = %d, sb_reserved_medium_tier_size = %d ",
            sbResHeader_.sb_reserved_fast_tier_size, sbResHeader_.sb_reserved_medium_tier_size);

    //Set FastTier
    if (!ft_->SetSBReservedContent(buf_ptr, sbResHeader_.sb_reserved_fast_tier_size)) {
        return false;
    }
    buf_ptr += sbResHeader_.sb_reserved_fast_tier_size;

    //Set MediumTier
    if (!mt_->SetSBReservedContent(buf_ptr, sbResHeader_.sb_reserved_medium_tier_size)) {
        return false;
    }

    return true;
}

bool DS_MultiTier_Impl::GetAllSSTs(char* buf, uint64_t length) {
    uint64_t sst_total_length = GetSSTsLengthOnDisk();
    if (length != sst_total_length) {
        return false;
    }
    char *buf_ptr = buf;

    //Copy TS
    int64_t time_len = KVTime::SizeOf();
    lastTime_->Update();
    time_t t = lastTime_->GetTime();
    memcpy((void *)buf_ptr, (const void *)&t, time_len);
    buf_ptr += time_len;
    __DEBUG("memcpy timestamp: %s at %p, at %ld", KVTime::ToChar(*lastTime_), (void *)buf_ptr, (int64_t)(buf_ptr-buf));

    //Copy First Tier SST
    uint64_t fast_tier_sst_length = ft_->GetSSTLength();
    if ( !ft_->GetSST(buf_ptr, fast_tier_sst_length) ) {
        return false;
    }
    buf_ptr += fast_tier_sst_length;

    //Copy Second Tier SST
    uint64_t med_tier_sst_length = mt_->GetSSTLength();
    if ( !mt_->GetSST(buf_ptr, med_tier_sst_length) ) {
        return false;
    }

    return true;
}

bool DS_MultiTier_Impl::SetAllSSTs(char* buf, uint64_t length) {
    uint64_t sst_total_length = GetSSTsLengthOnDisk();
    if (length != sst_total_length) {
        return false;
    }
    char *buf_ptr = buf;

    //Set TS
    int64_t time_len = KVTime::SizeOf();
    time_t t;
    memcpy((void *)&t, (const void *)buf_ptr, time_len);
    lastTime_->SetTime(t);
    buf_ptr += time_len;
    __DEBUG("memcpy timestamp: %s at %p, at %ld", KVTime::ToChar(*lastTime_), (void *)buf_ptr, (int64_t)(buf_ptr-buf));

    //Set First Tier SST
    uint64_t fast_tier_sst_length = ft_->GetSSTLength();
    if (!ft_->SetSST(buf_ptr, fast_tier_sst_length) ) {
        return false;
    }
    buf_ptr += fast_tier_sst_length;

    //Set Second Tier SST
    uint64_t med_tier_sst_length = mt_->GetSSTLength();
    if ( !mt_->SetSST(buf_ptr, med_tier_sst_length) ) {
        return false;
    }

    return true;
}

bool DS_MultiTier_Impl::CreateAllVolumes(uint64_t sst_offset) {
    bool ret;
    ret = mt_->CreateVolume(bdVec_);
    if (!ret) {
        return false;
    }
    uint32_t medTierSegTotalNum_ = mt_->GetSegTotalNum();

    ret = ft_->CreateVolume(bdVec_, sst_offset, medTierSegTotalNum_);
    if (!ret) {
        return false;
    }

    initSBReservedContentForCreate();

    segTotalNum_ = medTierSegTotalNum_ + ft_->GetSegNum();
    sstLengthOnDisk_ = ft_->GetSSTLengthOnDisk();
    maxValueLen_ = ft_->GetMaxValueLen();

    return true;
}

bool DS_MultiTier_Impl::OpenAllVolumes() {
    bool ret;

    ret = ft_->OpenVolume(bdVec_);

    if (!ret) {
        return false;
    }

    ret = mt_->OpenVolume(bdVec_);
    if (!ret) {
        return false;
    }

    segTotalNum_ = mt_->GetSegTotalNum() + ft_->GetSegNum();
    sstLengthOnDisk_ = ft_->GetSSTLengthOnDisk();
    maxValueLen_ = ft_->GetMaxValueLen();

    return true;
}

void DS_MultiTier_Impl::ModifyDeathEntry(HashEntry &entry) {
    return ft_->ModifyDeathEntry(entry);
}

std::string DS_MultiTier_Impl::GetKeyByHashEntry(HashEntry *entry) {
    return ft_->GetKeyByHashEntry(entry);
}

std::string DS_MultiTier_Impl::GetValueByHashEntry(HashEntry *entry) {
    return ft_->GetValueByHashEntry(entry);
}

void DS_MultiTier_Impl::deleteAllVolumes() {
    delete ft_;
    delete mt_;
}

void DS_MultiTier_Impl::initSBReservedContentForCreate() {
    sbResHeader_.sb_reserved_fast_tier_size = ft_->GetSbReservedSize();
    sbResHeader_.sb_reserved_medium_tier_size = mt_->GetSbReservedSize();
}

uint32_t DS_MultiTier_Impl::getTotalFreeSegs() {
    uint32_t free_num = ft_->GetTotalFreeSegs() + mt_->GetTotalFreeSegs();
    return free_num;
}

uint32_t DS_MultiTier_Impl::getReqQueSize() {
    return ft_->getReqQueSize();
}

uint32_t DS_MultiTier_Impl::getSegWriteQueSize() {
    return ft_->getSegWriteQueSize();
}

} // namespace hlkvds
