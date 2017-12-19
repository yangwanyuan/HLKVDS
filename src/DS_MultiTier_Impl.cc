#include <math.h>
#include "DataStor.h"
#include "DS_MultiTier_Impl.h"
#include "Db_Structure.h"
#include "BlockDevice.h"
#include "SuperBlockManager.h"
#include "IndexManager.h"
#include "Volume.h"
//#include "Tier.h"
#include "SegmentManager.h"

using namespace std;

namespace hlkvds {

DS_MultiTier_Impl::DS_MultiTier_Impl(Options& opts, vector<BlockDevice*> &dev_vec,
                            SuperBlockManager* sb, IndexManager* idx) :
        options_(opts), bdVec_(dev_vec), sbMgr_(sb), idxMgr_(idx),
        segTotalNum_(0), sstLengthOnDisk_(0), maxValueLen_(0),
        fstSegSize_(0), fstTier_(NULL), fstTierSegNum_(0),
        secSegSize_(0), secTierVolNum_(0), secTierSegTotalNum_(0),
        pickVolId_(-1), segTimeoutT_stop_(false) {
    shardsNum_ = options_.shards_num;
    lastTime_ = new KVTime();
}

DS_MultiTier_Impl::~DS_MultiTier_Impl() {
    deleteAllSegments();
    deleteAllVolumes();
    delete lastTime_;
}

void DS_MultiTier_Impl::CreateAllSegments() {
    std::unique_lock<std::mutex> lck_seg_map(segMapMtx_);
    for (int i = 0; i < shardsNum_; i++) {
        SegForReq * seg = new SegForReq(fstTier_, idxMgr_, options_.expired_time);
        segMap_.insert(make_pair(i, seg));

        std::mutex *seg_mtx = new mutex();
        segMtxVec_.push_back(seg_mtx);
    }
}

void DS_MultiTier_Impl::StartThds() {
    for (int i = 0; i < shardsNum_; i++) {
        ReqsMergeWQ *req_wq = new ReqsMergeWQ(this, 1);
        req_wq->Start();
        reqWQVec_.push_back(req_wq);
    }

    segWteWQ_ = new SegmentWriteWQ(this, options_.seg_write_thread);
    segWteWQ_->Start();

    segTimeoutT_stop_.store(false);
    segTimeoutT_ = std::thread(&DS_MultiTier_Impl::SegTimeoutThdEntry, this);

    fstTier_->StartThds();
    for (uint32_t i = 0; i < secTierVolNum_; i++) {
        secTierVolMap_[i]->StartThds();
    }
}

void DS_MultiTier_Impl::StopThds() {
    for (uint32_t i = 0; i < secTierVolNum_; i++) {
        secTierVolMap_[i]->StopThds();
    }

    fstTier_->StopThds();

    segTimeoutT_stop_.store(true);
    segTimeoutT_.join();

    for (int i = 0; i < shardsNum_; i++) {
        ReqsMergeWQ *req_wq = reqWQVec_[i];
        req_wq->Stop();
        delete req_wq;
    }
    reqWQVec_.clear();

    if (segWteWQ_) {
        segWteWQ_->Stop();
        delete segWteWQ_;
    }
}

void DS_MultiTier_Impl::printDeviceTopologyInfo() {
    __INFO("\nDB Device Topology information:\n"
            "\t First Tier Seg Size         : %d\n"
            "\t First Tier Dev Path         : %s\n"
            "\t First Tier Seg Number       : %d\n"
            "\t First Tier Cur Seg ID       : %d\n"
            "\t Sec Tier Seg Size           : %d\n"
            "\t Sec Tier Volume Number      : %d",
            sbResHeader_.fst_tier_seg_size, sbResHeader_.fst_tier_dev_path,
            sbResHeader_.fst_tier_seg_num, sbResHeader_.fst_tier_cur_seg_id,
            sbResHeader_.sec_tier_seg_size, sbResHeader_.sec_tier_vol_num);
    for (uint32_t i = 0 ; i < sbResHeader_.sec_tier_vol_num; i++) {
        __INFO("\nVolume[%d] : dev_path = %s, segment_num = %d, cur_seg_id = %d",
                i, sbResVolVec_[i].dev_path,
                sbResVolVec_[i].segment_num,
                sbResVolVec_[i].cur_seg_id);
    }
}

void DS_MultiTier_Impl::printDynamicInfo() {
    __INFO("\n DS_MultiTier_Impl Dynamic information: \n"
            "\t Request Queue Size          : %d\n"
            "\t Segment Write Queue Size    : %d\n",
            getReqQueSize(), getSegWriteQueSize());
}

Status DS_MultiTier_Impl::WriteData(KVSlice& slice) {
    if (slice.GetDataLen() > maxValueLen_) {
        return Status::NotSupported("Data length cann't be longer than max segment size");
    }

    Request *req = new Request(slice);

    int shards_id = calcShardId(slice);
    req->SetShardsWQId(shards_id);
    ReqsMergeWQ *req_wq = reqWQVec_[shards_id];
    req_wq->Add_task(req);

    req->Wait();
    Status s = updateMeta(req);
    delete req;
    return s;
}

Status DS_MultiTier_Impl::WriteBatchData(WriteBatch *batch) {
    uint32_t seg_id = 0;
    bool ret;

    Volume *vol = fstTier_;

    //TODO: use GcSegment first, need abstract segment.
    while (!vol->Alloc(seg_id)) {
        ret = vol->ForeGC();
        if (!ret) {
            __ERROR("Cann't get a new Empty Segment.\n");
            return Status::Aborted("Can't allocate a Empty Segment.");
        }
    }

    SegForSlice *seg = new SegForSlice(vol, idxMgr_);
    for (std::list<KVSlice *>::iterator iter = batch->batch_.begin();
            iter != batch->batch_.end(); iter++) {
        if (seg->TryPut(*iter)) {
            seg->Put(*iter);
        }
        else {
            __ERROR("The Batch is too large, can't put in one segment");
            delete seg;
            return Status::Aborted("Batch is too large.");
        }
    }

    seg->SetSegId(seg_id);
    ret = seg->WriteSegToDevice();
    if(!ret) {
        __ERROR("Write batch segment to device failed");
        vol->FreeForFailed(seg_id);
        delete seg;
        return Status::IOError("could not write batch segment to device ");
    }

    uint32_t free_size = seg->GetFreeSize();
    vol->Use(seg_id, free_size);
    seg->UpdateToIndex();
    delete seg;
    return Status::OK();
}

Status DS_MultiTier_Impl::ReadData(KVSlice &slice, std::string &data) {
    HashEntry *entry;
    entry = &slice.GetHashEntry();

    Volume *vol = fstTier_;

    uint64_t data_offset = 0;
    if (!vol->CalcDataOffsetPhyFromEntry(entry, data_offset)) {
        return Status::Aborted("Calculate data offset failed.");
    }

    uint16_t data_len = entry->GetDataSize();

    if (data_len == 0) {
        //The key is not exist
        return Status::NotFound("Key is not found.");
    }

    char *mdata = new char[data_len];
    if (!vol->Read(mdata, data_len, data_offset)) {
        __ERROR("Could not read data at position");
        delete[] mdata;
        return Status::IOError("Could not read data at position.");
    }
    data.assign(mdata, data_len);
    delete[] mdata;

    __DEBUG("get key: %s, data offset %ld, head_offset is %ld", slice.GetKeyStr().c_str(), data_offset, entry->GetHeaderOffset());

    return Status::OK();
}

void DS_MultiTier_Impl::ManualGC() {
    fstTier_->FullGC();
}

bool DS_MultiTier_Impl::GetSBReservedContent(char* buf, uint64_t length) {
    if (length != SuperBlockManager::ReservedRegionLength()) {
        return false;
    }
    memset((void*)buf, 0, length);

    updateAllVolSBRes();

    //Get sbResHeader_
    uint64_t header_size = sizeof(MultiTierDS_SB_Reserved_Header);
    memcpy((void*)buf, (const void*)&sbResHeader_, header_size);
    __DEBUG("SB_RESERVED_HEADER: fst_tier_seg_size = %d, fst_tier_dev_path = %s, fst_tier_seg_num = %d, fst_tier_cur_seg_id = %d, sec_tier_seg_size = %d, sec_tier_vol_num = %d ",
            sbResHeader_.fst_tier_seg_size, sbResHeader_.fst_tier_dev_path,
            sbResHeader_.fst_tier_seg_num, sbResHeader_.fst_tier_cur_seg_id,
            sbResHeader_.sec_tier_seg_size, sbResHeader_.sec_tier_vol_num);

    //Get sbResVolVec_
    uint64_t sb_res_vol_size = sizeof(MultiTierDS_SB_Reserved_Volume);
    uint32_t vol_num = sbResHeader_.sec_tier_vol_num;
    char * sb_res_vol_ptr = buf + header_size;

    for (uint32_t i = 0; i < vol_num; i++) {
        memcpy((void*)(sb_res_vol_ptr), (const void*)&sbResVolVec_[i], sb_res_vol_size);
        sb_res_vol_ptr += sb_res_vol_size;
        __DEBUG("Volume [i], device_path= %s, segment_num = %d, current_seg_id = %d",
                i, sbResVolVec_[i].dev_path, sbResVolVec_[i].segment_num,
                sbResVolVec_[i].cur_seg_id);
    }

    return true;
}

bool DS_MultiTier_Impl::SetSBReservedContent(char* buf, uint64_t length) {
    if (length != SuperBlockManager::ReservedRegionLength()) {
        return false;
    }

    //Set sbResHeader_
    uint64_t header_size = sizeof(MultiTierDS_SB_Reserved_Header);
    memcpy((void*)&sbResHeader_, (const void*)buf, header_size);
    __DEBUG("SB_RESERVED_HEADER: fst_tier_seg_size = %d, fst_tier_dev_path = %s, fst_tier_seg_num = %d, fst_tier_cur_seg_id = %d, sec_tier_seg_size = %d, sec_tier_vol_num = %d ",
            sbResHeader_.fst_tier_seg_size, sbResHeader_.fst_tier_dev_path,
            sbResHeader_.fst_tier_seg_num, sbResHeader_.fst_tier_cur_seg_id,
            sbResHeader_.sec_tier_seg_size, sbResHeader_.sec_tier_vol_num);

    //Set sbResVolVec_
    uint64_t sb_res_vol_size = sizeof(MultiTierDS_SB_Reserved_Volume);
    uint32_t vol_num = sbResHeader_.sec_tier_vol_num;
    char *sb_res_vol_ptr = buf + header_size;

    for (uint32_t i = 0; i < vol_num; i++) {
        MultiTierDS_SB_Reserved_Volume sb_res_vol;
        memcpy((void*)&sb_res_vol, (const void*)sb_res_vol_ptr, sb_res_vol_size);
        sbResVolVec_.push_back(sb_res_vol);
        sb_res_vol_ptr += sb_res_vol_size;
        __DEBUG("Volume [%d], device_path= %s, segment_num = %d, current_seg_id = %d",
                i, sbResVolVec_[i].dev_path, sbResVolVec_[i].segment_num,
                sbResVolVec_[i].cur_seg_id);
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
    uint64_t fst_tier_sst_length = fstTier_->GetSSTLength();
    if ( !fstTier_->GetSST(buf_ptr, fst_tier_sst_length) ) {
        return false;
    }
    buf_ptr += fst_tier_sst_length;

    //Copy Second Tier SST
    for (uint32_t i = 0; i < secTierVolNum_; i++) {
        uint64_t vol_sst_length = secTierVolMap_[i]->GetSSTLength();
        if ( !secTierVolMap_[i]->GetSST(buf_ptr, vol_sst_length) ) {
            return false;
        }
        buf_ptr += vol_sst_length;
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
    uint64_t fst_tier_sst_length = fstTier_->GetSSTLength();
    if (!fstTier_->SetSST(buf_ptr, fst_tier_sst_length) ) {
        return false;
    }
    buf_ptr += fst_tier_sst_length;

    //Set Second Tier SST
    for (uint32_t i = 0; i < secTierVolNum_; i++) {
        uint64_t vol_sst_length = secTierVolMap_[i]->GetSSTLength();
        if ( !secTierVolMap_[i]->SetSST(buf_ptr, vol_sst_length) ) {
            return false;
        }
        buf_ptr += vol_sst_length;
    }
    return true;
}

bool DS_MultiTier_Impl::CreateAllVolumes(uint64_t sst_offset) {
    fstSegSize_ = options_.segment_size;
    secSegSize_ = options_.secondary_seg_size;

    maxValueLen_ = fstSegSize_ - Volume::SizeOfSegOnDisk() - IndexManager::SizeOfHashEntryOnDisk();
    secTierVolNum_ = bdVec_.size() - 1;

    //Create WarmTier Volumes
    for (uint32_t i = 0; i < secTierVolNum_; i++ ) {
        BlockDevice *bdev = bdVec_[i+1];
        uint64_t device_capacity = bdev->GetDeviceCapacity();
        uint32_t seg_num = calcSegNumForSecTierVolume(device_capacity, secSegSize_);
        secTierSegTotalNum_ += seg_num;
        Volume *vol = new Volume(bdev, idxMgr_, options_, i, 0, secSegSize_, seg_num, 0);
        secTierVolMap_.insert( pair<int, Volume*>(i, vol) );
    }

    //Create FastTier Volumes
    BlockDevice *bdev = bdVec_[0];
    uint64_t device_capacity = bdev->GetDeviceCapacity();
    uint32_t seg_num = calcSegNumForFstTierVolume(device_capacity, sst_offset, secTierSegTotalNum_, secSegSize_);
    fstTierSegNum_ = seg_num;

    segTotalNum_ = fstTierSegNum_ + secTierSegTotalNum_;
    sstLengthOnDisk_ = calcSSTsLengthOnDiskBySegNum(segTotalNum_);

    uint64_t start_off = sst_offset + sstLengthOnDisk_;
    Volume *vol = new Volume(bdev, idxMgr_, options_, 0, start_off, fstSegSize_, fstTierSegNum_, 0);
    fstTier_ = vol;

    initSBReservedContentForCreate();

    return true;
}

bool DS_MultiTier_Impl::OpenAllVolumes() {
    fstSegSize_ = sbResHeader_.fst_tier_seg_size;
    fstTierSegNum_ = sbResHeader_.fst_tier_seg_num;
    uint32_t fst_tier_cur_seg_id = sbResHeader_.fst_tier_cur_seg_id;

    secSegSize_ = sbResHeader_.sec_tier_seg_size;
    secTierVolNum_ = sbResHeader_.sec_tier_vol_num;

    maxValueLen_ = fstSegSize_ - Volume::SizeOfSegOnDisk() - IndexManager::SizeOfHashEntryOnDisk();
    sstLengthOnDisk_ = sbMgr_->GetSSTRegionLength();

    if (!verifyTopology()) {
        __ERROR("TheDevice Topology is inconsistent");
        return false;
    }

    //Create First Tier
    BlockDevice *fst_tier_bdev = bdVec_[0];
    uint64_t start_off = sbMgr_->GetSSTRegionOffset() + sstLengthOnDisk_;

    fstTier_ = new Volume(fst_tier_bdev, idxMgr_, options_, 100, start_off, fstSegSize_, fstTierSegNum_, fst_tier_cur_seg_id);

    //Create Second Tier
    for (uint32_t i = 0; i < secTierVolNum_; i++) {
        BlockDevice *bdev = bdVec_[i+1];
        uint32_t seg_num = sbResVolVec_[i].segment_num;
        uint32_t cur_seg_id = sbResVolVec_[i].cur_seg_id;
        Volume *vol = new Volume(bdev, idxMgr_, options_, i, 0, secSegSize_, seg_num, cur_seg_id);
        secTierVolMap_.insert( pair<int, Volume*>(i, vol) );
        secTierSegTotalNum_ += seg_num;
    }

    return true;
}

void DS_MultiTier_Impl::ModifyDeathEntry(HashEntry &entry) {
    fstTier_->ModifyDeathEntry(entry);
}

std::string DS_MultiTier_Impl::GetKeyByHashEntry(HashEntry *entry) {
    uint64_t key_offset = 0;

    Volume *vol = fstTier_;

    if (!vol->CalcKeyOffsetPhyFromEntry(entry, key_offset)) {
        return "";
    }
    __DEBUG("key offset: %lu",key_offset);
    uint16_t key_len = entry->GetKeySize();
    char *mkey = new char[key_len+1];
    if (!vol->Read(mkey, key_len, key_offset)) {
        __ERROR("Could not read data at position");
        delete[] mkey;
        return "";
    }
    mkey[key_len] = '\0';
    string res(mkey, key_len);
    //__INFO("Iterator key is %s, key_offset = %lu, key_len = %u", mkey, key_offset, key_len);
    delete[] mkey;

    return res;
}

std::string DS_MultiTier_Impl::GetValueByHashEntry(HashEntry *entry) {
    uint64_t data_offset = 0;

    Volume *vol = fstTier_;

    if (!vol->CalcDataOffsetPhyFromEntry(entry, data_offset)) {
        return "";
    }
    __DEBUG("data offset: %lu",data_offset);
    uint16_t data_len = entry->GetDataSize();
    if ( data_len ==0 ) {
        return "";
    }
    char *mdata = new char[data_len+1];
    if (!vol->Read(mdata, data_len, data_offset)) {
        __ERROR("Could not read data at position");
        delete[] mdata;
        return "";
    }
    mdata[data_len]= '\0';
    string res(mdata, data_len);
    //__INFO("Iterator value is %s, data_offset = %lu, data_len = %u", mdata, data_offset, data_len);
    delete[] mdata;

    return res;
}

void DS_MultiTier_Impl::deleteAllVolumes() {
    delete fstTier_;

    map<int, Volume *>::iterator iter;
    for (iter = secTierVolMap_.begin(); iter != secTierVolMap_.end(); ) {
        Volume *vol = iter->second;
        delete vol;
        secTierVolMap_.erase(iter++);
    }
}

void DS_MultiTier_Impl::deleteAllSegments() {
    std::unique_lock<std::mutex> lck_seg_map(segMapMtx_);
    for (int i = 0; i< shardsNum_; i++) {
        SegForReq* seg = segMap_[i];
        delete seg;
        std::mutex *seg_mtx = segMtxVec_[i];
        delete seg_mtx;
    }
    segMap_.clear();
    segMtxVec_.clear();
}


void DS_MultiTier_Impl::initSBReservedContentForCreate() {
    sbResHeader_.fst_tier_seg_size = fstSegSize_;
    string ft_dev_path = fstTier_->GetDevicePath();
    memcpy((void*)&sbResHeader_.fst_tier_dev_path, (const void*)ft_dev_path.c_str(), ft_dev_path.size());
    sbResHeader_.fst_tier_seg_num = fstTierSegNum_;
    sbResHeader_.fst_tier_cur_seg_id = fstTier_->GetCurSegId();

    sbResHeader_.sec_tier_seg_size = secSegSize_;
    sbResHeader_.sec_tier_vol_num = secTierVolNum_;

    for (uint32_t i = 0; i < secTierVolNum_; i++) {
        MultiTierDS_SB_Reserved_Volume sb_res_vol;
        string dev_path = secTierVolMap_[i]->GetDevicePath();
        memcpy((void*)&sb_res_vol.dev_path, (const void*)dev_path.c_str(), dev_path.size());
        sb_res_vol.segment_num = secTierVolMap_[i]->GetNumberOfSeg();
        sb_res_vol.cur_seg_id = secTierVolMap_[i]->GetCurSegId();
        sbResVolVec_.push_back(sb_res_vol);
    }

}

void DS_MultiTier_Impl::updateAllVolSBRes() {
    uint32_t fst_tier_cur_id = fstTier_->GetCurSegId();
    sbResHeader_.fst_tier_cur_seg_id = fst_tier_cur_id;

    for (uint32_t i = 0; i< sbResHeader_.sec_tier_vol_num; i++) {
        uint32_t cur_id = secTierVolMap_[i]->GetCurSegId();
        sbResVolVec_[i].cur_seg_id = cur_id;
    }
}

bool DS_MultiTier_Impl::verifyTopology() {
    if (secTierVolNum_ != bdVec_.size() - 1) {
        __ERROR("record secTierVolNum_ = %d, total block device number: %d", secTierVolNum_, (int)bdVec_.size() );
        return false;
    }
    //Verify First Tier
    string fst_tier_record_path = string(sbResHeader_.fst_tier_dev_path);
    string fst_tier_device_path = bdVec_[0]->GetDevicePath();
    if (fst_tier_record_path != fst_tier_device_path) {
        __ERROR("fst_tier_record_path: %s, fst_tier_device_path:%s ", fst_tier_record_path.c_str(), fst_tier_device_path.c_str());
        return false;
    }

    //Verify Second Tier
    for (uint32_t i = 0; i < secTierVolNum_; i++) {
        string record_path = string(sbResVolVec_[i].dev_path);
        string device_path = bdVec_[i+1]->GetDevicePath();
        if (record_path != device_path) {
            __ERROR("record_path: %s, device_path:%s ", record_path.c_str(), device_path.c_str());
            return false;
        }
    }
    return true;
}

uint64_t DS_MultiTier_Impl::calcSSTsLengthOnDiskBySegNum(uint32_t seg_num) {
    uint64_t sst_pure_length = sizeof(SegmentStat) * seg_num;
    uint64_t sst_length = sst_pure_length + sizeof(time_t);
    uint64_t sst_pages = sst_length / getpagesize();
    sst_length = (sst_pages + 1) * getpagesize();
    return sst_length;
}

uint32_t DS_MultiTier_Impl::calcSegNumForSecTierVolume(uint64_t capacity, uint32_t sec_tier_seg_size) {
    return capacity / sec_tier_seg_size;
}

uint32_t DS_MultiTier_Impl::calcSegNumForFstTierVolume(uint64_t capacity, uint64_t sst_offset, uint32_t sec_tier_seg_num, uint32_t fst_tier_seg_size) {
    uint64_t valid_capacity = capacity - sst_offset;
    uint32_t seg_num_candidate = valid_capacity / fst_tier_seg_size;

    uint32_t seg_num_total = seg_num_candidate + sec_tier_seg_num;
    uint64_t sst_length = calcSSTsLengthOnDiskBySegNum( seg_num_total );

    uint32_t seg_size_bit = log2(fst_tier_seg_size);

    while( sst_length + ((uint64_t) seg_num_candidate << seg_size_bit) > valid_capacity) {
         seg_num_candidate--;
         seg_num_total = seg_num_candidate + sec_tier_seg_num;
         sst_length = calcSSTsLengthOnDiskBySegNum( seg_num_total );
    }
    return seg_num_candidate;
}

uint32_t DS_MultiTier_Impl::getTotalFreeSegs() {
    uint32_t free_num = fstTier_->GetTotalFreeSegs();
    for (uint32_t i = 0; i < secTierVolNum_; i++) {
        free_num += secTierVolMap_[i]->GetTotalFreeSegs();
    }
    return free_num;
}

uint32_t DS_MultiTier_Impl::getReqQueSize() {
    if (reqWQVec_.empty()) {
        return 0;
    }
    int reqs_num = 0;
    for (int i = 0; i< shardsNum_; i++) {
        ReqsMergeWQ *req_wq = reqWQVec_[i];
        reqs_num += req_wq->Size();
    }
    return reqs_num;
}

uint32_t DS_MultiTier_Impl::getSegWriteQueSize() {
    return (!segWteWQ_)? 0 : segWteWQ_->Size();
}

Status DS_MultiTier_Impl::updateMeta(Request *req) {
    bool res = req->GetWriteStat();
    // update index
    if (!res) {
        return Status::Aborted("Write failed");
    }
    KVSlice *slice = &req->GetSlice();
    res = idxMgr_->UpdateIndex(slice);
    // minus the segment delete counter
    SegForReq *seg = req->GetSeg();
    if (!seg->CommitedAndGetNum()) {
        idxMgr_->AddToReaper(seg);
    }
    return Status::OK();
}

int DS_MultiTier_Impl::calcShardId(KVSlice& slice) {
    return KeyDigestHandle::Hash(&slice.GetDigest()) % shardsNum_;
}

void DS_MultiTier_Impl::ReqMerge(Request* req) {
    int shard_id = req->GetShardsWQId();

    std::unique_lock<std::mutex> lck_seg_map(segMapMtx_, std::defer_lock);

    std::mutex *seg_mtx = segMtxVec_[shard_id];
    std::unique_lock<std::mutex> lck_seg(*seg_mtx);

    lck_seg_map.lock();
    SegForReq *seg = segMap_[shard_id];
    lck_seg_map.unlock();

    if (seg->TryPut(req)) {
        seg->Put(req);
    } else {
        seg->Completion();
        segWteWQ_->Add_task(seg);

        seg = new SegForReq(fstTier_, idxMgr_, options_.expired_time);

        lck_seg_map.lock();
        std::map<int, SegForReq*>::iterator iter = segMap_.find(shard_id);
        segMap_.erase(iter);
        segMap_.insert( make_pair(shard_id, seg) );
        lck_seg_map.unlock();

        seg->Put(req);
    }

}

void DS_MultiTier_Impl::SegWrite(SegForReq *seg) {
    Volume *vol =  seg->GetSelfVolume();

    uint32_t seg_id = 0;
    bool res;
    while (!vol->Alloc(seg_id)) {
        res = vol->ForeGC();
        if (!res) {
            __ERROR("Cann't get a new Empty Segment.\n");
            seg->Notify(res);
        }
    }
    uint32_t free_size = seg->GetFreeSize();
    seg->SetSegId(seg_id);
    res = seg->WriteSegToDevice();
    if (res) {
        vol->Use(seg_id, free_size);
    } else {
        vol->FreeForFailed(seg_id);
    }
    seg->Notify(res);
}

void DS_MultiTier_Impl::SegTimeoutThdEntry() {
    __DEBUG("Segment Timeout thread start!!");
    std::unique_lock<std::mutex> lck_seg_map(segMapMtx_, std::defer_lock);

    while (!segTimeoutT_stop_) {
        for ( int i = 0; i < shardsNum_; i++) {
            std::mutex *mtx = segMtxVec_[i];
            std::unique_lock<std::mutex> l(*mtx);

            lck_seg_map.lock();
            SegForReq *seg = segMap_[i];
            lck_seg_map.unlock();

            if (seg->IsExpired()) {
                seg->Completion();

                segWteWQ_->Add_task(seg);
                seg = new SegForReq(fstTier_, idxMgr_, options_.expired_time);

                lck_seg_map.lock();
                std::map<int, SegForReq*>::iterator iter = segMap_.find(i);
                segMap_.erase(iter);
                segMap_.insert( make_pair(i, seg) );
                lck_seg_map.unlock();
            }
        }

        usleep(options_.expired_time);
    } __DEBUG("Segment Timeout thread stop!!");
}

} // namespace hlkvds
