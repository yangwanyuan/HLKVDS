#include <math.h>
#include "Tier.h"
#include "Db_Structure.h"
#include "BlockDevice.h"
#include "SuperBlockManager.h"
#include "IndexManager.h"
#include "Volume.h"
#include "SegmentManager.h"

using namespace std;

namespace hlkvds {
Tier::Tier() {
}

Tier::~Tier() {
}

FastTier::FastTier(Options& opts, SuperBlockManager* sb, IndexManager* idx) :
        options_(opts), sbMgr_(sb), idxMgr_(idx), maxValueLen_(0),
        segSize_(0), segNum_(0), vol_(NULL) {
    shardsNum_ = options_.shards_num;
}

FastTier::~FastTier() {
    deleteAllSegments();
    if(vol_) {
        delete vol_;
    }
}

void FastTier::CreateAllSegments() {
    std::unique_lock<std::mutex> lck_seg_map(segMapMtx_);
    for (int i = 0; i < shardsNum_; i++) {
        SegForReq * seg = new SegForReq(vol_, idxMgr_, options_.expired_time);
        segMap_.insert(make_pair(i, seg));

        std::mutex *seg_mtx = new mutex();
        segMtxVec_.push_back(seg_mtx);
    }
}

void FastTier::StartThds() {
    for (int i = 0; i < shardsNum_; i++) {
        ReqsMergeWQ *req_wq = new ReqsMergeWQ(this, 1);
        req_wq->Start();
        reqWQVec_.push_back(req_wq);
    }

    segWteWQ_ = new SegmentWriteWQ(this, options_.seg_write_thread);
    segWteWQ_->Start();

    segTimeoutT_stop_.store(false);
    segTimeoutT_ = std::thread(&FastTier::SegTimeoutThdEntry, this);

    vol_->StartThds();
}

void FastTier::StopThds() {
    vol_->StopThds();

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

void FastTier::printDeviceTopologyInfo() {
    __INFO( "\n\t Fast Tier Infomation:\n"
            "\t Fast Tier Seg Size          : %d\n"
            "\t Fast Tier Dev Path          : %s\n"
            "\t Fast Tier Seg Number        : %d\n"
            "\t Fast Tier Cur Seg ID        : %d",
            sbResFastTier_.segment_size, sbResFastTier_.dev_path,
            sbResFastTier_.segment_num, sbResFastTier_.cur_seg_id);
}

void FastTier::printDynamicInfo() {
}

Status FastTier::WriteData(KVSlice& slice) {
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

Status FastTier::WriteBatchData(WriteBatch *batch) {
    uint32_t seg_id = 0;
    bool ret;

    //TODO: use GcSegment first, need abstract segment.
    while (!vol_->Alloc(seg_id)) {
        ret = vol_->ForeGC();
        if (!ret) {
            __ERROR("Cann't get a new Empty Segment.\n");
            return Status::Aborted("Can't allocate a Empty Segment.");
        }
    }

    SegForSlice *seg = new SegForSlice(vol_, idxMgr_);
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
        vol_->FreeForFailed(seg_id);
        delete seg;
        return Status::IOError("could not write batch segment to device ");
    }

    uint32_t free_size = seg->GetFreeSize();
    vol_->Use(seg_id, free_size);
    seg->UpdateToIndex();
    delete seg;
    return Status::OK();
}

Status FastTier::ReadData(KVSlice &slice, std::string &data) {
    HashEntry *entry;
    entry = &slice.GetHashEntry();

    uint64_t data_offset = 0;
    if (!vol_->CalcDataOffsetPhyFromEntry(entry, data_offset)) {
        return Status::Aborted("Calculate data offset failed.");
    }

    uint16_t data_len = entry->GetDataSize();

    if (data_len == 0) {
        //The key is not exist
        return Status::NotFound("Key is not found.");
    }

    char *mdata = new char[data_len];
    if (!vol_->Read(mdata, data_len, data_offset)) {
        __ERROR("Could not read data at position");
        delete[] mdata;
        return Status::IOError("Could not read data at position.");
    }
    data.assign(mdata, data_len);
    delete[] mdata;

    __DEBUG("get key: %s, data offset %ld, head_offset is %ld", slice.GetKeyStr().c_str(), data_offset, entry->GetHeaderOffset());

    return Status::OK();
}

void FastTier::ManualGC() {
    vol_->FullGC();
}

bool FastTier::GetSBReservedContent(char* buf, uint64_t length) {
    if (length != sizeof(MultiTierDS_SB_Reserved_FastTier)) {
        return false;
    }

    sbResFastTier_.cur_seg_id = vol_->GetCurSegId();

    memcpy((void*)buf, (const void*)&sbResFastTier_, length);
    __DEBUG("MultiTierDS_SB_Reserved_FastTier: segment_size = %d, dev_path = %s, segment_num = %d, cur_seg_id = %d",
            sbResFastTier_.segment_size, sbResFastTier_.dev_path,
            sbResFastTier_.segment_num, sbResFastTier_.cur_seg_id);

    return true;
}

bool FastTier::SetSBReservedContent(char* buf, uint64_t length) {
    if (length != sizeof(MultiTierDS_SB_Reserved_FastTier)) {
        return false;
    }

    memcpy((void*)&sbResFastTier_, (const void*)buf, length);
    __DEBUG("MultiTierDS_SB_Reserved_FastTier: segment_size = %d, dev_path = %s, segment_num = %d, cur_seg_id = %d",
            sbResFastTier_.segment_size, sbResFastTier_.dev_path,
            sbResFastTier_.segment_num, sbResFastTier_.cur_seg_id);
    return true;
}

void FastTier::initSBReservedContentForCreate() {
    sbResFastTier_.segment_size = segSize_;
    string dev_path  = vol_->GetDevicePath();
    memcpy((void*)&sbResFastTier_.dev_path, (const void*)dev_path.c_str(), dev_path.size());
    sbResFastTier_.segment_num = segNum_;
    sbResFastTier_.cur_seg_id = vol_->GetCurSegId();
}


bool FastTier::GetSST(char* buff, uint64_t length) {
    return vol_->GetSST(buff, length);
}

bool FastTier::SetSST(char* buff, uint64_t length) {
    return vol_->SetSST(buff, length);
}

//bool FastTier::CreateVolume(BlockDevice *bdev, uint64_t start_offset, uint32_t seg_size, uint32_t seg_num, uint32_t cur_id) {
bool FastTier::CreateVolume(std::vector<BlockDevice*> &bd_vec, int fast_tier_device_num,uint64_t sst_offset, uint32_t warm_tier_total_seg_num) {
    //bdev_ = bdev;
    //segSize_ = options_.segment_size;
    //segNum_ = seg_num;
    //maxValueLen_ = segSize_ - Volume::SizeOfSegOnDisk() - IndexManager::SizeOfHashEntryOnDisk();

    //vol_ = new Volume(bdev_, idxMgr_, options_, 100, start_offset, segSize_, segNum_, cur_id);
    //return true;
    //
    bdev_ = bd_vec[0];
    segSize_ = options_.segment_size;
    uint64_t device_capacity = bdev_->GetDeviceCapacity();
    uint32_t seg_num = calcSegNumForFastTierVolume(device_capacity, sst_offset, segSize_, warm_tier_total_seg_num);
    segNum_ = seg_num;
    maxValueLen_ = segSize_ - Volume::SizeOfSegOnDisk() - IndexManager::SizeOfHashEntryOnDisk();

    uint32_t seg_total_num = segNum_ + warm_tier_total_seg_num;
    sstLengthOnDisk_ = calcSSTsLengthOnDiskBySegNum(seg_total_num);

    uint64_t start_off = sst_offset + sstLengthOnDisk_;
    vol_ = new Volume(bdev_, idxMgr_, options_, 100, start_off, segSize_, segNum_, 0);

    initSBReservedContentForCreate();
    return true;
}

bool FastTier::OpenVolume(std::vector<BlockDevice*> &bd_vec, int fast_tier_device_num) {
    bdev_ = bd_vec[0];
    segSize_ = sbResFastTier_.segment_size;
    segNum_ = sbResFastTier_.segment_num;
    maxValueLen_ = segSize_ - Volume::SizeOfSegOnDisk() - IndexManager::SizeOfHashEntryOnDisk();
    sstLengthOnDisk_ = sbMgr_->GetSSTRegionLength();

    if (!verifyTopology(bd_vec, fast_tier_device_num)) {
        __ERROR("TheDevice Topology is inconsistent");
        return false;
    }

    uint32_t cur_id = sbResFastTier_.cur_seg_id;
    uint64_t start_off = sbMgr_->GetSSTRegionOffset() + sbMgr_->GetSSTRegionLength();
    vol_ = new Volume(bdev_, idxMgr_, options_, 100, start_off, segSize_, segNum_, cur_id);
    return true;
}

bool FastTier::verifyTopology(std::vector<BlockDevice*> &bd_vec, int fast_tier_device_num) {
    string record_path = string(sbResFastTier_.dev_path);
    string device_path = bd_vec[0]->GetDevicePath();
    if (record_path != device_path) {
        __ERROR("record_path: %s, device_path:%s ", record_path.c_str(), device_path.c_str());
        return false;
    }
    return true;
}

uint32_t FastTier::GetTotalFreeSegs() {
    return vol_->GetTotalFreeSegs();
}

uint32_t FastTier::GetTotalUsedSegs() {
    return vol_->GetTotalUsedSegs();
}

uint32_t FastTier::GetCurSegId() {
    return vol_->GetCurSegId();
}

uint64_t FastTier::GetSSTLength() {
    return vol_->GetSSTLength();
}

std::string FastTier::GetDevicePath() {
    return vol_->GetDevicePath();
}

void FastTier::ModifyDeathEntry(HashEntry &entry) {
    vol_->ModifyDeathEntry(entry);
}

std::string FastTier::GetKeyByHashEntry(HashEntry *entry) {
    uint64_t key_offset = 0;

    if (!vol_->CalcKeyOffsetPhyFromEntry(entry, key_offset)) {
        return "";
    }
    __DEBUG("key offset: %lu",key_offset);
    uint16_t key_len = entry->GetKeySize();
    char *mkey = new char[key_len+1];
    if (!vol_->Read(mkey, key_len, key_offset)) {
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

std::string FastTier::GetValueByHashEntry(HashEntry *entry) {
    uint64_t data_offset = 0;

    if (!vol_->CalcDataOffsetPhyFromEntry(entry, data_offset)) {
        return "";
    }
    __DEBUG("data offset: %lu",data_offset);
    uint16_t data_len = entry->GetDataSize();
    if ( data_len ==0 ) {
        return "";
    }
    char *mdata = new char[data_len+1];
    if (!vol_->Read(mdata, data_len, data_offset)) {
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

uint32_t FastTier::getReqQueSize() {
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

uint32_t FastTier::getSegWriteQueSize() {
    return (!segWteWQ_)? 0 : segWteWQ_->Size();
}

Status FastTier::updateMeta(Request *req) {
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

void FastTier::deleteAllSegments() {
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

int FastTier::calcShardId(KVSlice& slice) {
    return KeyDigestHandle::Hash(&slice.GetDigest()) % shardsNum_;
}

uint64_t FastTier::calcSSTsLengthOnDiskBySegNum(uint32_t seg_num) {
    uint64_t sst_pure_length = sizeof(SegmentStat) * seg_num;
    uint64_t sst_length = sst_pure_length + sizeof(time_t);
    uint64_t sst_pages = sst_length / getpagesize();
    sst_length = (sst_pages + 1) * getpagesize();
    return sst_length;
}

uint32_t FastTier::calcSegNumForFastTierVolume(uint64_t capacity, uint64_t sst_offset, uint32_t fst_tier_seg_size, uint32_t sec_tier_seg_num) {
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

void FastTier::ReqMerge(Request* req) {
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

        seg = new SegForReq(vol_, idxMgr_, options_.expired_time);

        lck_seg_map.lock();
        std::map<int, SegForReq*>::iterator iter = segMap_.find(shard_id);
        segMap_.erase(iter);
        segMap_.insert( make_pair(shard_id, seg) );
        lck_seg_map.unlock();

        seg->Put(req);
    }

}

void FastTier::SegWrite(SegForReq *seg) {
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

void FastTier::SegTimeoutThdEntry() {
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
                seg = new SegForReq(vol_, idxMgr_, options_.expired_time);

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

WarmTier::WarmTier(Options& opts, SuperBlockManager* sb, IndexManager* idx) :
        options_(opts), sbMgr_(sb), idxMgr_(idx),
        segSize_(0), segTotalNum_(0), volNum_(0), pickVolId_(-1) {
}

WarmTier::~WarmTier() {
    deleteAllVolume();
}

void WarmTier::CreateAllSegments() {
}

void WarmTier::StartThds() {
    for (uint32_t i = 0; i < volNum_; i++) {
        volMap_[i]->StartThds();
    }
}

void WarmTier::StopThds() {
    for (uint32_t i = 0; i < volNum_; i++) {
        volMap_[i]->StopThds();
    }
}

void WarmTier::printDeviceTopologyInfo() {
    __INFO( "\n\t Warm Tier Infomation:\n"
            "\t Warm Tier Seg Size          : %d\n"
            "\t Warm Tier Vol Num           : %d",
            sbResWarmTier_.segment_size, sbResWarmTier_.volume_num);

    for (uint32_t i = 0 ; i < volNum_; i++) {
        __INFO("\n\t Warm Tier Volume[%d] : dev_path = %s, segment_num = %d, cur_seg_id = %d",
                i, sbResWarmTierVolVec_[i].dev_path,
                sbResWarmTierVolVec_[i].segment_num,
                sbResWarmTierVolVec_[i].cur_seg_id);
    }
}

void WarmTier::printDynamicInfo() {
}

bool WarmTier::GetSBReservedContent(char* buf, uint64_t length) {
    uint32_t except_length = sizeof(MultiTierDS_SB_Reserved_WarmTier_Header) +
                            volNum_ * sizeof(MultiTierDS_SB_Reserved_WarmTier_Volume);
    if (length != except_length) {
        return false;
    }

    updateAllVolSBRes();

    char* buf_ptr = buf;
    uint64_t warm_tier_size = sizeof(MultiTierDS_SB_Reserved_WarmTier_Header);
    memcpy((void*)buf, (const void *)&sbResWarmTier_, warm_tier_size);
    __DEBUG("MultiTierDS_SB_Reserved_WarmTier_Header: segment_size = %d, volume_num = %d",
            sbResWarmTier_.segment_size, sbResWarmTier_.volume_num);
    buf_ptr += warm_tier_size;

    //Get sbResVolVec_
    uint64_t sb_res_vol_size = sizeof(MultiTierDS_SB_Reserved_WarmTier_Volume);
    for (uint32_t i = 0; i < volNum_; i++) {
        memcpy((void*)buf_ptr, (const void*)&sbResWarmTierVolVec_[i], sb_res_vol_size);
        buf_ptr += sb_res_vol_size;
        __DEBUG("MultiTierDS_SB_Reserved_WarmTier_Volume[%d]: device_path = %s, segment_num = %d, current_seg_id = %d",
            i, sbResWarmTierVolVec_[i].dev_path, sbResWarmTierVolVec_[i].segment_num,
            sbResWarmTierVolVec_[i].cur_seg_id);

    }
    return true;
}

bool WarmTier::SetSBReservedContent(char* buf, uint64_t length) {
    char* buf_ptr = buf;
    uint64_t warm_tier_size = sizeof(MultiTierDS_SB_Reserved_WarmTier_Header);
    memcpy((void*)&sbResWarmTier_, (const void *)buf_ptr, warm_tier_size);
    __DEBUG("MultiTierDS_SB_Reserved_WarmTier_Header: segment_size = %d, volume_num = %d",
            sbResWarmTier_.segment_size, sbResWarmTier_.volume_num);
    buf_ptr += warm_tier_size;

    uint64_t except_length = sizeof(MultiTierDS_SB_Reserved_WarmTier_Header) +
                            sbResWarmTier_.volume_num * sizeof(MultiTierDS_SB_Reserved_WarmTier_Volume);
    if (length != except_length) {
        __INFO("!!!!!length = %ld, except_length = %ld, volNum_ = %d", length, except_length, volNum_);
        return false;
    }

    //Get sbResVolVec_
    uint64_t sb_res_vol_size = sizeof(MultiTierDS_SB_Reserved_WarmTier_Volume);
    for (uint32_t i = 0; i < sbResWarmTier_.volume_num; i++) {
        MultiTierDS_SB_Reserved_WarmTier_Volume sb_res_vol;
        memcpy((void*)&sb_res_vol, (const void*)buf_ptr, sb_res_vol_size);
        sbResWarmTierVolVec_.push_back(sb_res_vol);
        buf_ptr += sb_res_vol_size;
        __DEBUG("MultiTierDS_SB_Reserved_WarmTier_Volume[%d]: device_path = %s, segment_num = %d, current_seg_id = %d",
            i, sbResWarmTierVolVec_[i].dev_path, sbResWarmTierVolVec_[i].segment_num,
            sbResWarmTierVolVec_[i].cur_seg_id);
    }
    return true;
}

void WarmTier::initSBReservedContentForCreate() {
    sbResWarmTier_.segment_size = segSize_;
    sbResWarmTier_.volume_num = volNum_;
    for (uint32_t i = 0; i < volNum_; i++) {
        MultiTierDS_SB_Reserved_WarmTier_Volume sb_res_vol;
        string dev_path = volMap_[i]->GetDevicePath();
        memcpy((void*)&sb_res_vol.dev_path, (const void*)dev_path.c_str(), dev_path.size());
        sb_res_vol.segment_num = volMap_[i]->GetNumberOfSeg();
        sb_res_vol.cur_seg_id = volMap_[i]->GetCurSegId();
        sbResWarmTierVolVec_.push_back(sb_res_vol);
    }
}

void WarmTier::updateAllVolSBRes() {
    for (uint32_t i = 0; i < volNum_; i++) {
        uint32_t cur_id = volMap_[i]->GetCurSegId();
        sbResWarmTierVolVec_[i].cur_seg_id = cur_id;
    }

}

bool WarmTier::GetSST(char* buf, uint64_t length) {
    char* buf_ptr = buf;
    for (uint32_t i = 0; i < volNum_; i++) {
        uint64_t vol_sst_length = volMap_[i]->GetSSTLength();
        if ( !volMap_[i]->GetSST(buf_ptr, vol_sst_length) ) {
            return false;
        }
        buf_ptr += vol_sst_length;
    }

    return true;
}

bool WarmTier::SetSST(char* buf, uint64_t length) {
    char* buf_ptr = buf;
    for (uint32_t i = 0; i < volNum_; i++) {
        uint64_t vol_sst_length = volMap_[i]->GetSSTLength();
        if ( !volMap_[i]->SetSST(buf_ptr, vol_sst_length) ) {
            return false;
        }
        buf_ptr += vol_sst_length;
    }
    return true;
}

bool WarmTier::CreateVolume(std::vector<BlockDevice*> &bd_vec, int fast_tier_device_num) {
    segSize_ = options_.secondary_seg_size;
    volNum_ = bd_vec.size() - fast_tier_device_num;

    for (uint32_t i = 0; i < volNum_; i++ ) {
        BlockDevice *bdev = bd_vec[i + fast_tier_device_num];
        uint64_t device_capacity = bdev->GetDeviceCapacity();
        uint32_t seg_num = calcSegNumForVolume(device_capacity, segSize_);
        Volume *vol = new Volume(bdev, idxMgr_, options_, i, 0, segSize_, seg_num, 0);
        volMap_.insert( pair<int, Volume*>(i, vol) );
        segTotalNum_ += seg_num;
    }

    initSBReservedContentForCreate();
    return true;
}

//bool WarmTier::OpenVolume(std::vector<BlockDevice*> &bd_vec, int bd_cursor, uint32_t bd_num, uint32_t seg_size, std::vector<DS_MultiTier_Impl::MultiTierDS_SB_Reserved_Volume> &sb_res_vol_vec) {
bool WarmTier::OpenVolume(std::vector<BlockDevice*> &bd_vec, int fast_tier_device_num) {
    segSize_ = sbResWarmTier_.segment_size;
    volNum_ = sbResWarmTier_.volume_num;

    if (!verifyTopology(bd_vec, fast_tier_device_num)) {
        __ERROR("TheDevice Topology is inconsistent");
        return false;
    }

    for (uint32_t i = 0; i < volNum_; i++) {
        BlockDevice *bdev = bd_vec[i + fast_tier_device_num];
        uint32_t seg_num = sbResWarmTierVolVec_[i].segment_num;
        uint32_t cur_seg_id = sbResWarmTierVolVec_[i].cur_seg_id;
        Volume *vol = new Volume(bdev, idxMgr_, options_, i, 0, segSize_, seg_num, cur_seg_id);
        volMap_.insert( pair<int, Volume*>(i, vol) );
        segTotalNum_ += seg_num;
    }
    return true;
}

bool WarmTier::verifyTopology(std::vector<BlockDevice*> &bd_vec, int fast_tier_device_num) {
    // Verify WarmTier Volume Number
    if (volNum_ != bd_vec.size() - fast_tier_device_num) {
        __ERROR("record WarmTierVolNum_ = %d, total block device number: %d", volNum_, (int)bd_vec.size() );
        return false;
    }

    // Verify WarmTier every Volume
    for (uint32_t i = 0; i < volNum_; i++) {
        string record_path = string(sbResWarmTierVolVec_[i].dev_path);
        string device_path = bd_vec[i+ fast_tier_device_num]->GetDevicePath();
        if (record_path != device_path) {
            __ERROR("record_path: %s, device_path:%s ", record_path.c_str(), device_path.c_str());
            return false;
        }
    }

    return true;

}

uint32_t WarmTier::GetTotalFreeSegs() {
    uint32_t free_num = 0;
    for (uint32_t i = 0; i < volNum_; i++) {
        free_num += volMap_[i]->GetTotalFreeSegs();
    }
    return free_num;
}

uint32_t WarmTier::GetTotalUsedSegs() {
    uint32_t used_num = 0;
    for (uint32_t i = 0; i < volNum_; i++) {
        used_num += volMap_[i]->GetTotalUsedSegs();
    }
    return used_num;
}
std::string WarmTier::GetDevicePath(uint32_t index) {
    return volMap_[index]->GetDevicePath();
}
uint32_t WarmTier::GetSegNum(uint32_t index) {
    return volMap_[index]->GetNumberOfSeg();
}
uint32_t WarmTier::GetCurSegId(uint32_t index) {
    return volMap_[index]->GetCurSegId();
}

uint64_t WarmTier::GetSSTLength() {
    uint64_t length = 0;
    for (uint32_t i = 0; i < volNum_; i++) {
        length += volMap_[i]->GetSSTLength();
    }
    return length;
}

void WarmTier::ModifyDeathEntry(HashEntry &entry) {
}

void WarmTier::deleteAllVolume() {
    map<int, Volume *>::iterator iter;
    for (iter = volMap_.begin(); iter != volMap_.end(); ) {
        Volume *vol = iter->second;
        delete vol;
        volMap_.erase(iter++);
    }

}


uint32_t WarmTier::calcSegNumForVolume(uint64_t capacity, uint32_t seg_size) {
    return capacity / seg_size;
}

}// namespace hlkvds
