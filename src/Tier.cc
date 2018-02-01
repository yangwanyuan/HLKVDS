#include <math.h>
#include "Tier.h"
#include "Db_Structure.h"
#include "BlockDevice.h"
#include "SuperBlockManager.h"
#include "IndexManager.h"
#include "Volume.h"
#include "SegmentManager.h"
#include "DS_MultiTier_Impl.h"
#include "Migrate.h"

using namespace std;

namespace hlkvds {

Tier::Tier() {
}

Tier::~Tier() {
}

FastTier::FastTier(Options& opts, SuperBlockManager* sb, IndexManager* idx, MediumTier* mt) :
        options_(opts), sbMgr_(sb), idxMgr_(idx), maxValueLen_(0),
        segSize_(0), segNum_(0), vol_(NULL), mig_(NULL), mt_(mt),
        segWteWQ_(NULL) {
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

    //Create Migrate;
    mig_ = new Migrate(idxMgr_, this, mt_, options_);
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

    //vol_->StartThds();

    migrationT_stop_.store(false);
    migrationT_ = std::thread(&FastTier::MigrationThdEntry, this);
}

void FastTier::StopThds() {
    //vol_->StopThds();

    migrationT_stop_.store(true);
    migrationT_.join();

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

    if (options_.aggregate_request == 1) {
        return FastTier::writeDataAggregate(slice);
    }
    else {
        return FastTier::writeDataImmediately(slice);
    }

}

Status FastTier::WriteBatchData(WriteBatch *batch) {
    uint32_t seg_id = 0;
    bool ret = false;

    ret = allocSegment(seg_id);
    if (!ret) {
        __ERROR("Cann't get a new Empty Segment.\n");
        return Status::Aborted("Can't allocate a Empty Segment.");
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
    //vol_->FullGC();
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

bool FastTier::GetSST(char* buff, uint64_t length) {
    return vol_->GetSST(buff, length);
}

bool FastTier::SetSST(char* buff, uint64_t length) {
    return vol_->SetSST(buff, length);
}

bool FastTier::CreateVolume(std::vector<BlockDevice*> &bd_vec, uint64_t sst_offset, uint32_t medium_tier_total_seg_num) {
    segSize_ = options_.segment_size;

    uint32_t align_bit = log2(ALIGNED_SIZE);
    if (segSize_ != (segSize_ >> align_bit) << align_bit) {
        __ERROR("FastTier Segment Size is not page aligned!");
        return false;
    }

    BlockDevice *bdev = bd_vec[0];
    uint64_t device_capacity = bdev->GetDeviceCapacity();
    uint32_t seg_num = DS_MultiTier_Impl::CalcSegNumForFastTierVolume(device_capacity, sst_offset, segSize_, medium_tier_total_seg_num);
    segNum_ = seg_num;
    maxValueLen_ = segSize_ - SegBase::SizeOfSegOnDisk() - IndexManager::SizeOfHashEntryOnDisk();

    uint32_t seg_total_num = segNum_ + medium_tier_total_seg_num;
    sstLengthOnDisk_ = DS_MultiTier_Impl::CalcSSTsLengthOnDiskBySegNum(seg_total_num);

    uint64_t start_off = sst_offset + sstLengthOnDisk_;
    vol_ = new Volume(bdev, idxMgr_, options_, hlkvds::FastTierVolId, start_off, segSize_, segNum_, 0);

    initSBReservedContentForCreate();

    return true;
}

bool FastTier::OpenVolume(std::vector<BlockDevice*> &bd_vec) {
    segSize_ = sbResFastTier_.segment_size;
    segNum_ = sbResFastTier_.segment_num;
    maxValueLen_ = segSize_ - SegBase::SizeOfSegOnDisk() - IndexManager::SizeOfHashEntryOnDisk();
    sstLengthOnDisk_ = sbMgr_->GetSSTRegionLength();

    if (!verifyTopology(bd_vec)) {
        __ERROR("TheDevice Topology is inconsistent");
        return false;
    }

    BlockDevice *bdev = bd_vec[0];
    uint32_t cur_id = sbResFastTier_.cur_seg_id;
    uint64_t start_off = sbMgr_->GetSSTRegionOffset() + sbMgr_->GetSSTRegionLength();
    vol_ = new Volume(bdev, idxMgr_, options_, hlkvds::FastTierVolId, start_off, segSize_, segNum_, cur_id);
    return true;
}

uint32_t FastTier::GetTotalFreeSegs() {
    return vol_->GetTotalFreeSegs();
}

uint32_t FastTier::GetTotalUsedSegs() {
    return vol_->GetTotalUsedSegs();
}

uint64_t FastTier::GetSSTLength() {
    return vol_->GetSSTLength();
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

Status FastTier::writeDataAggregate(KVSlice& slice) {

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

Status FastTier::writeDataImmediately(KVSlice& slice){
    uint32_t seg_id = 0;
    bool ret = false;

    ret = allocSegment(seg_id);
    if (!ret) {
        __ERROR("Cann't get a new Empty Segment.\n");
        return Status::Aborted("Can't allocate a Empty Segment.");
    }

    SegLatencyFriendly *seg = new SegLatencyFriendly(vol_, idxMgr_);
    seg->Put(&slice);
    seg->SegSegId(seg_id);
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
    delete mig_;

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

void FastTier::initSBReservedContentForCreate() {
    sbResFastTier_.segment_size = segSize_;
    string dev_path  = vol_->GetDevicePath();
    memcpy((void*)&sbResFastTier_.dev_path, (const void*)dev_path.c_str(), dev_path.size());
    sbResFastTier_.segment_num = segNum_;
    sbResFastTier_.cur_seg_id = vol_->GetCurSegId();
}

bool FastTier::verifyTopology(std::vector<BlockDevice*> &bd_vec) {
    string record_path = string(sbResFastTier_.dev_path);
    string device_path = bd_vec[0]->GetDevicePath();
    if (record_path != device_path) {
        __ERROR("record_path: %s, device_path:%s ", record_path.c_str(), device_path.c_str());
        return false;
    }
    return true;
}

int FastTier::calcShardId(KVSlice& slice) {
    return KeyDigestHandle::Hash(&slice.GetDigest()) % shardsNum_;
}

bool FastTier::allocSegment(uint32_t &seg_id) {
    std::lock_guard <std::mutex> l(allocMtx_);

    uint32_t mt_vol_num = mt_->GetVolumeNum();
    while (!vol_->Alloc(seg_id)) {
        __DEBUG("cant't alloc segment, need migrate, free = %d, used = %d", vol_->GetTotalFreeSegs(), vol_->GetTotalUsedSegs());
        if (mt_vol_num == 0) {
            return false;
        }
        uint32_t mt_vol_id = mt_->PickVolForMigrate();
        mt_vol_num--;
        uint32_t free_num = mig_->ForeMigrate(mt_vol_id);
        __DEBUG("After migration total free %d segments on Medium Tier Volume [%d].", free_num, mt_vol_id);
    }

    return true;
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
    uint32_t seg_id = 0;
    bool ret = false;

    ret = allocSegment(seg_id);
    if (!ret) {
        __ERROR("Cann't get a new Empty Segment.\n");
        seg->Notify(ret);
        return ;
    }

    uint32_t free_size = seg->GetFreeSize();
    seg->SetSegId(seg_id);
    ret = seg->WriteSegToDevice();
    if (ret) {
        vol_->Use(seg_id, free_size);
    } else {
        vol_->FreeForFailed(seg_id);
    }
    seg->Notify(ret);
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

void FastTier::MigrationThdEntry() {
    __DEBUG("Migration thread start!!");
    while (!migrationT_stop_) {
        uint32_t free_seg_num = vol_->GetTotalFreeSegs();
        uint32_t total_seg_num = vol_->GetNumberOfSeg();
        uint32_t used_seg_num = total_seg_num - free_seg_num;

        double util_rate = (double)used_seg_num / (double)total_seg_num;

        //if ( util_rate > 0.3) {
        //    uint32_t mt_vol_id = mt_->PickVolForMigrate();
        //    uint32_t free_num = mig_->BackMigrate(mt_vol_id);
        //    __DEBUG("Once migration done, free %d segments", free_num);

        //    int sleep_time;
        //    if (util_rate > 0.8) {
        //        sleep_time = 10;
        //    }
        //    else if (util_rate > 0.5) {
        //        sleep_time = 1000;
        //    }
        //    else{
        //        sleep_time = 100000;
        //    }
        //    usleep(sleep_time);
        //}
        if (util_rate > 0.1) {
            uint32_t mt_vol_id = mt_->PickVolForMigrate();
            uint32_t free_num = mig_->BackMigrate(mt_vol_id);
            __DEBUG("Once migration done, free %d segments", free_num);
        }
        usleep(1000);
    }__DEBUG("Migration thread stop!!");
}

MediumTier::MediumTier(Options& opts, SuperBlockManager* sb, IndexManager* idx) :
        options_(opts), sbMgr_(sb), idxMgr_(idx),
        segSize_(0), segTotalNum_(0), volNum_(0), pickVolId_(-1) {
}

MediumTier::~MediumTier() {
    deleteAllVolume();
}

void MediumTier::StartThds() {
    for (uint32_t i = 0; i < volNum_; i++) {
        volMap_[i]->StartThds();
    }
}

void MediumTier::StopThds() {
    for (uint32_t i = 0; i < volNum_; i++) {
        volMap_[i]->StopThds();
    }
}

void MediumTier::printDeviceTopologyInfo() {
    __INFO( "\n\t Medium Tier Infomation:\n"
            "\t Medium Tier Seg Size          : %d\n"
            "\t Medium Tier Vol Num           : %d",
            sbResMediumTierHeader_.segment_size, sbResMediumTierHeader_.volume_num);

    for (uint32_t i = 0 ; i < volNum_; i++) {
        __INFO("\n\t Medium Tier Volume[%d] : dev_path = %s, segment_num = %d, cur_seg_id = %d",
                i, sbResMediumTierVolVec_[i].dev_path,
                sbResMediumTierVolVec_[i].segment_num,
                sbResMediumTierVolVec_[i].cur_seg_id);
    }
}

void MediumTier::printDynamicInfo() {
}

Status MediumTier::ReadData(KVSlice &slice, std::string &data) {
    HashEntry *entry;
    entry = &slice.GetHashEntry();

    int vol_id = getVolIdFromEntry(entry);
    Volume *vol = volMap_[vol_id];

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

void MediumTier::ManualGC() {
    map<int, Volume *>::iterator iter;
    for (iter = volMap_.begin(); iter != volMap_.end(); ) {
        Volume *vol = iter->second;
        vol->FullGC();
    }
}

bool MediumTier::GetSBReservedContent(char* buf, uint64_t length) {
    uint32_t except_length = sizeof(MultiTierDS_SB_Reserved_MediumTier_Header) +
                            volNum_ * sizeof(MultiTierDS_SB_Reserved_MediumTier_Content);
    if (length != except_length) {
        return false;
    }

    updateAllVolSBRes();

    char* buf_ptr = buf;
    uint64_t medium_tier_header_size = sizeof(MultiTierDS_SB_Reserved_MediumTier_Header);
    memcpy((void*)buf, (const void *)&sbResMediumTierHeader_, medium_tier_header_size);
    __DEBUG("MultiTierDS_SB_Reserved_MediumTier_Header: segment_size = %d, volume_num = %d",
            sbResMediumTierHeader_.segment_size, sbResMediumTierHeader_.volume_num);
    buf_ptr += medium_tier_header_size;

    //Get sbResVolVec_
    uint64_t sb_res_item_size = sizeof(MultiTierDS_SB_Reserved_MediumTier_Content);
    for (uint32_t i = 0; i < volNum_; i++) {
        memcpy((void*)buf_ptr, (const void*)&sbResMediumTierVolVec_[i], sb_res_item_size);
        buf_ptr += sb_res_item_size;
        __DEBUG("MultiTierDS_SB_Reserved_MediumTier_Content[%d]: device_path = %s, segment_num = %d, current_seg_id = %d",
            i, sbResMediumTierVolVec_[i].dev_path, sbResMediumTierVolVec_[i].segment_num,
            sbResMediumTierVolVec_[i].cur_seg_id);

    }
    return true;
}

bool MediumTier::SetSBReservedContent(char* buf, uint64_t length) {
    char* buf_ptr = buf;
    uint64_t medium_tier_header_size = sizeof(MultiTierDS_SB_Reserved_MediumTier_Header);
    memcpy((void*)&sbResMediumTierHeader_, (const void *)buf_ptr, medium_tier_header_size);
    __DEBUG("MultiTierDS_SB_Reserved_MediumTier_Header: segment_size = %d, volume_num = %d",
            sbResMediumTierHeader_.segment_size, sbResMediumTierHeader_.volume_num);
    buf_ptr += medium_tier_header_size;

    uint64_t except_length = sizeof(MultiTierDS_SB_Reserved_MediumTier_Header) +
                            sbResMediumTierHeader_.volume_num * sizeof(MultiTierDS_SB_Reserved_MediumTier_Content);
    if (length != except_length) {
        __INFO("!!!!!length = %ld, except_length = %ld, volNum_ = %d", length, except_length, volNum_);
        return false;
    }

    //Get sbResVolVec_
    uint64_t sb_res_item_size = sizeof(MultiTierDS_SB_Reserved_MediumTier_Content);
    for (uint32_t i = 0; i < sbResMediumTierHeader_.volume_num; i++) {
        MultiTierDS_SB_Reserved_MediumTier_Content sb_res_item;
        memcpy((void*)&sb_res_item, (const void*)buf_ptr, sb_res_item_size);
        sbResMediumTierVolVec_.push_back(sb_res_item);
        buf_ptr += sb_res_item_size;
        __DEBUG("MultiTierDS_SB_Reserved_MediumTier_Content[%d]: device_path = %s, segment_num = %d, current_seg_id = %d",
            i, sbResMediumTierVolVec_[i].dev_path, sbResMediumTierVolVec_[i].segment_num,
            sbResMediumTierVolVec_[i].cur_seg_id);
    }
    return true;
}

bool MediumTier::GetSST(char* buf, uint64_t length) {
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

bool MediumTier::SetSST(char* buf, uint64_t length) {
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

bool MediumTier::CreateVolume(std::vector<BlockDevice*> &bd_vec) {

    segSize_ = options_.secondary_seg_size;
    uint32_t align_bit = log2(ALIGNED_SIZE);
    if (segSize_ != (segSize_ >> align_bit) << align_bit) {
        __ERROR("Medium Tier Segment Size is not page aligned!");
        return false;
    }

    int fast_tier_device_num = hlkvds::FastTierVolNum;
    volNum_ = bd_vec.size() - fast_tier_device_num;

    for (uint32_t i = 0; i < volNum_; i++ ) {
        BlockDevice *bdev = bd_vec[i + fast_tier_device_num];
        uint64_t device_capacity = bdev->GetDeviceCapacity();
        uint32_t seg_num = DS_MultiTier_Impl::CalcSegNumForMediumTierVolume(device_capacity, segSize_);
        Volume *vol = new Volume(bdev, idxMgr_, options_, i, 0, segSize_, seg_num, 0);
        volMap_.insert( pair<int, Volume*>(i, vol) );
        segTotalNum_ += seg_num;
    }

    initSBReservedContentForCreate();
    return true;
}

bool MediumTier::OpenVolume(std::vector<BlockDevice*> &bd_vec) {
    int fast_tier_device_num = hlkvds::FastTierVolNum;

    segSize_ = sbResMediumTierHeader_.segment_size;
    volNum_ = sbResMediumTierHeader_.volume_num;

    if (!verifyTopology(bd_vec)) {
        __ERROR("TheDevice Topology is inconsistent");
        return false;
    }

    for (uint32_t i = 0; i < volNum_; i++) {
        BlockDevice *bdev = bd_vec[i + fast_tier_device_num];
        uint32_t seg_num = sbResMediumTierVolVec_[i].segment_num;
        uint32_t cur_seg_id = sbResMediumTierVolVec_[i].cur_seg_id;
        Volume *vol = new Volume(bdev, idxMgr_, options_, i, 0, segSize_, seg_num, cur_seg_id);
        volMap_.insert( pair<int, Volume*>(i, vol) );
        segTotalNum_ += seg_num;
    }
    return true;
}

uint32_t MediumTier::GetTotalFreeSegs() {
    uint32_t free_num = 0;
    for (uint32_t i = 0; i < volNum_; i++) {
        free_num += volMap_[i]->GetTotalFreeSegs();
    }
    return free_num;
}

uint32_t MediumTier::GetTotalUsedSegs() {
    uint32_t used_num = 0;
    for (uint32_t i = 0; i < volNum_; i++) {
        used_num += volMap_[i]->GetTotalUsedSegs();
    }
    return used_num;
}

uint64_t MediumTier::GetSSTLength() {
    uint64_t length = 0;
    for (uint32_t i = 0; i < volNum_; i++) {
        length += volMap_[i]->GetSSTLength();
    }
    return length;
}

void MediumTier::ModifyDeathEntry(HashEntry &entry) {
    int vol_id = getVolIdFromEntry(&entry);
    volMap_[vol_id]->ModifyDeathEntry(entry);
}

std::string MediumTier::GetKeyByHashEntry(HashEntry *entry){
    uint64_t key_offset = 0;

    int vol_id = getVolIdFromEntry(entry);
    Volume *vol = volMap_[vol_id];

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

std::string MediumTier::GetValueByHashEntry(HashEntry *entry) {
    uint64_t data_offset = 0;

    int vol_id = getVolIdFromEntry(entry);
    Volume *vol = volMap_[vol_id];

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

Volume* MediumTier::GetVolume(uint32_t vol_id) {
    Volume *vol = volMap_[vol_id];
    return vol;
}

uint32_t MediumTier::PickVolForMigrate() {
    std::lock_guard <std::mutex> l(volIdMtx_);
    if ( pickVolId_ == (int)(volNum_ - 1) ){
        pickVolId_ = 0;
    }
    else {
        pickVolId_++;
    }
    return pickVolId_;
}

int MediumTier::getVolIdFromEntry(HashEntry* entry) {
    uint16_t pos = entry->GetHeaderLocation();
    int vol_id = (int)pos;
    return vol_id;
}

void MediumTier::deleteAllVolume() {
    map<int, Volume *>::iterator iter;
    for (iter = volMap_.begin(); iter != volMap_.end(); ) {
        Volume *vol = iter->second;
        delete vol;
        volMap_.erase(iter++);
    }
}

void MediumTier::initSBReservedContentForCreate() {
    sbResMediumTierHeader_.segment_size = segSize_;
    sbResMediumTierHeader_.volume_num = volNum_;
    for (uint32_t i = 0; i < volNum_; i++) {
        MultiTierDS_SB_Reserved_MediumTier_Content sb_res_item;
        string dev_path = volMap_[i]->GetDevicePath();
        memcpy((void*)&sb_res_item.dev_path, (const void*)dev_path.c_str(), dev_path.size());
        sb_res_item.segment_num = volMap_[i]->GetNumberOfSeg();
        sb_res_item.cur_seg_id = volMap_[i]->GetCurSegId();
        sbResMediumTierVolVec_.push_back(sb_res_item);
    }
}

void MediumTier::updateAllVolSBRes() {
    for (uint32_t i = 0; i < volNum_; i++) {
        uint32_t cur_id = volMap_[i]->GetCurSegId();
        sbResMediumTierVolVec_[i].cur_seg_id = cur_id;
    }
}

bool MediumTier::verifyTopology(std::vector<BlockDevice*> &bd_vec) {
    int fast_tier_device_num = hlkvds::FastTierVolNum;

    // Verify MediumTier Volume Number
    if (volNum_ != bd_vec.size() - fast_tier_device_num) {
        __ERROR("record MediumTierVolNum_ = %d, total block device number: %d", volNum_, (int)bd_vec.size() );
        return false;
    }

    // Verify MediumTier every Volume
    for (uint32_t i = 0; i < volNum_; i++) {
        string record_path = string(sbResMediumTierVolVec_[i].dev_path);
        string device_path = bd_vec[i + fast_tier_device_num]->GetDevicePath();
        if (record_path != device_path) {
            __ERROR("record_path: %s, device_path:%s ", record_path.c_str(), device_path.c_str());
            return false;
        }
    }
    return true;
}

}// namespace hlkvds
