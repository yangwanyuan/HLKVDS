#include <math.h>
#include "DataStor.h"
#include "Db_Structure.h"
#include "BlockDevice.h"
#include "SuperBlockManager.h"
#include "IndexManager.h"
#include "Volumes.h"
#include "SegmentManager.h"

using namespace std;

namespace hlkvds {

//DataStor* DataStor::Create() {
//    return new SimpleDS_Impl();
//}

SimpleDS_Impl::SimpleDS_Impl(Options& opts, vector<BlockDevice*> &dev_vec, SuperBlockManager* sb, IndexManager* idx) :
        options_(opts), bdVec_(dev_vec), sbMgr_(sb), idxMgr_(idx), segSize_(0), maxValueLen_(0),
        volNum_(0), segTotalNum_(0), sstLengthOnDisk_(0), pickVolId_(-1), segWteWQ_(NULL),
        segTimeoutT_stop_(false) {
    shardsNum_ = options_.shards_num;
    lastTime_ = new KVTime();
}

SimpleDS_Impl::~SimpleDS_Impl() {
    deleteAllSegments();
    deleteAllVolumes();
    delete lastTime_;
}

void SimpleDS_Impl::printDeviceTopologyInfo() {
    __INFO("\nDB Device Topology information:\n"
            "\t Segment size                : %d\n"
            "\t Volumes number              : %d",
            sbResHeader_.segment_size, sbResHeader_.volume_num);
    for (uint32_t i = 0 ; i < sbResHeader_.volume_num; i++) {
        __INFO("\nVolume[%d] : dev_path = %s, segment_num = %d, cur_seg_id = %d",
                i, sbResVolVec_[i].dev_path,
                sbResVolVec_[i].segment_num,
                sbResVolVec_[i].cur_seg_id);
    }
}

void SimpleDS_Impl::printDynamicInfo() {
    __INFO("\n SimpleDS_Impl Dynamic information: \n"
            "\t Request Queue Size          : %d\n"
            "\t Segment Write Queue Size    : %d\n",
            GetReqQueSize(), GetSegWriteQueSize());
}

void SimpleDS_Impl::deleteAllSegments() {
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

void SimpleDS_Impl::deleteAllVolumes() {
    map<int, Volumes *>::iterator iter;
    for (iter = volMap_.begin(); iter != volMap_.end(); ) {
        Volumes *vol = iter->second;
        delete vol;
        volMap_.erase(iter++);
    }
}

Status SimpleDS_Impl::WriteData(KVSlice& slice) {
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

int SimpleDS_Impl::calcShardId(KVSlice& slice) {
    return KeyDigestHandle::Hash(&slice.GetDigest()) % shardsNum_;
}

Status SimpleDS_Impl::updateMeta(Request *req) {
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

Status SimpleDS_Impl::WriteBatchData(WriteBatch *batch) {
    uint32_t seg_id = 0;
    bool ret;

    int vol_id = pickVol();
    Volumes *vol = volMap_[vol_id];

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

Status SimpleDS_Impl::ReadData(KVSlice &slice, string &data) {
    HashEntry *entry;
    entry = &slice.GetHashEntry();

    int vol_id = getVolIdFromEntry(entry);
    Volumes *vol = volMap_[vol_id];

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

bool SimpleDS_Impl::GetAllSSTs(char* buf, uint64_t length) {
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

    //Copy SST
    for (uint32_t i = 0; i < volNum_; i++) {
        uint64_t vol_sst_length = volMap_[i]->GetSSTLength();
        if ( !volMap_[i]->GetSST(buf_ptr, vol_sst_length) ) {
            return false;
        }
        buf_ptr += vol_sst_length;
    }
    return true;
}

bool SimpleDS_Impl::SetAllSSTs(char* buf, uint64_t length) {
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

    //Set SST
    for (uint32_t i = 0; i < volNum_; i++) {
        uint64_t vol_sst_length = volMap_[i]->GetSSTLength();
        if ( !volMap_[i]->SetSST(buf_ptr, vol_sst_length) ) {
            return false;
        }
        buf_ptr += vol_sst_length;
    }
    return true;
}

void SimpleDS_Impl::initSBReservedContentForCreate() {
    sbResHeader_.segment_size = segSize_;
    sbResHeader_.volume_num = volNum_;

    for(uint32_t i = 0; i < volNum_; i++)
    {
        SimpleDS_SB_Reserved_Volume sb_res_vol;

        string dev_path = volMap_[i]->GetDevicePath();
        memcpy((void*)&sb_res_vol.dev_path, (const void*)dev_path.c_str(), dev_path.size());

        sb_res_vol.segment_num = volMap_[i]->GetNumberOfSeg();
        sb_res_vol.cur_seg_id = volMap_[i]->GetCurSegId();

        sbResVolVec_.push_back(sb_res_vol);
    }
}

bool SimpleDS_Impl::SetSBReservedContent(char* buf, uint64_t length) {
    if (length != SuperBlockManager::ReservedRegionLength()) {
        return false;
    }

    //Set sbResHeader_
    uint64_t header_size = sizeof(SimpleDS_SB_Reserved_Header);
    memcpy((void*)&sbResHeader_, (const void*)buf, header_size);
    __DEBUG("SB_RESERVED_HEADER: volume_num= %d, segment_size = %d", sbResHeader_.volume_num, sbResHeader_.segment_size);

    //Set sbResVolVec_
    uint64_t sb_res_vol_size = sizeof(SimpleDS_SB_Reserved_Volume);
    uint32_t vol_num = sbResHeader_.volume_num;
    char * sb_res_vol_ptr = buf + header_size;

    for (uint32_t i = 0; i < vol_num; i++) {
        SimpleDS_SB_Reserved_Volume sb_res_vol;
        memcpy((void*)&sb_res_vol, (const void*)sb_res_vol_ptr, sb_res_vol_size);
        sbResVolVec_.push_back(sb_res_vol);
        sb_res_vol_ptr += sb_res_vol_size;
        __DEBUG("Volume [%d], device_path= %s, segment_num = %d, current_seg_id = %d", i, sbResVolVec_[i].dev_path, sbResVolVec_[i].segment_num, sbResVolVec_[i].cur_seg_id);
    }

    return true;
}

bool SimpleDS_Impl::GetSBReservedContent(char* buf, uint64_t length) {
    if (length != SuperBlockManager::ReservedRegionLength()) {
        return false;
    }
    memset((void*)buf, 0, length);

    //Get sbResHeader_
    uint64_t header_size = sizeof(SimpleDS_SB_Reserved_Header);
    memcpy((void*)buf, (const void*)&sbResHeader_, header_size);
    __DEBUG("SB_RESERVED_HEADER: volume_num= %d, segment_size = %d", sbResHeader_.volume_num, sbResHeader_.segment_size);

    //Get sbResVolVec_
    uint64_t sb_res_vol_size = sizeof(SimpleDS_SB_Reserved_Volume);
    uint32_t vol_num = sbResHeader_.volume_num;
    char * sb_res_vol_ptr = buf + header_size;

    updateAllVolSBRes();

    for (uint32_t i = 0; i < vol_num; i++) {
        memcpy((void*)(sb_res_vol_ptr), (const void*)&sbResVolVec_[i], sb_res_vol_size);
        sb_res_vol_ptr += sb_res_vol_size;
        __DEBUG("Volume [i], device_path= %s, segment_num = %d, current_seg_id = %d", i, sbResVolVec_[i].dev_path, sbResVolVec_[i].segment_num, sbResVolVec_[i].cur_seg_id);
    }

    return true;
}

void SimpleDS_Impl::updateAllVolSBRes() {
    for (uint32_t i = 0; i< sbResHeader_.volume_num; i++) {
        uint32_t cur_id = volMap_[i]->GetCurSegId();
        sbResVolVec_[i].cur_seg_id = cur_id;
    }
}
void SimpleDS_Impl::CreateAllVolumes(uint64_t sst_offset, uint32_t segment_size) {
    segSize_ = segment_size;
    maxValueLen_ = segSize_ - Volumes::SizeOfSegOnDisk() - IndexManager::SizeOfHashEntryOnDisk();
    volNum_ = bdVec_.size();

    //Create Pure Volumes
    if (volNum_ > 1) {
        for (uint32_t i = 1; i < volNum_; i++ ) {
            BlockDevice * bdev = bdVec_[i];
            uint64_t device_capacity = bdev->GetDeviceCapacity();
            uint32_t seg_num = SimpleDS_Impl::CalcSegNumForPureVolume(device_capacity, segSize_);
            segTotalNum_ += seg_num;
            Volumes *vol = new Volumes(bdev, idxMgr_, options_, i, 0, segSize_, seg_num, 0);
            volMap_.insert( pair<int, Volumes *>(i, vol) );
        }
    }

    //Create Meta Volumes
    BlockDevice * bdev = bdVec_[0];
    uint64_t device_capacity = bdev->GetDeviceCapacity();
    uint32_t seg_num = SimpleDS_Impl::CalcSegNumForMetaVolume(device_capacity, sst_offset, segTotalNum_, segSize_);
    segTotalNum_ += seg_num;
    sstLengthOnDisk_ = SimpleDS_Impl::CalcSSTsLengthOnDiskBySegNum(segTotalNum_);
    uint64_t start_off = sst_offset + sstLengthOnDisk_;
    Volumes *vol = new Volumes(bdev, idxMgr_, options_, 0, start_off, segSize_, seg_num, 0);
    volMap_.insert( make_pair(0, vol) );

    initSBReservedContentForCreate();
}

bool SimpleDS_Impl::OpenAllVolumes() {
    volNum_ = sbResHeader_.volume_num;
    segSize_ = sbResHeader_.segment_size;
    maxValueLen_ = segSize_ - Volumes::SizeOfSegOnDisk() - IndexManager::SizeOfHashEntryOnDisk();
    sstLengthOnDisk_ = sbMgr_->GetSSTRegionLength();

    BlockDevice *meta_bdev = bdVec_[0];
    uint32_t meta_seg_num = sbResVolVec_[0].segment_num;
    uint32_t meta_cur_seg_id = sbResVolVec_[0].cur_seg_id;

    if (!verifyTopology()) {
        __ERROR("The Device Topoloygy is inconsistent");
        return false;
    }

    //Create Meta Volume
    uint64_t start_off = sbMgr_->GetSSTRegionOffset() + sstLengthOnDisk_;

    Volumes *meta_vol = new Volumes(meta_bdev, idxMgr_, options_, 0, start_off, segSize_, meta_seg_num, meta_cur_seg_id);
    volMap_.insert( pair<int, Volumes *>(0, meta_vol) );

    segTotalNum_ += meta_seg_num;

    //Create Pure Volumes
    if (volNum_ > 1) {
        for (uint32_t i = 1 ; i < volNum_; i++) {
            BlockDevice *bdev = bdVec_[i];
            uint32_t seg_num = sbResVolVec_[i].segment_num;
            uint32_t cur_seg_id = sbResVolVec_[i].cur_seg_id;
            Volumes *vol = new Volumes(bdev, idxMgr_, options_, i, 0, segSize_, seg_num, cur_seg_id);
            volMap_.insert( pair<int, Volumes *>(i, vol) );

            segTotalNum_ += seg_num;
        }
    }

    return true;
}

bool SimpleDS_Impl::verifyTopology() {
    if (volNum_ != bdVec_.size()) {
        return false;
    }
    for (uint32_t i = 0; i < volNum_; i++) {
        string record_path = string(sbResVolVec_[i].dev_path);
        string device_path = bdVec_[i]->GetDevicePath();
        if (record_path != device_path) {
            __ERROR("record_path: %s, device_path:%s ", record_path.c_str(), device_path.c_str());
            return false;
        }
    }
    return true;
}

int SimpleDS_Impl::pickVol() {
    std::lock_guard <std::mutex> l(volIdMtx_);
    if ( pickVolId_ == (int)(volNum_ - 1) ){
        pickVolId_ = 0;
    }
    else {
        pickVolId_++;
    }
    return pickVolId_;
}

int SimpleDS_Impl::getVolIdFromEntry(HashEntry* entry) {
    uint16_t pos = entry->GetHeaderLocation();
    int vol_id = (int)pos;
    return vol_id;
}

void SimpleDS_Impl::CreateAllSegments() {
    std::unique_lock<std::mutex> lck_seg_map(segMapMtx_);
    for (int i = 0; i < shardsNum_; i++) {
        int vol_id = pickVol();
        SegForReq * seg = new SegForReq(volMap_[vol_id], idxMgr_, options_.expired_time);
        segMap_.insert(make_pair(i, seg));

        std::mutex *seg_mtx = new mutex();
        segMtxVec_.push_back(seg_mtx);
    }
}

void SimpleDS_Impl::StartThds() {
    for (int i = 0; i < shardsNum_; i++) {
        ReqsMergeWQ *req_wq = new ReqsMergeWQ(this, 1);
        req_wq->Start();
        reqWQVec_.push_back(req_wq);
    }

    segWteWQ_ = new SegmentWriteWQ(this, options_.seg_write_thread);
    segWteWQ_->Start();

    segTimeoutT_stop_.store(false);
    segTimeoutT_ = std::thread(&SimpleDS_Impl::SegTimeoutThdEntry, this);

    for (uint32_t i = 0; i < volNum_; i++) {
        volMap_[i]->StartThds();
    }
}

void SimpleDS_Impl::StopThds() {
    for (uint32_t i = 0; i < volNum_; i++) {
        volMap_[i]->StopThds();
    }

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

string SimpleDS_Impl::GetKeyByHashEntry(HashEntry *entry) {
    uint64_t key_offset = 0;

    int vol_id = getVolIdFromEntry(entry);
    Volumes *vol = volMap_[vol_id];

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

string SimpleDS_Impl::GetValueByHashEntry(HashEntry *entry) {
    uint64_t data_offset = 0;

    int vol_id = getVolIdFromEntry(entry);
    Volumes *vol = volMap_[vol_id];

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

uint32_t SimpleDS_Impl::GetReqQueSize() {
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

uint32_t SimpleDS_Impl::GetSegWriteQueSize() {
    return (!segWteWQ_)? 0 : segWteWQ_->Size();
}

////////////////////////////////////////////////////

void SimpleDS_Impl::ModifyDeathEntry(HashEntry &entry) {
    int vol_id = getVolIdFromEntry(&entry);
    volMap_[vol_id]->ModifyDeathEntry(entry);
}

uint64_t SimpleDS_Impl::CalcSSTsLengthOnDiskBySegNum(uint32_t seg_num) {
    uint64_t sst_pure_length = sizeof(SegmentStat) * seg_num;
    uint64_t sst_length = sst_pure_length + sizeof(time_t);
    uint64_t sst_pages = sst_length / getpagesize();
    sst_length = (sst_pages + 1) * getpagesize();
    return sst_length;
}

uint32_t SimpleDS_Impl::CalcSegNumForPureVolume(uint64_t capacity, uint32_t seg_size) {
    return capacity / seg_size;
}

uint32_t SimpleDS_Impl::CalcSegNumForMetaVolume(uint64_t capacity, uint64_t sst_offset, uint32_t total_buddy_seg_num, uint32_t seg_size) {
    uint64_t valid_capacity = capacity - sst_offset;
    uint32_t seg_num_candidate = valid_capacity / seg_size;

    uint32_t seg_num_total = seg_num_candidate + total_buddy_seg_num;
    uint64_t sst_length = SimpleDS_Impl::CalcSSTsLengthOnDiskBySegNum( seg_num_total );

    uint32_t seg_size_bit = log2(seg_size);

    while( sst_length + ((uint64_t) seg_num_candidate << seg_size_bit) > valid_capacity) {
         seg_num_candidate--;
         seg_num_total = seg_num_candidate + total_buddy_seg_num;
         sst_length = SimpleDS_Impl::CalcSSTsLengthOnDiskBySegNum( seg_num_total );
    }
    return seg_num_candidate;
}

uint32_t SimpleDS_Impl::GetTotalFreeSegs() {
    uint32_t free_num = 0;
    for (uint32_t i = 0; i < volNum_; i++) {
        free_num += volMap_[i]->GetTotalFreeSegs();
    }
    return free_num;
}

/////////////////////////////////////////////////////

void SimpleDS_Impl::ReqMerge(Request* req) {
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

        int vol_id = pickVol();
        seg = new SegForReq(volMap_[vol_id], idxMgr_, options_.expired_time);

        lck_seg_map.lock();
        std::map<int, SegForReq*>::iterator iter = segMap_.find(shard_id);
        segMap_.erase(iter);
        segMap_.insert( make_pair(shard_id, seg) );
        lck_seg_map.unlock();

        seg->Put(req);
    }

}

void SimpleDS_Impl::SegWrite(SegForReq *seg) {
    Volumes *vol =  seg->GetSelfVolume();

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

void SimpleDS_Impl::SegTimeoutThdEntry() {
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
                int vol_id = pickVol();
                seg = new SegForReq(volMap_[vol_id], idxMgr_, options_.expired_time);

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

void SimpleDS_Impl::Do_GC() {
    __INFO("Application call GC!!!!!");
    for (uint32_t i = 0; i < volNum_; i++) {
        volMap_[i]->FullGC();
    }
}

} // namespace hlkvds
