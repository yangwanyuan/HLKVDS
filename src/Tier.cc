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

bool FastTier::GetSST(char* buff, uint64_t length) {
    return vol_->GetSST(buff, length);
}

bool FastTier::SetSST(char* buff, uint64_t length) {
    return vol_->SetSST(buff, length);
}

bool FastTier::CreateVolume(BlockDevice *bdev, uint64_t start_offset, uint32_t seg_size, uint32_t seg_num, uint32_t cur_id) {
    bdev_ = bdev;
    segSize_ = seg_size;
    segNum_ = seg_num;
    maxValueLen_ = segSize_ - Volume::SizeOfSegOnDisk() - IndexManager::SizeOfHashEntryOnDisk();

    vol_ = new Volume(bdev_, idxMgr_, options_, 100, start_offset, segSize_, segNum_, cur_id);
    return true;
}

bool FastTier::OpenVolume(BlockDevice *bdev, uint64_t start_offset, uint32_t seg_size, uint32_t seg_num, uint32_t cur_id) {
    bdev_ = bdev;
    segSize_ = seg_size;
    segNum_ = seg_num;
    maxValueLen_ = segSize_ - Volume::SizeOfSegOnDisk() - IndexManager::SizeOfHashEntryOnDisk();
    vol_ = new Volume(bdev_, idxMgr_, options_, 100, start_offset, segSize_, segNum_, cur_id);
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
}

int FastTier::calcShardId(KVSlice& slice) {
    return KeyDigestHandle::Hash(&slice.GetDigest()) % shardsNum_;
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

//WarmTier::WarmTier() {
//}
//
//WarmTier::~WarmTier() {
//    deleteAllVolumes();
//}
//
//void WarmTier::CreateAllSegments() {
//}
//
//void WarmTier::StartThds() {
//}
//
//void WarmTier::StopThds() {
//}
//
//bool WarmTier::GetSSTs(char* buf, uint64_t length) {
//}
//
//bool WarmTier::SetSSTs(char* buf, uint64_t length) {
//}
//
//bool WarmTier::CreateVolumes() {
//}
//
//bool WarmTier::OpenVolumes() {
//}
//
//void WarmTier::ModifyDeathEntry(HashEntry &entry) {
//}
//
//void WarmTier::deleteAllVolumes() {
//}

}// namespace hlkvds
