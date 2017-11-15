#include "DataStor.h"
#include "Db_Structure.h"
#include "BlockDevice.h"
#include "IndexManager.h"
#include "Volumes.h"

using namespace std;

namespace hlkvds {

//DataStor* DataStor::Create() {
//    return new SimpleDS_Impl();
//}

SimpleDS_Impl::SimpleDS_Impl(Options& opts, vector<BlockDevice*> &dev_vec, SuperBlockManager* sb, IndexManager* idx) :
        options_(opts), bdVec_(dev_vec), sbMgr_(sb), idxMgr_(idx), seg_(NULL), maxValueLen_(0),
        reqWQ_(NULL), segWteWQ_(NULL), segTimeoutT_stop_(false) {
}

SimpleDS_Impl::~SimpleDS_Impl() {
    delete seg_;
    deleteAllVolumes();
}

void SimpleDS_Impl::createAllVolumes(uint64_t sst_offset, uint32_t seg_num, uint32_t segment_size, uint32_t cur_seg_id) {
    BlockDevice * bdev = bdVec_[0];
    uint64_t start_off = sst_offset + ComputeTotalSSTsSizeOnDisk(seg_num);
    Volumes *vol = new Volumes(bdev, sbMgr_, idxMgr_, options_, start_off, segment_size, seg_num, cur_seg_id);
    volMap_.insert( pair<int, Volumes*>(0, vol) );
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
    reqWQ_->Add_task(req);
    req->Wait();
    Status s = updateMeta(req);
    delete req;
    return s;
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
    //TODO: use GcSegment first, need abstract segment.
    while (!volMap_[0]->Alloc(seg_id)) {
        ret = volMap_[0]->ForeGC();
        if (!ret) {
            __ERROR("Cann't get a new Empty Segment.\n");
            return Status::Aborted("Can't allocate a Empty Segment.");
        }
    }

    SegForSlice *seg = new SegForSlice(volMap_[0], idxMgr_);
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
        volMap_[0]->FreeForFailed(seg_id);
        delete seg;
        return Status::IOError("could not write batch segment to device ");
    }

    uint32_t free_size = seg->GetFreeSize();
    volMap_[0]->Use(seg_id, free_size);
    seg->UpdateToIndex();
    delete seg;
    return Status::OK();
}

Status SimpleDS_Impl::ReadData(KVSlice &slice, string &data) {
    HashEntry *entry;
    entry = &slice.GetHashEntry();

    uint64_t data_offset = 0;
    if (!volMap_[0]->ComputeDataOffsetPhyFromEntry(entry, data_offset)) {
        return Status::Aborted("Compute data offset failed.");
    }

    uint16_t data_len = entry->GetDataSize();

    if (data_len == 0) {
        //The key is not exist
        return Status::NotFound("Key is not found.");
    }

    char *mdata = new char[data_len];
    if (!volMap_[0]->Read(mdata, data_len, data_offset)) {
        __ERROR("Could not read data at position");
        delete[] mdata;
        return Status::IOError("Could not read data at position.");
    }
    data.assign(mdata, data_len);
    delete[] mdata;

    __DEBUG("get key: %s, data offset %ld, head_offset is %ld", slice.GetKeyStr().c_str(), data_offset, entry->GetHeaderOffset());

    return Status::OK();
}

bool SimpleDS_Impl::UpdateSST() {
    return true;
}

bool SimpleDS_Impl::GetAllSSTs(char* buf, uint64_t length) {
    return volMap_[0]->GetSST(buf, length);
}

bool SimpleDS_Impl::SetAllSSTs(char* buf, uint64_t length) {
    return volMap_[0]->SetSST(buf, length);
}

void SimpleDS_Impl::InitMeta(uint64_t sst_offset, uint32_t segment_size, uint32_t number_segments, uint32_t cur_seg_id) {
    createAllVolumes(sst_offset, number_segments, segment_size, cur_seg_id);

    maxValueLen_ = segment_size - Volumes::SizeOfSegOnDisk() - IndexManager::SizeOfHashEntryOnDisk();

}

void SimpleDS_Impl::UpdateMetaToSB() {
    volMap_[0]->UpdateMetaToSB();
}


void SimpleDS_Impl::InitSegment() {
    seg_ = new SegForReq(volMap_[0], idxMgr_, options_.expired_time);
}

void SimpleDS_Impl::StartThds() {
    reqWQ_ = new ReqsMergeWQ(this, 1);
    reqWQ_->Start();

    segWteWQ_ = new SegmentWriteWQ(this, options_.seg_write_thread);
    segWteWQ_->Start();

    segTimeoutT_stop_.store(false);
    segTimeoutT_ = std::thread(&SimpleDS_Impl::SegTimeoutThdEntry, this);

    volMap_[0]->StartThds();
}

void SimpleDS_Impl::StopThds() {
    volMap_[0]->StopThds();

    segTimeoutT_stop_.store(true);
    segTimeoutT_.join();

    if (reqWQ_) {
        reqWQ_->Stop();
        delete reqWQ_;
    }

    if (segWteWQ_) {
        segWteWQ_->Stop();
        delete segWteWQ_;
    }
}

string SimpleDS_Impl::GetKeyByHashEntry(HashEntry *entry) {
    uint64_t key_offset = 0;
    if (!volMap_[0]->ComputeKeyOffsetPhyFromEntry(entry, key_offset)) {
        return "";
    }
    __DEBUG("key offset: %lu",key_offset);
    uint16_t key_len = entry->GetKeySize();
    char *mkey = new char[key_len+1];
    if (!volMap_[0]->Read(mkey, key_len, key_offset)) {
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
    if (!volMap_[0]->ComputeDataOffsetPhyFromEntry(entry, data_offset)) {
        return "";
    }
    __DEBUG("data offset: %lu",data_offset);
    uint16_t data_len = entry->GetDataSize();
    if ( data_len ==0 ) {
        return "";
    }
    char *mdata = new char[data_len+1];
    if (!volMap_[0]->Read(mdata, data_len, data_offset)) {
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

////////////////////////////////////////////////////

void SimpleDS_Impl::ModifyDeathEntry(HashEntry &entry) {
    volMap_[0]->ModifyDeathEntry(entry);
}

uint32_t SimpleDS_Impl::ComputeTotalSegNum(uint32_t seg_size, uint64_t meta_start_off) {
    int seg_num = 0;
    uint64_t dev_capacity = 0;
    uint64_t vol_capacity = 0;
    vector<BlockDevice *>::iterator iter = bdVec_.begin();
    BlockDevice *bdev = *iter;
    // compute meta device segment number
    dev_capacity = bdev->GetDeviceCapacity();
    vol_capacity =dev_capacity - meta_start_off;
    seg_num += Volumes::ComputeSegNum(vol_capacity, seg_size);
    iter++;
    __DEBUG("Compute seg_num is: %d", seg_num);

    // compute volume device segment number
    while ( iter!= bdVec_.end() ) {
        bdev = *iter;
        dev_capacity = bdev->GetDeviceCapacity();
        seg_num += Volumes::ComputeSegNum(dev_capacity, seg_size);
        iter++;
        __DEBUG("Compute seg_num is: %d", seg_num);
    }

    return seg_num;
}

uint64_t SimpleDS_Impl::ComputeTotalSSTsSizeOnDisk(uint32_t seg_num) {
    return Volumes::ComputeSegTableSizeOnDisk(seg_num);
}

uint64_t SimpleDS_Impl::GetDataRegionSize() {
    return volMap_[0]->GetDataRegionSize();
}


uint32_t SimpleDS_Impl::GetTotalFreeSegs() {
    return volMap_[0]->GetTotalFreeSegs();
}

/////////////////////////////////////////////////////

void SimpleDS_Impl::ReqMerge(Request* req) {
    std::unique_lock < std::mutex > lck_seg(segMtx_);
    if (seg_->TryPut(req)) {
        seg_->Put(req);
    } else {
        seg_->Complete();
        segWteWQ_->Add_task(seg_);
        seg_ = new SegForReq(volMap_[0], idxMgr_, options_.expired_time);
        seg_->Put(req);
    }
}

void SimpleDS_Impl::SegWrite(SegForReq *seg) {
    uint32_t seg_id = 0;
    bool res;
    while (!volMap_[0]->Alloc(seg_id)) {
        res = volMap_[0]->ForeGC();
        if (!res) {
            __ERROR("Cann't get a new Empty Segment.\n");
            seg->Notify(res);
        }
    }
    uint32_t free_size = seg->GetFreeSize();
    seg->SetSegId(seg_id);
    res = seg->WriteSegToDevice();
    if (res) {
        volMap_[0]->Use(seg_id, free_size);
    } else {
        volMap_[0]->FreeForFailed(seg_id);
    }
    seg->Notify(res);
}

void SimpleDS_Impl::SegTimeoutThdEntry() {
    __DEBUG("Segment Timeout thread start!!");
    std::unique_lock < std::mutex > lck(segMtx_, std::defer_lock);
    while (!segTimeoutT_stop_) {
        lck.lock();
        if (seg_->IsExpired()) {
            seg_->Complete();

            segWteWQ_->Add_task(seg_);
            seg_ = new SegForReq(volMap_[0], idxMgr_, options_.expired_time);
        }
        lck.unlock();

        usleep(options_.expired_time);
    } __DEBUG("Segment Timeout thread stop!!");
}

void SimpleDS_Impl::Do_GC() {
    __INFO("Application call GC!!!!!");
    return volMap_[0]->FullGC();
}

} // namespace hlkvds
