#include "DataStor.h"
#include "Db_Structure.h"
#include "BlockDevice.h"
#include "IndexManager.h"
#include "GcManager.h"
#include "SegmentManager.h"

namespace hlkvds {

//DataStor* DataStor::Create() {
//    return new SimpleDS_Impl();
//}

SimpleDS_Impl::SimpleDS_Impl(Options& opts, BlockDevice* dev, SuperBlockManager* sb, IndexManager* idx) :
        options_(opts), bdev_(dev), sbMgr_(sb), idxMgr_(idx),
        gcMgr_(NULL), seg_(NULL),
        reqWQ_(NULL), segWteWQ_(NULL), segTimeoutT_stop_(false),
        gcT_stop_(false) {
    segMgr_ = new SegmentManager(bdev_, sbMgr_, options_);
    gcMgr_ = new GcManager(bdev_, idxMgr_, this, options_);
}

SimpleDS_Impl::~SimpleDS_Impl() {
    delete segMgr_;
    delete gcMgr_;
    delete seg_;
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
    while (!segMgr_->Alloc(seg_id)) {
        ret = gcMgr_->ForeGC();
        if (!ret) {
            __ERROR("Cann't get a new Empty Segment.\n");
            return Status::Aborted("Can't allocate a Empty Segment.");
        }
    }

    SegForSlice *seg = new SegForSlice(this, idxMgr_, bdev_);
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
        segMgr_->FreeForFailed(seg_id);
        delete seg;
        return Status::IOError("could not write batch segment to device ");
    }

    uint32_t free_size = seg->GetFreeSize();
    segMgr_->Use(seg_id, free_size);
    seg->UpdateToIndex();
    delete seg;
    return Status::OK();
}

Status SimpleDS_Impl::ReadData(KVSlice &slice, string &data) {
    HashEntry *entry;
    entry = &slice.GetHashEntry();

    uint64_t data_offset = 0;
    if (!segMgr_->ComputeDataOffsetPhyFromEntry(entry, data_offset)) {
        return Status::Aborted("Compute data offset failed.");
    }

    uint16_t data_len = entry->GetDataSize();

    if (data_len == 0) {
        //The key is not exist
        return Status::NotFound("Key is not found.");
    }

    char *mdata = new char[data_len];
    if (bdev_->pRead(mdata, data_len, data_offset) != (ssize_t) data_len) {
        __ERROR("Could not read data at position");
        delete[] mdata;
        return Status::IOError("Could not read data at position.");
    }
    data.assign(mdata, data_len);
    delete[] mdata;

    __DEBUG("get key: %s, data offset %ld, head_offset is %ld", slice.GetKeyStr().c_str(), data_offset, entry->GetHeaderOffsetPhy());

    return Status::OK();
}

bool SimpleDS_Impl::UpdateSST() {
    return true;
}

bool SimpleDS_Impl::GetAllSSTs() {
    return true;
}

bool SimpleDS_Impl::SetAllSSTs() {
    return true;
}


void SimpleDS_Impl::InitSegment() {
    seg_ = new SegForReq(this, idxMgr_, bdev_, options_.expired_time);
}

void SimpleDS_Impl::startThds() {
    reqWQ_ = new ReqsMergeWQ(this, 1);
    reqWQ_->Start();

    segWteWQ_ = new SegmentWriteWQ(this, options_.seg_write_thread);
    segWteWQ_->Start();

    segTimeoutT_stop_.store(false);
    segTimeoutT_ = std::thread(&SimpleDS_Impl::SegTimeoutThdEntry, this);

    idxMgr_->StartThds();

    gcT_stop_.store(false);
    gcT_ = std::thread(&SimpleDS_Impl::GCThdEntry, this);
}

void SimpleDS_Impl::stopThds() {
    gcT_stop_.store(true);
    gcT_.join();

    idxMgr_->StopThds();

    segTimeoutT_stop_.store(true);
    segTimeoutT_.join();

    reqWQ_->Stop();
    delete reqWQ_;

    segWteWQ_->Stop();
    delete segWteWQ_;
}

////////////////////////////////////////////////////

bool SimpleDS_Impl::ComputeDataOffsetPhyFromEntry(HashEntry* entry, uint64_t& data_offset) {
    return segMgr_->ComputeDataOffsetPhyFromEntry(entry, data_offset);
}

bool SimpleDS_Impl::ComputeKeyOffsetPhyFromEntry(HashEntry* entry, uint64_t& key_offset) {
    return segMgr_->ComputeKeyOffsetPhyFromEntry(entry, key_offset);
}

void SimpleDS_Impl::ModifyDeathEntry(HashEntry &entry) {
    segMgr_->ModifyDeathEntry(entry);
}

uint32_t SimpleDS_Impl::ComputeSegNum(uint64_t total_size, uint32_t seg_size) {
    return SegmentManager::ComputeSegNum(total_size, seg_size);
}

uint64_t SimpleDS_Impl::ComputeSegTableSizeOnDisk(uint32_t seg_num) {
    return SegmentManager::ComputeSegTableSizeOnDisk(seg_num);
}


bool SimpleDS_Impl::InitSegmentForCreateDB(uint64_t start_offset, uint32_t segment_size, uint32_t number_segments) {
    return segMgr_->InitSegmentForCreateDB(start_offset, segment_size, number_segments);
}

bool SimpleDS_Impl::LoadSegmentTableFromDevice(uint64_t start_offset, uint32_t segment_size, uint32_t num_seg, uint32_t current_seg) {
    return segMgr_->LoadSegmentTableFromDevice(start_offset, segment_size, num_seg, current_seg);
}

bool SimpleDS_Impl::WriteSegmentTableToDevice() {
    return segMgr_->WriteSegmentTableToDevice();
}

uint64_t SimpleDS_Impl::GetDataRegionSize() {
    return segMgr_->GetDataRegionSize();
}


uint32_t SimpleDS_Impl::GetTotalFreeSegs() {
    return segMgr_->GetTotalFreeSegs();
}

uint32_t SimpleDS_Impl::GetMaxValueLength() {
    return segMgr_->GetMaxValueLength();
}

uint32_t SimpleDS_Impl::GetTotalUsedSegs() {
    return segMgr_->GetTotalUsedSegs();
}

void SimpleDS_Impl::SortSegsByUtils(std::multimap<uint32_t, uint32_t> &cand_map, double utils) {
return segMgr_->SortSegsByUtils(cand_map, utils);
}

uint32_t SimpleDS_Impl::GetSegmentSize() {
return segMgr_->GetSegmentSize();
}
uint32_t SimpleDS_Impl::GetNumberOfSeg() {
return segMgr_->GetNumberOfSeg();
}

bool SimpleDS_Impl::AllocForGC(uint32_t& seg_id) {
return segMgr_->AllocForGC(seg_id);
}
void SimpleDS_Impl::FreeForFailed(uint32_t seg_id) {
return segMgr_->FreeForFailed(seg_id);
}
void SimpleDS_Impl::FreeForGC(uint32_t seg_id) {
return segMgr_->FreeForGC(seg_id);
}
void SimpleDS_Impl::Use(uint32_t seg_id, uint32_t free_size) {
return segMgr_->Use(seg_id, free_size);
}

bool SimpleDS_Impl::ComputeSegOffsetFromId(uint32_t seg_id, uint64_t& offset) {
return segMgr_->ComputeSegOffsetFromId(seg_id, offset);
}

/////////////////////////////////////////////////////

void SimpleDS_Impl::ReqMerge(Request* req) {
    std::unique_lock < std::mutex > lck_seg(segMtx_);
    if (seg_->TryPut(req)) {
        seg_->Put(req);
    } else {
        seg_->Complete();
        segWteWQ_->Add_task(seg_);
        seg_ = new SegForReq(this, idxMgr_, bdev_, options_.expired_time);
        seg_->Put(req);
    }
}

void SimpleDS_Impl::SegWrite(SegForReq *seg) {
    uint32_t seg_id = 0;
    bool res;
    while (!segMgr_->Alloc(seg_id)) {
        res = gcMgr_->ForeGC();
        if (!res) {
            __ERROR("Cann't get a new Empty Segment.\n");
            seg->Notify(res);
        }
    }
    uint32_t free_size = seg->GetFreeSize();
    seg->SetSegId(seg_id);
    res = seg->WriteSegToDevice();
    if (res) {
        segMgr_->Use(seg_id, free_size);
    } else {
        segMgr_->FreeForFailed(seg_id);
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
            seg_ = new SegForReq(this, idxMgr_, bdev_, options_.expired_time);
        }
        lck.unlock();

        usleep(options_.expired_time);
    } __DEBUG("Segment Timeout thread stop!!");
}

void SimpleDS_Impl::Do_GC() {
    __INFO("Application call GC!!!!!");
    return gcMgr_->FullGC();
}

void SimpleDS_Impl::GCThdEntry() {
    __DEBUG("GC thread start!!");
    while (!gcT_stop_) {
        gcMgr_->BackGC();
        usleep(1000000);
    } __DEBUG("GC thread stop!!");
}

} // namespace hlkvds
