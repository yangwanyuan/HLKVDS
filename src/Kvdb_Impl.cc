#include <stdio.h>
#include <string.h>
#include <inttypes.h>
#include <thread>
#include <map>

#include "Kvdb_Impl.h"
#include "KeyDigestHandle.h"
#include "KvdbIter.h"

namespace hlkvds {

KVDS* KVDS::Create_KVDS(const char* filename, Options opts) {
    if (filename == NULL) {
        return NULL;
    }

    KVDS* ds = new KVDS(filename, opts);

    if (!ds->metaStor_->CreateMetaData()) {
        delete ds;
        return NULL;
    }

    __INFO("\nCreateKVDS Success!!!\n");
    ds->printDbStates();

    ds->seg_ = new SegForReq(ds->segMgr_, ds->idxMgr_, ds->bdev_,
                                ds->options_.expired_time);
    ds->startThds();

    return ds;

}

Iterator* KVDS::NewIterator() {
    return new KvdbIter(idxMgr_, segMgr_, bdev_);
}

void KVDS::printDbStates() {

    uint32_t hash_table_size = sbMgr_->GetHTSize();
    uint32_t num_entries = idxMgr_->GetKeyCounter();
    uint32_t segment_size = sbMgr_->GetSegmentSize();
    uint32_t number_segments = sbMgr_->GetSegmentNum();
    uint32_t free_segment = segMgr_->GetTotalFreeSegs();
    uint64_t db_sb_size = sbMgr_->GetSbSize();
    uint64_t db_index_size = sbMgr_->GetIndexSize();
    uint64_t db_seg_table_size = sbMgr_->GetSegTableSize();
    uint64_t db_meta_size = db_sb_size + db_index_size + db_seg_table_size;
    uint64_t db_data_region_size = sbMgr_->GetDataRegionSize();
    uint64_t db_size = db_meta_size + db_data_region_size;
    uint64_t device_capacity = sbMgr_->GetDeviceCapacity();

    __INFO("\n DB Static information:\n"
            "\t hashtable_size            : %d\n"
            "\t segment_size              : %d Bytes\n"
            "\t number_segments           : %d\n"
            "\t Database Superblock Size  : %ld Bytes\n"
            "\t Database Index Size       : %ld Bytes\n"
            "\t Database Seg Table Size   : %ld Bytes\n"
            "\t Total DB Meta Region Size : %ld Bytes\n"
            "\t Total DB Data Region Size : %ld Bytes\n"
            "\t Total DB Total Size       : %ld Bytes\n"
            "\t Total Device Size         : %ld Bytes",
            hash_table_size, segment_size,
            number_segments, db_sb_size,
            db_index_size, db_seg_table_size, db_meta_size,
            db_data_region_size, db_size, device_capacity);

    __INFO("\n DB Dynamic information: \n"
            "\t # of entries              : %d\n"
            "\t # of free segments        : %d\n"
            "\t Current Segment ID        : %d\n"
            "\t DB Data Theory Size       : %ld Bytes\n"
            "\t Request Queue Size        : %d\n"
            "\t Segment Reaper Queue Size : %d\n"
            "\t Segment Write Queue Size  : %d",
            num_entries, free_segment,
            sbMgr_->GetCurSegmentId(),
            sbMgr_->GetDataTheorySize(), getReqQueSize(),
            getSegReaperQueSize(), getSegWriteQueSize());
}

KVDS* KVDS::Open_KVDS(const char* filename, Options opts) {
    KVDS *instance_ = new KVDS(filename, opts);

    Status s = instance_->openDB();
    if (!s.ok()) {
        delete instance_;
        return NULL;
    }
    return instance_;

}

Status KVDS::openDB() {
    if (!metaStor_->LoadMetaData()) {
        return Status::IOError("Could not read meta data");
    }

    printDbStates();

    seg_ = new SegForReq(segMgr_, idxMgr_, bdev_, options_.expired_time);
    startThds();
    return Status::OK();
}

Status KVDS::closeDB() {
    if (!metaStor_->PersistMetaData()) {
        __ERROR("Could not to write metadata to device\n");
        return Status::IOError("Could not to write metadata to device");
    }
    stopThds();
    return Status::OK();
}

void KVDS::startThds() {
    reqWQ_ = new ReqsMergeWQ(this, 1);
    reqWQ_->Start();

    segWteWQ_ = new SegmentWriteWQ(this, options_.seg_write_thread);
    segWteWQ_->Start();

    segTimeoutT_stop_.store(false);
    segTimeoutT_ = std::thread(&KVDS::SegTimeoutThdEntry, this);

    segRprWQ_ = new SegmentReaperWQ(this, 1);
    segRprWQ_->Start();

    gcT_stop_.store(false);
    gcT_ = std::thread(&KVDS::GCThdEntry, this);
}

void KVDS::stopThds() {
    gcT_stop_.store(true);
    gcT_.join();

    segRprWQ_->Stop();
    delete segRprWQ_;

    segTimeoutT_stop_.store(true);
    segTimeoutT_.join();

    reqWQ_->Stop();
    delete reqWQ_;

    segWteWQ_->Stop();
    delete segWteWQ_;
}

KVDS::~KVDS() {
    closeDB();
    delete gcMgr_;
    delete idxMgr_;
    delete segMgr_;
    delete sbMgr_;
    delete bdev_;
    delete seg_;
    if(!options_.disable_cache){
        delete rdCache_;
    }
    delete metaStor_;

}

KVDS::KVDS(const string& filename, Options opts) :
    fileName_(filename), seg_(NULL), options_(opts), metaStor_(NULL),
            reqWQ_(NULL), segWteWQ_(NULL), segTimeoutT_stop_(false),
            segRprWQ_(NULL), gcT_stop_(false) {
    bdev_ = BlockDevice::CreateDevice();
    sbMgr_ = new SuperBlockManager(bdev_, options_);
    segMgr_ = new SegmentManager(bdev_, sbMgr_, options_);
    idxMgr_ = new IndexManager(bdev_, sbMgr_, segMgr_, options_);
    gcMgr_ = new GcManager(bdev_, idxMgr_, segMgr_, options_);
    if(!options_.disable_cache){
        rdCache_ = new dslab::ReadCache(dslab::CachePolicy(options_.cache_policy), (size_t) options_.cache_size, options_.slru_partition);
    }
    metaStor_ = new MetaStor(filename.c_str(), options_, bdev_, sbMgr_, idxMgr_, segMgr_);
}

Status KVDS::Insert(const char* key, uint32_t key_len, const char* data,
                      uint16_t length) {
    if (key == NULL || key[0] == '\0') {
        return Status::InvalidArgument("Key is null or empty.");
    }

    if (length > segMgr_->GetMaxValueLength()) {
        return Status::NotSupported("Data length cann't be longer than max segment size");
    }

    KVSlice slice(key, key_len, data, length);

    if(!options_.disable_cache && !slice.GetDataLen()){
        rdCache_->Put(slice.GetKeyStr(),slice.GetDataStr());
    }

    return insertKey(slice);

}

Status KVDS::Delete(const char* key, uint32_t key_len) {
    return Insert(key, key_len, NULL, 0);
}

Status KVDS::Get(const char* key, uint32_t key_len, string &data) {
    bool res = false;
    if (key == NULL) {
        return Status::InvalidArgument("Key is null.");
    }

    KVSlice slice(key, key_len, NULL, 0);

    res = idxMgr_->GetHashEntry(&slice);
    if (!res) {
        //The key is not exist
        return Status::NotFound("Key is not found.");
    }
    if(!options_.disable_cache){
	if(rdCache_->Get(slice.GetKeyStr(), data)){
            return  Status::OK();
        }else{
	    Status _status = readData(slice, data);
            if(_status.ok()){
		rdCache_->Put(slice.GetKeyStr(), data);
	    }
	    return _status;
        }
    }else{
        Status _status = readData(slice, data);
        return _status;
    }
}

Status KVDS::InsertBatch(WriteBatch *batch)
{
    if (batch->batch_.empty()) {
        return Status::OK();
    }

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

    SegForSlice *seg = new SegForSlice(segMgr_, idxMgr_, bdev_);
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
    if(!options_.disable_cache){
        for (std::list<KVSlice *>::iterator iter = batch->batch_.begin();
            iter != batch->batch_.end(); iter++) {
	    if(!(*iter)->GetDataLen()){//no "" should be put in to the cache
	    	rdCache_->Put((*iter)->GetKeyStr(), (*iter)->GetDataStr());
    	    }
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

Status KVDS::insertKey(KVSlice& slice) {
    Request *req = new Request(slice);
    reqWQ_->Add_task(req);
    req->Wait();
    Status s = updateMeta(req);
    delete req;
    return s;
}

Status KVDS::updateMeta(Request *req) {
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
        segRprWQ_->Add_task(seg);
    }
    return Status::OK();
}

Status KVDS::readData(KVSlice &slice, string &data) {
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

void KVDS::ReqMerge(Request* req) {
    std::unique_lock < std::mutex > lck_seg(segMtx_);
    if (seg_->TryPut(req)) {
        seg_->Put(req);
    } else {
        seg_->Complete();
        segWteWQ_->Add_task(seg_);
        seg_ = new SegForReq(segMgr_, idxMgr_, bdev_,
                            options_.expired_time);
        seg_->Put(req);
    }
}

void KVDS::SegWrite(SegForReq *seg) {
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

void KVDS::SegTimeoutThdEntry() {
    __DEBUG("Segment Timeout thread start!!");
    std::unique_lock < std::mutex > lck(segMtx_, std::defer_lock);
    while (!segTimeoutT_stop_) {
        lck.lock();
        if (seg_->IsExpired()) {
            seg_->Complete();

            segWteWQ_->Add_task(seg_);
            seg_ = new SegForReq(segMgr_, idxMgr_, bdev_,
                                options_.expired_time);
        }
        lck.unlock();

        usleep(options_.expired_time);
    } __DEBUG("Segment Timeout thread stop!!");
}

void KVDS::SegReaper(SegForReq *seg) {
    seg->CleanDeletedEntry();
    __DEBUG("Segment reaper delete seg_id = %d", seg->GetSegId());
    delete seg;
}

void KVDS::Do_GC() {
    __INFO("Application call GC!!!!!");
    return gcMgr_->FullGC();
}

void KVDS::GCThdEntry() {
    __DEBUG("GC thread start!!");
    while (!gcT_stop_) {
        gcMgr_->BackGC();
        usleep(1000000);
    } __DEBUG("GC thread stop!!");
}
}

