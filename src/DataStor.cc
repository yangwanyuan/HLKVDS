#include "DataStor.h"
#include "Db_Structure.h"
#include "BlockDevice.h"
#include "SuperBlockManager.h"
#include "IndexManager.h"
#include "Volumes.h"

using namespace std;

namespace hlkvds {

//DataStor* DataStor::Create() {
//    return new SimpleDS_Impl();
//}

SimpleDS_Impl::SimpleDS_Impl(Options& opts, vector<BlockDevice*> &dev_vec, SuperBlockManager* sb, IndexManager* idx) :
        options_(opts), bdVec_(dev_vec), sbMgr_(sb), idxMgr_(idx), seg_(NULL), segSize_(0), maxValueLen_(0),
        volNum_(0), segTotalNum_(0), pickVolId_(-1), reqWQ_(NULL), segWteWQ_(NULL), segTimeoutT_stop_(false) {
}

SimpleDS_Impl::~SimpleDS_Impl() {
    delete seg_;
    deleteAllVolumes();
}

void SimpleDS_Impl::printDeviceTopology() {
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
    if (!vol->ComputeDataOffsetPhyFromEntry(entry, data_offset)) {
        return Status::Aborted("Compute data offset failed.");
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

bool SimpleDS_Impl::UpdateSST() {
    return true;
}

bool SimpleDS_Impl::GetAllSSTs(char* buf, uint64_t length) {
    uint64_t sst_total_length = GetSSTsLengthOnDisk();
    if (length != sst_total_length) {
        return false;
    }
    char *buf_ptr = buf;
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
    volNum_ = bdVec_.size();

    //Create Pure Volumes
    if (volNum_ > 1) {
        for (uint32_t i = 1; i < volNum_; i++ ) {
            BlockDevice * bdev = bdVec_[i];
            uint64_t device_capacity = bdev->GetDeviceCapacity();
            uint32_t seg_num = Volumes::ComputeSegNumForPureVolume(device_capacity, segSize_);
            segTotalNum_ += seg_num;
            Volumes *vol = new Volumes(bdev, idxMgr_, options_, i, 0, segSize_, seg_num, 0);
            volMap_.insert( pair<int, Volumes *>(i, vol) );
        }
    }

    //Create Meta Volumes
    BlockDevice * bdev = bdVec_[0];
    uint64_t device_capacity = bdev->GetDeviceCapacity();
    uint32_t seg_num = Volumes::ComputeSegNumForMetaVolume(device_capacity, sst_offset, segTotalNum_, segSize_);
    segTotalNum_ += seg_num;
    uint64_t start_off = sst_offset + Volumes::ComputeSSTsLengthOnDiskBySegNum(segTotalNum_);
    Volumes *vol = new Volumes(bdev, idxMgr_, options_, 0, start_off, segSize_, seg_num, 0);
    volMap_.insert( make_pair(0, vol) );

    //set Meta value
    maxValueLen_ = segSize_ - Volumes::SizeOfSegOnDisk() - IndexManager::SizeOfHashEntryOnDisk();

    initSBReservedContentForCreate();
}

bool SimpleDS_Impl::OpenAllVolumes() {
    volNum_ = sbResHeader_.volume_num;
    segSize_ = sbResHeader_.segment_size;

    BlockDevice *meta_bdev = bdVec_[0];
    uint32_t meta_seg_num = sbResVolVec_[0].segment_num;
    uint32_t meta_cur_seg_id = sbResVolVec_[0].cur_seg_id;

    if (!verifyTopology()) {
        __ERROR("The Device Topoloygy is inconsistent");
        return false;
    }

    //Create Meta Volume
    uint64_t start_off = sbMgr_->GetSSTRegionOffset() + sbMgr_->GetSSTRegionLength();

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

    maxValueLen_ = segSize_ - Volumes::SizeOfSegOnDisk() - IndexManager::SizeOfHashEntryOnDisk();

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

void SimpleDS_Impl::InitSegment() {
    int vol_id = pickVol();
    seg_ = new SegForReq(volMap_[vol_id], idxMgr_, options_.expired_time);
}

void SimpleDS_Impl::StartThds() {
    reqWQ_ = new ReqsMergeWQ(this, 1);
    reqWQ_->Start();

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

    int vol_id = getVolIdFromEntry(entry);
    Volumes *vol = volMap_[vol_id];

    if (!vol->ComputeKeyOffsetPhyFromEntry(entry, key_offset)) {
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

    if (!vol->ComputeDataOffsetPhyFromEntry(entry, data_offset)) {
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

////////////////////////////////////////////////////

void SimpleDS_Impl::ModifyDeathEntry(HashEntry &entry) {
    int vol_id = getVolIdFromEntry(&entry);
    volMap_[vol_id]->ModifyDeathEntry(entry);
}

uint64_t SimpleDS_Impl::ComputeSSTsLengthOnDiskBySegNum(uint32_t seg_num) {
    return Volumes::ComputeSSTsLengthOnDiskBySegNum(seg_num);
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
    std::unique_lock < std::mutex > lck_seg(segMtx_);
    if (seg_->TryPut(req)) {
        seg_->Put(req);
    } else {
        seg_->Complete();
        segWteWQ_->Add_task(seg_);

        int vol_id = pickVol();
        seg_ = new SegForReq(volMap_[vol_id], idxMgr_, options_.expired_time);
        seg_->Put(req);
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
    std::unique_lock < std::mutex > lck(segMtx_, std::defer_lock);
    while (!segTimeoutT_stop_) {
        lck.lock();
        if (seg_->IsExpired()) {
            seg_->Complete();

            segWteWQ_->Add_task(seg_);
            int vol_id = pickVol();
            seg_ = new SegForReq(volMap_[vol_id], idxMgr_, options_.expired_time);
        }
        lck.unlock();

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
