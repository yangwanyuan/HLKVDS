#include <sys/uio.h>
#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <errno.h>
#include <inttypes.h>

#include "Segment.h"
#include "IndexManager.h"
#include "Volume.h"

using namespace std;

namespace hlkvds {

SegHeaderOnDisk::SegHeaderOnDisk() :
        timestamp(0), trx_id(0), trx_segs(0), checksum_data(0),
        checksum_length(0), number_keys(0) {
}

SegHeaderOnDisk::~SegHeaderOnDisk() {
}

SegHeaderOnDisk::SegHeaderOnDisk(const SegHeaderOnDisk& toBeCopied) {
    timestamp = toBeCopied.timestamp;
    trx_id = toBeCopied.trx_id;
    trx_segs = toBeCopied.trx_segs;
    checksum_data = toBeCopied.checksum_data;
    checksum_length = toBeCopied.checksum_length;
    number_keys = toBeCopied.number_keys;
}

SegHeaderOnDisk& SegHeaderOnDisk::operator=(const SegHeaderOnDisk& toBeCopied) {
    timestamp = toBeCopied.timestamp;
    trx_id = toBeCopied.trx_id;
    trx_segs = toBeCopied.trx_segs;
    checksum_data = toBeCopied.checksum_data;
    checksum_length = toBeCopied.checksum_length;
    number_keys = toBeCopied.number_keys;
    return *this;
}

SegHeaderOnDisk::SegHeaderOnDisk(uint64_t ts, uint64_t id, uint32_t segs,
                                uint32_t data, uint32_t len, uint32_t keys_num) :
        timestamp(ts), trx_id(id), trx_segs(segs), checksum_data(data),
        checksum_length(len), number_keys(keys_num) {
}


KVSlice::KVSlice() :
    key_(NULL), keyLength_(0), data_(NULL), dataLength_(0), digest_(NULL),
            entry_(NULL), segId_(0), entryGC_(NULL) {
}

KVSlice::~KVSlice() {
    if (deepCopy_) {
        delete[] key_;
        delete[] data_;
    }
    if (digest_) {
        delete digest_;
    }
    if (entry_) {
        delete entry_;
    }
    if (entryGC_) {
        delete entryGC_;
    }
}

KVSlice::KVSlice(const KVSlice& toBeCopied) :
    key_(NULL), keyLength_(0), data_(NULL), dataLength_(0), digest_(NULL),
            entry_(NULL), segId_(0), deepCopy_(false), entryGC_(NULL) {
    copy_helper(toBeCopied);
}

KVSlice& KVSlice::operator=(const KVSlice& toBeCopied) {
    copy_helper(toBeCopied);
    return *this;
}

void KVSlice::copy_helper(const KVSlice& toBeCopied) {
    keyLength_ = toBeCopied.GetKeyLen();
    dataLength_ = toBeCopied.GetDataLen();
    key_ = toBeCopied.GetKey();
    data_ = toBeCopied.GetData();
    *digest_ = *toBeCopied.digest_;
    *entry_ = *toBeCopied.entry_;
    segId_ = toBeCopied.segId_;
    deepCopy_ = toBeCopied.deepCopy_;
    *entryGC_ = *toBeCopied.entryGC_;
}

KVSlice::KVSlice(const char* key, int key_len, const char* data, int data_len, bool deep_copy) :
    key_(NULL), keyLength_(key_len), data_(NULL), dataLength_(data_len),
            digest_(NULL), entry_(NULL), segId_(0), deepCopy_(deep_copy),
            entryGC_(NULL) {
    if (deepCopy_) {
        key_ = new char[key_len];
        data_ = new char[data_len];
        memcpy((void*)key_, key, key_len);
        memcpy((void*)data_, data, data_len);
    } else {
        key_ = key;
        data_ = data;
    }
    calcDigest();
}

KVSlice::KVSlice(Kvdb_Digest *digest, const char* key, int key_len,
                const char* data, int data_len) :
    key_(key), keyLength_(key_len), data_(data), dataLength_(data_len),
            digest_(NULL), entry_(NULL), segId_(0), deepCopy_(false),
            entryGC_(NULL) {
    digest_ = new Kvdb_Digest(*digest);
}

void KVSlice::SetKeyValue(const char* key, int key_len, const char* data,
                          int data_len) {
    keyLength_ = key_len;
    dataLength_ = data_len;
    key_ = key;
    data_ = data;

    calcDigest();
}

void KVSlice::calcDigest() {
    if (digest_) {
        delete digest_;
        digest_=nullptr;
    }
    digest_ = new Kvdb_Digest();
    Kvdb_Key vkey(key_, keyLength_);
    KeyDigestHandle::CalcDigest(&vkey, *digest_);
}

string KVSlice::GetKeyStr() const {
    return string(key_, keyLength_);
}

string KVSlice::GetDataStr() const {
    return string(data_, dataLength_);
}

void KVSlice::SetHashEntry(const HashEntry *hash_entry) {
    entry_ = new HashEntry(*hash_entry);
}

void KVSlice::SetHashEntryBeforeGC(const HashEntry *hash_entry) {
    entryGC_ = new HashEntry(*hash_entry);
}

void KVSlice::SetSegId(uint32_t seg_id) {
    segId_ = seg_id;
}

Request::Request() :
    done_(false), stat_(ReqStat::INIT), slice_(NULL), segPtr_(NULL), shardsWqId_(-1) {
}

Request::~Request() {
}

Request::Request(const Request& toBeCopied) :
    done_(false), stat_(toBeCopied.stat_), slice_(toBeCopied.slice_),
    segPtr_(toBeCopied.segPtr_) {
}

Request& Request::operator=(const Request& toBeCopied) {
    done_ = toBeCopied.done_;
    stat_ = toBeCopied.stat_;
    slice_ = toBeCopied.slice_;
    segPtr_ = toBeCopied.segPtr_;
    return *this;
}
Request::Request(KVSlice& slice) : 
    done_(false), stat_(ReqStat::INIT), slice_(&slice), segPtr_(NULL) {
}

void Request::SetWriteStat(bool stat) {
    std::lock_guard<std::mutex> l(mtx_);
    stat_ = stat? ReqStat::SUCCESS : ReqStat::FAIL ;
}

void Request::Wait() {
    std::unique_lock<std::mutex> l(mtx_);
    while(!done_) {
        cv_.wait(l);
    }
}

void Request::Signal() {
    std::unique_lock<std::mutex> l(mtx_);
    done_ = true;
    cv_.notify_one();
}

SegBase::SegBase() :
    segId_(-1), vol_(NULL), segSize_(-1),
        headPos_(0), tailPos_(0), keyNum_(0),
        keyAlignedNum_(0), dataBuf_(NULL) {
}

SegBase::~SegBase() {
}

SegBase::SegBase(const SegBase& toBeCopied) {
    copyHelper(toBeCopied);
}

SegBase& SegBase::operator=(const SegBase& toBeCopied) {
    copyHelper(toBeCopied);
    return *this;
}

void SegBase::copyHelper(const SegBase& toBeCopied) {
    segId_ = toBeCopied.segId_;
    vol_ = toBeCopied.vol_;
    segSize_ = toBeCopied.segSize_;
    headPos_ = toBeCopied.headPos_;
    tailPos_ = toBeCopied.tailPos_;
    keyNum_ = toBeCopied.keyNum_;
    keyAlignedNum_ = toBeCopied.keyAlignedNum_;
    memcpy(dataBuf_, toBeCopied.dataBuf_, segSize_);
    sliceList_ = toBeCopied.sliceList_;
}

SegBase::SegBase(Volume* vol) :
        segId_(-1), vol_(vol),
        segSize_(vol_->GetSegmentSize()),
        headPos_(SegBase::SizeOfSegOnDisk()), tailPos_(segSize_),
        keyNum_(0), keyAlignedNum_(0), dataBuf_(NULL) {
}

bool SegBase::TryPut(KVSlice* slice) {
    uint32_t freeSize = tailPos_ - headPos_;
    uint32_t needSize = slice->GetDataLen() + slice->GetKeyLen() + IndexManager::SizeOfDataHeader();
    return freeSize > needSize;
}

void SegBase::Put(KVSlice* slice) {
    if (slice->IsAlignedData()) {
        headPos_ += IndexManager::SizeOfDataHeader() + slice->GetKeyLen();
        tailPos_ -= ALIGNED_SIZE;
        keyAlignedNum_++;
    } else {
        headPos_ += IndexManager::SizeOfDataHeader() + slice->GetKeyLen() + slice->GetDataLen();
    }
    keyNum_++;
    sliceList_.push_back(slice);
    __DEBUG("Put request key = %s", slice->GetKeyStr().c_str());
}

bool SegBase::TryPutList(std::list<KVSlice*> &slice_list) {
    uint32_t freeSize = tailPos_ - headPos_;
    uint32_t needSize = 0;
    for (list<KVSlice *>::iterator iter = slice_list.begin(); iter != slice_list.end(); iter++) {
        KVSlice *slice = *iter;
        needSize += slice->GetDataLen() + slice->GetKeyLen() + IndexManager::SizeOfDataHeader();
    }
    return freeSize > needSize;

}

void SegBase::PutList(std::list<KVSlice*> &slice_list) {
    for (list<KVSlice *>::iterator iter = slice_list.begin(); iter != slice_list.end(); iter++) {
        KVSlice *slice = *iter;
        Put(slice);
    }

}

bool SegBase::WriteSegToDevice() {
    if (segId_ < 0)
    {
        __ERROR("Not set seg_id to segment");
        return false;
    }
    fillEntryToSlice();
    __DEBUG("Begin write seg, free size %u, seg id: %d, key num: %d", tailPos_-headPos_ , segId_, keyNum_);
    return _writeDataToDevice();
}

void SegBase::fillEntryToSlice() {
    uint32_t head_pos = SegBase::SizeOfSegOnDisk();
    uint32_t tail_pos = segSize_;
    int vol_id = vol_->GetId();
    for (list<KVSlice *>::iterator iter = sliceList_.begin(); iter
            != sliceList_.end(); iter++) {

        KVSlice *slice = *iter;
        slice->SetSegId(segId_);
        if (slice->IsAlignedData()) {
            uint32_t data_offset = tail_pos - ALIGNED_SIZE;
            uint32_t next_offset = head_pos + IndexManager::SizeOfDataHeader() + slice->GetKeyLen();
            DataHeader data_header(slice->GetDigest(), slice->GetKeyLen(), slice->GetDataLen(),
                                   data_offset, next_offset);

            uint64_t seg_offset = 0;
            vol_->CalcSegOffsetFromId(segId_, seg_offset);
            uint64_t header_offset = seg_offset + head_pos;

            DataHeaderAddress addrs(vol_id, header_offset);
            HashEntry hash_entry(data_header, addrs, NULL);
            slice->SetHashEntry(&hash_entry);

            head_pos += IndexManager::SizeOfDataHeader() + slice->GetKeyLen();
            tail_pos -= ALIGNED_SIZE;
            __DEBUG("SegmentSlice: key=%s, data_offset=%u, header_offset=%lu, seg_id=%u, head_pos=%u, tail_pos = %u", slice->GetKey(), data_offset, header_offset, segId_, head_pos, tail_pos);
        } else {
            uint32_t data_offset = head_pos + IndexManager::SizeOfDataHeader() + slice->GetKeyLen();
            uint32_t next_offset = head_pos + IndexManager::SizeOfDataHeader()
                    + slice->GetKeyLen() + slice->GetDataLen();

            DataHeader data_header(slice->GetDigest(), slice->GetKeyLen(), slice->GetDataLen(),
                                   data_offset, next_offset);
            uint64_t seg_offset = 0;
            vol_->CalcSegOffsetFromId(segId_, seg_offset);
            uint64_t header_offset = seg_offset + head_pos;

            DataHeaderAddress addrs(vol_id, header_offset);
            HashEntry hash_entry(data_header, addrs, NULL);
            slice->SetHashEntry(&hash_entry);

            head_pos += IndexManager::SizeOfDataHeader() + slice->GetKeyLen() + slice->GetDataLen();
            __DEBUG("SegmentSlice: key=%s, data_offset=%u, header_offset=%lu, seg_id=%u, head_pos=%u, tail_pos = %u", slice->GetKey(), data_offset, header_offset, segId_, head_pos, tail_pos);

        }
    }
    if (head_pos != headPos_ || tail_pos != tailPos_) {
        __ERROR("Segment fillEntryToSlice  Failed!!! head_pos= %u, headPos_=%u, tail_pos= %u, tailPos_ = %u", head_pos, headPos_, tail_pos, tailPos_);
    }
}


bool SegBase::_writeDataToDevice() {
    posix_memalign((void **)&dataBuf_, 4096, segSize_);

    copyToDataBuf();
    uint64_t offset = 0;
    vol_->CalcSegOffsetFromId(segId_, offset);

    bool ret = vol_->Write(dataBuf_, segSize_, offset);

    free(dataBuf_);
    dataBuf_ = NULL;

    return ret;
}

void SegBase::copyToDataBuf() {
    uint64_t offset = 0;
    vol_->CalcSegOffsetFromId(segId_, offset);

    uint32_t offset_begin = 0;
    uint32_t offset_end = segSize_;

    offset_begin += SegBase::SizeOfSegOnDisk();

    //aggregate iovec
    for (list<KVSlice *>::iterator iter = sliceList_.begin(); iter
            != sliceList_.end(); iter++) {
        KVSlice *slice = *iter;
        DataHeader *header =
                &slice->GetHashEntry().GetEntryOnDisk().GetDataHeader();
        char *data = (char *) slice->GetData();
        uint16_t data_len = slice->GetDataLen();

        char *key = (char *) slice->GetKey();
        uint16_t key_len = slice->GetKeyLen();
        memcpy(&(dataBuf_[offset_begin]), header,
               IndexManager::SizeOfDataHeader());
        offset_begin += IndexManager::SizeOfDataHeader();
        memcpy(&(dataBuf_[offset_begin]), key, key_len);
        offset_begin += key_len;

        if (slice->IsAlignedData()) {
            offset_end -= data_len;
            memcpy(&(dataBuf_[offset_end]), data, data_len);
            __DEBUG("write key = %s, data position: %u", slice->GetKey(), offset_end);
        } else {
            memcpy(&(dataBuf_[offset_begin]), data, data_len);
            __DEBUG("write key = %s, data position: %u", slice->GetKey(), offset_begin);
            offset_begin += data_len;
        }
    }

    // Generate SegHeaderOnDisk
    uint64_t timestamp = KVTime::GetNow();
    uint64_t trx_id = 0;
    uint32_t trx_segs = 0;
    uint32_t checksum_data = 0;
    uint32_t checksum_length = 0;
    SegHeaderOnDisk seg_header(timestamp, trx_id, trx_segs, checksum_data, checksum_length, keyNum_);
    memcpy(dataBuf_, &seg_header, SegBase::SizeOfSegOnDisk());

    //set 0 to free data buffer
    memset(&(dataBuf_[offset_begin]), 0, (offset_end - offset_begin));
}

SegForReq::SegForReq() :
    SegBase(), idxMgr_(NULL), timeout_(0), startTime_(KVTime()), persistTime_(KVTime()),
        isCompletion_(false), hasReq_(false), reqCommited_(0) {
}

SegForReq::~SegForReq() {
}

SegForReq::SegForReq(const SegForReq& toBeCopied) : SegBase(toBeCopied) {
}

SegForReq& SegForReq::operator=(const SegForReq& toBeCopied) {
    if (this == &toBeCopied) {
        return *this;
    }

    SegBase::operator=(toBeCopied);
    idxMgr_ = toBeCopied.idxMgr_;
    timeout_ = toBeCopied.timeout_;
    startTime_ = toBeCopied.startTime_;
    persistTime_ = toBeCopied.persistTime_;
    isCompletion_ = toBeCopied.isCompletion_;
    hasReq_ = toBeCopied.hasReq_;
    reqCommited_.store(toBeCopied.reqCommited_.load());
    reqList_ = toBeCopied.reqList_;
    delReqList_ = toBeCopied.delReqList_;
    return *this;
}

SegForReq::SegForReq(Volume* vol, IndexManager* im, uint32_t timeout) :
    SegBase(vol), idxMgr_(im), timeout_(timeout), startTime_(KVTime()), persistTime_(KVTime()),
    isCompletion_(false), hasReq_(false), reqCommited_(0) {
}

bool SegForReq::TryPut(Request* req) {
    if (isCompletion_) {
        return false;
    }
    KVSlice *slice= &req->GetSlice();
    return SegBase::TryPut(slice);
}

void SegForReq::Put(Request* req) {
    KVSlice *slice = &req->GetSlice();
    if (GetKeyNum() == 0) {
        hasReq_ = true;
        startTime_.Update();
    }
    SegBase::Put(slice);
    reqList_.push_back(req);
    req->SetSeg(this);
    __DEBUG("Put request key = %s", req->GetSlice().GetKeyStr().c_str());
}

void SegForReq::Completion() {
    if (isCompletion_) {
         return;
    }
    isCompletion_ = true;
}

void SegForReq::Notify(bool stat) {
    std::lock_guard < std::mutex > l(mtx_);

    //Set logic timestamp to hashentry
    reqCommited_.store(GetKeyNum());

    int32_t keyNo = 0;
    persistTime_.Update();
    for (list<Request *>::iterator iter = reqList_.begin(); iter
            != reqList_.end(); iter++) {
        keyNo++;
        KVSlice *slice = &(*iter)->GetSlice();
        HashEntry &entry = slice->GetHashEntry();
        entry.SetLogicStamp(persistTime_, keyNo);

        if (!slice->GetData()) {
            delReqList_.push_back(entry);
        }
    }

    //notify and clean req.
    while (!reqList_.empty()) {
        Request *req = reqList_.front();
        reqList_.pop_front();
        req->SetWriteStat(stat);
        req->Signal();
    }

}

bool SegForReq::IsExpired() {
    KVTime nowTime;
    if (!hasReq_) {
        return false;
    }
    int64_t interval = nowTime - startTime_;
    return (interval > timeout_);
}

void SegForReq::CleanDeletedEntry() {
    std::lock_guard < std::mutex > l(mtx_);
    for (list<HashEntry>::iterator iter = delReqList_.begin(); iter
            != delReqList_.end(); iter++) {
        idxMgr_->RemoveEntry(*iter);
    }
    delReqList_.clear();
}

SegForSlice::SegForSlice() :
    SegBase(), idxMgr_(NULL) {
}

SegForSlice::~SegForSlice() {
}

SegForSlice::SegForSlice(const SegForSlice& toBeCopied) : SegBase(toBeCopied) {
    idxMgr_ = toBeCopied.idxMgr_;
}

SegForSlice& SegForSlice::operator=(const SegForSlice& toBeCopied) {
    if (this == &toBeCopied) {
        return *this;
    }
    SegBase::operator=(toBeCopied);
    idxMgr_ = toBeCopied.idxMgr_;
    return *this;
}

SegForSlice::SegForSlice(Volume* vol, IndexManager* im) :
    SegBase(vol), idxMgr_(im) {
}

void SegForSlice::UpdateToIndex() {
    list<KVSlice *> &slice_list = GetSliceList();
    idxMgr_->UpdateIndexes(slice_list);

}

SegForMigrate::SegForMigrate() :
    SegBase(), idxMgr_(NULL) {
}

SegForMigrate::~SegForMigrate() {
}

SegForMigrate& SegForMigrate::operator=(const SegForMigrate& toBeCopied) {
    if (this == &toBeCopied) {
        return *this;
    }
    SegBase::operator=(toBeCopied);
    idxMgr_ = toBeCopied.idxMgr_;
    return *this;
}

SegForMigrate::SegForMigrate(Volume* vol, IndexManager* im) :
    SegBase(vol), idxMgr_(im) {
}

void SegForMigrate::UpdateToIndex() {
    list<KVSlice *> &slice_list = GetSliceList();
    idxMgr_->UpdateIndexesForGC(slice_list);
}


SegLatencyFriendly::SegLatencyFriendly() :
    vol_(NULL), idxMgr_(NULL), segId_(-1), segSize_(-1),
    headPos_(0), keyNum_(0), checksumSize_(0), dataBuf_(NULL) {
}

SegLatencyFriendly::~SegLatencyFriendly() {
}

SegLatencyFriendly::SegLatencyFriendly(const SegLatencyFriendly& toBeCopied) {
    copyHelper(toBeCopied);
}

SegLatencyFriendly& SegLatencyFriendly::operator=(const SegLatencyFriendly& toBeCopied) {
    copyHelper(toBeCopied);
    return *this;
}

SegLatencyFriendly::SegLatencyFriendly(Volume* vol, IndexManager* im) :
    vol_(vol), idxMgr_(im), segId_(-1), segSize_(vol_->GetSegmentSize()),
    headPos_(SegBase::SizeOfSegOnDisk()), keyNum_(0), checksumSize_(0),
    dataBuf_(NULL){
}

void SegLatencyFriendly::copyHelper(const SegLatencyFriendly& toBeCopied) {
    segId_ = toBeCopied.segId_;
    vol_ = toBeCopied.vol_;
    segSize_ = toBeCopied.segSize_;

    headPos_ = toBeCopied.headPos_;
    keyNum_ = toBeCopied.keyNum_;
    checksumSize_ = toBeCopied.checksumSize_;
}

bool SegLatencyFriendly::TryPut(KVSlice* slice) {
    uint32_t free_size = segSize_ - headPos_;
    uint32_t need_size = slice->GetDataLen() + slice->GetKeyLen() + IndexManager::SizeOfDataHeader();
    return free_size > need_size;
}

void SegLatencyFriendly::Put(KVSlice* slice) {
    headPos_ += IndexManager::SizeOfDataHeader() + slice->GetKeyLen() + slice->GetDataLen();
    keyNum_++;
    sliceList_.push_back(slice);
    __DEBUG("Put request key = %s", slice->GetKeyStr().c_str());
}

bool SegLatencyFriendly::TryPutList(std::list<KVSlice*> &slice_list) {
    uint32_t free_size = segSize_ - headPos_;
    uint32_t need_size = 0;
    for (list<KVSlice *>::iterator iter = slice_list.begin(); iter != slice_list.end(); iter++) {
        KVSlice *slice = *iter;
        need_size += slice->GetDataLen() + slice->GetKeyLen() + IndexManager::SizeOfDataHeader();
    }
    return free_size > need_size;
}

void SegLatencyFriendly::PutList(std::list<KVSlice*> &slice_list) {
    for (list<KVSlice *>::iterator iter = slice_list.begin(); iter != slice_list.end(); iter++) {
        KVSlice *slice = *iter;
        Put(slice);
    }
}

bool SegLatencyFriendly::WriteSegToDevice() {
    if (segId_ < 0)
    {
        __ERROR("Not set seg_id to segment");
        return false;
    }
    fillEntryToSlice();
    __DEBUG("Begin write seg, free size %u, seg id: %d, key num: %d", tailPos_-headPos_ , segId_, keyNum_);
    return _writeDataToDevice();
}

void SegLatencyFriendly::fillEntryToSlice() {
    uint32_t head_pos = SegBase::SizeOfSegOnDisk();
    int vol_id = vol_->GetId();

    for (list<KVSlice *>::iterator iter = sliceList_.begin(); iter
            != sliceList_.end(); iter++) {
        KVSlice *slice = *iter;
        slice->SetSegId(segId_);

        uint32_t data_offset = head_pos + IndexManager::SizeOfDataHeader() + slice->GetKeyLen();
        uint32_t next_offset = head_pos + IndexManager::SizeOfDataHeader() + slice->GetKeyLen() + slice->GetDataLen();
        DataHeader data_header(slice->GetDigest(), slice->GetKeyLen(), slice->GetDataLen(),
                               data_offset, next_offset);
        uint64_t seg_offset = 0;
        vol_->CalcSegOffsetFromId(segId_, seg_offset);
        uint64_t header_offset = seg_offset + head_pos;

        DataHeaderAddress addrs(vol_id, header_offset);
        HashEntry hash_entry(data_header, addrs, NULL);
        slice->SetHashEntry(&hash_entry);

        head_pos += IndexManager::SizeOfDataHeader() + slice->GetKeyLen() + slice->GetDataLen();
        __DEBUG("SegmentSlice: key=%s, data_offset=%u, header_offset=%lu, seg_id=%u, head_pos=%u", slice->GetKey(), data_offset, header_offset, segId_, head_pos);
    }

    if (head_pos != headPos_ ) {
        __ERROR("Segment fillEntryToSlice  Failed!!! head_pos= %u, headPos_=%u", head_pos, headPos_);
    }
}

bool SegLatencyFriendly::_writeDataToDevice() {
    checksumSize_ = headPos_ - SegBase::SizeOfSegOnDisk();
    uint32_t pages_num = headPos_ / getpagesize();
    uint32_t aligned_size = ( pages_num + 1 ) * getpagesize();

    posix_memalign((void**)&dataBuf_, 4096, aligned_size);

    copyToDataBuf();
    uint64_t offset = 0;
    vol_->CalcSegOffsetFromId(segId_, offset);

    bool ret = vol_->Write(dataBuf_, aligned_size, offset);

    free(dataBuf_);
    dataBuf_ = NULL;


    return ret;
}

void SegLatencyFriendly::copyToDataBuf() {
    uint64_t offset = 0;
    vol_->CalcSegOffsetFromId(segId_, offset);

    uint32_t offset_begin = 0;

    offset_begin += SegBase::SizeOfSegOnDisk();

    //aggregate iovec
    for (list<KVSlice *>::iterator iter = sliceList_.begin(); iter
            != sliceList_.end(); iter++) {
        KVSlice *slice = *iter;
        DataHeader *header =
                &slice->GetHashEntry().GetEntryOnDisk().GetDataHeader();
        char *data = (char *) slice->GetData();
        uint16_t data_len = slice->GetDataLen();

        char *key = (char *) slice->GetKey();
        uint16_t key_len = slice->GetKeyLen();
        memcpy(&(dataBuf_[offset_begin]), header, IndexManager::SizeOfDataHeader());
        __DEBUG("write key = %s, seg_id: %u, header offset: %u", slice->GetKey(), segId_, offset_begin);
        offset_begin += IndexManager::SizeOfDataHeader();
        memcpy(&(dataBuf_[offset_begin]), key, key_len);
        __DEBUG("write key = %s, key offset: %u", slice->GetKey(), offset_begin);
        offset_begin += key_len;
        memcpy(&(dataBuf_[offset_begin]), data, data_len);
        __DEBUG("write key = %s, data position: %u", slice->GetKey(), offset_begin);
        offset_begin += data_len;
    }

    // Generate SegHeaderOnDisk
    uint64_t timestamp = KVTime::GetNow();
    uint64_t trx_id = 0;
    uint32_t trx_segs = 0;
    uint32_t checksum_data = 0;
    SegHeaderOnDisk seg_header(timestamp, trx_id, trx_segs, checksum_data, checksumSize_, keyNum_);
    memcpy(dataBuf_, &seg_header, SegBase::SizeOfSegOnDisk());

}

void SegLatencyFriendly::UpdateToIndex() {
    list<KVSlice *> &slice_list = GetSliceList();
    idxMgr_->UpdateIndexes(slice_list);
}

}
