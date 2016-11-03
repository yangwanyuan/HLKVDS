#include <sys/uio.h>
#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <errno.h>
#include <inttypes.h>

#include "DataHandle.h"

namespace kvdb{

    KVSlice::KVSlice()
        : key_(NULL), keyLength_(0), data_(NULL), dataLength_(0), digest_(NULL), isComputed_(false), entry_(NULL), segId_(0){}

    KVSlice::~KVSlice()
    {
        if (digest_)
        {
            delete digest_;
        }
        if (entry_)
        {
            delete entry_;
        }
    }

    KVSlice::KVSlice(const KVSlice& toBeCopied)
        : key_(NULL), keyLength_(0), data_(NULL), dataLength_(0), digest_(NULL), isComputed_(false), entry_(NULL), segId_(0)
    {
        copy_helper(toBeCopied);
    }

    KVSlice& KVSlice::operator=(const KVSlice& toBeCopied)
    {
        copy_helper(toBeCopied);
        return *this;
    }

    void KVSlice::copy_helper(const KVSlice& toBeCopied)
    {
        keyLength_ = toBeCopied.GetKeyLen();
        dataLength_ = toBeCopied.GetDataLen();
        key_ = toBeCopied.GetKey();
        data_ = toBeCopied.GetData();
        *digest_ = *toBeCopied.digest_;
        isComputed_ = toBeCopied.IsDigestComputed();
        *entry_ = *toBeCopied.entry_;
        segId_ = toBeCopied.segId_;
    }

    KVSlice::KVSlice(const char* key, int key_len, const char* data, int data_len)
        : key_(key), keyLength_(key_len), data_(data), dataLength_(data_len), digest_(NULL), isComputed_(false), entry_(NULL), segId_(0){}

    void KVSlice::SetKeyValue(const char* key, int key_len, const char* data, int data_len)
    {
        isComputed_ = false;
        keyLength_ = key_len;
        dataLength_ = data_len;
        key_ = key;
        data_ = data;
    }

    bool KVSlice::ComputeDigest()
    {
        if (!key_)
        {
            return false;
        }
        digest_ = new Kvdb_Digest();
        Kvdb_Key vkey(key_, keyLength_);
        KeyDigestHandle::ComputeDigest(&vkey, *digest_);

        isComputed_ = true;
        return true;
    }

    string KVSlice::GetKeyStr() const
    {
        char * data = new char[keyLength_ + 1];
        memcpy(data, key_, keyLength_);
        data[keyLength_] = '\0';
        string r(data);
        delete[] data;
        return r;
    }

    string KVSlice::GetDataStr() const
    {
        char * data = new char[dataLength_ + 1];
        memcpy(data, data_, dataLength_);
        data[dataLength_] = '\0';
        string r(data);
        delete[] data;
        return r;
    }

    void KVSlice::SetHashEntry(const HashEntry *hash_entry)
    {
        entry_ = new HashEntry(*hash_entry);
    }

    void KVSlice::SetSegId(uint32_t seg_id)
    {
        segId_ = seg_id;
    }

    Request::Request(): isDone_(0), writeStat_(false), slice_(NULL), segPtr_(NULL){}

    Request::~Request(){}

    Request::Request(const Request& toBeCopied) :
        isDone_(toBeCopied.isDone_), writeStat_(toBeCopied.writeStat_),
        slice_(toBeCopied.slice_), segPtr_(toBeCopied.segPtr_){}

    Request& Request::operator=(const Request& toBeCopied)
    {
        isDone_ = toBeCopied.isDone_;
        writeStat_ = toBeCopied.writeStat_;
        slice_ = toBeCopied.slice_;
        segPtr_ = toBeCopied.segPtr_;
        return *this;
    }

    Request::Request(KVSlice& slice) : isDone_(0), writeStat_(false), slice_(&slice), segPtr_(NULL){}

    void Request::Done()
    {
        std::lock_guard<std::mutex> l(mtx_);
        isDone_ = 1;
    }

    void Request::SetState(bool state)
    {
        std::lock_guard<std::mutex> l(mtx_);
        writeStat_ = state;
    }

    void Request::Wait()
    {
        std::unique_lock<std::mutex> l(mtx_);
        cv_.wait(l);
    }

    void Request::Signal()
    {
        std::unique_lock<std::mutex> l(mtx_);
        cv_.notify_all();
    }

    SegmentSlice::SegmentSlice()
        :segId_(0), segMgr_(NULL), bdev_(NULL), segSize_(0),
        persistTime_(KVTime()), startTime_(KVTime()),
        headPos_(0), tailPos_(0), keyNum_(0), keyAlignedNum_(0),
        isCompleted_(false), hasReq_(false), segOndisk_(NULL)
    {
        segOndisk_ = new SegmentOnDisk();
    }

    SegmentSlice::~SegmentSlice()
    {
        delReqList_.clear();
        delete segOndisk_;
        //if(data_)
        //{
        //    delete[] data_;
        //}
    }

    SegmentSlice::SegmentSlice(const SegmentSlice& toBeCopied)
    {
        segOndisk_ = new SegmentOnDisk();
        copyHelper(toBeCopied);
    }

    SegmentSlice& SegmentSlice::operator=(const SegmentSlice& toBeCopied)
    {
        copyHelper(toBeCopied);
        return *this;
    }

    void SegmentSlice::copyHelper(const SegmentSlice& toBeCopied)
    {
        segId_ = toBeCopied.segId_;
        segMgr_ = toBeCopied.segMgr_;
        bdev_ = toBeCopied.bdev_;
        segSize_ = toBeCopied.segSize_;
        persistTime_ = toBeCopied.persistTime_;
        startTime_ = toBeCopied.startTime_;
        headPos_ = toBeCopied.headPos_;
        tailPos_ = toBeCopied.tailPos_;
        keyNum_ = toBeCopied.keyNum_;
        keyAlignedNum_ = toBeCopied.keyAlignedNum_;
        isCompleted_ = toBeCopied.isCompleted_;
        hasReq_ = toBeCopied.hasReq_;
        reqCommited_.store(toBeCopied.reqCommited_.load());
        isCanReap_.store(toBeCopied.isCanReap_.load());
        reqList_ = toBeCopied.reqList_;
        *segOndisk_ = *toBeCopied.segOndisk_;
        delReqList_ = toBeCopied.delReqList_;
        //data_ = toBeCopied.data_;
    }

    SegmentSlice::SegmentSlice(SegmentManager* sm, BlockDevice* bdev)
        : segId_(0), segMgr_(sm), bdev_(bdev),
        segSize_(segMgr_->GetSegmentSize()), persistTime_(KVTime()),
        startTime_(KVTime()),
        headPos_(SegmentManager::SizeOfSegOnDisk()), tailPos_(segSize_),
        keyNum_(0), keyAlignedNum_(0), isCompleted_(false), hasReq_(false),
        reqCommited_(-1), isCanReap_(false), segOndisk_(NULL)
    {
        segOndisk_ = new SegmentOnDisk();
        //data_ = new char[segSize_];
    }

    bool SegmentSlice::TryPut(Request* req)
    {
        if(isCompleted_)
        {
            return false;
        }
        KVSlice *slice = &req->GetSlice();
        uint32_t freeSize = tailPos_ - headPos_;
        uint32_t needSize = slice->GetDataLen() + IndexManager::SizeOfDataHeader();
        if (freeSize < needSize)
        {
            return false;
        }
        return true;
    }

    bool SegmentSlice::Put(Request* req)
    {
        //std::lock_guard<std::mutex> l(mtx_);
        //if(isCompleted_)
        //{
        //    return false;
        //}
        //KVSlice *slice = &req->GetSlice();
        //uint32_t freeSize = tailPos_ - headPos_;
        //uint32_t needSize = slice->GetDataLen() + IndexManager::SizeOfDataHeader();
        //if (freeSize < needSize)
        //{
        //    return false;
        //}
        //
        KVSlice *slice = &req->GetSlice();

        if ( keyNum_ == 0)
        {
            hasReq_ = true;
            startTime_.Update();
        }

        if (slice->IsAlignedData())
        {
            headPos_ += IndexManager::SizeOfDataHeader();
            tailPos_ -= ALIGNED_SIZE;
            keyAlignedNum_++;
        }
        else
        {
            headPos_ += IndexManager::SizeOfDataHeader() + slice->GetDataLen();
        }
        keyNum_++;
        reqList_.push_back(req);
        req->SetSeg(this);

        __DEBUG("Put request key = %s", req->GetSlice().GetKeyStr().c_str());
        return true;
    }

    bool SegmentSlice::_writeToDeviceHugeHole()
    {
        //calculate iovec offset
        uint64_t front_offset = 0;
        segMgr_->ComputeSegOffsetFromId(segId_, front_offset);
        uint64_t back_offset = (front_offset + segSize_) - (ALIGNED_SIZE * keyAlignedNum_);

        //alloc iovec
        uint32_t iovec_num = keyNum_ * 2 + 1;
        uint32_t front_iovec_num = iovec_num - keyAlignedNum_;
        uint32_t back_iovec_num = keyAlignedNum_;
        ssize_t front_nwriten = 0;
        ssize_t back_nwriten = 0;
        uint32_t front_index = 0;
        uint32_t back_index = keyAlignedNum_ - 1;

        struct iovec *iov_front = new struct iovec[front_iovec_num];
        struct iovec *iov_back = new struct iovec[back_iovec_num];

        //aggregate SegmentOnDisk to iovec
        iov_front[front_index].iov_base = segOndisk_;
        iov_front[front_index].iov_len = SegmentManager::SizeOfSegOnDisk();
        front_index++;
        front_nwriten += SegmentManager::SizeOfSegOnDisk();

        //aggregate iovec
        for(list<Request *>::iterator iter=reqList_.begin(); iter != reqList_.end(); iter++)
        {
            KVSlice *slice = &(*iter)->GetSlice();
            DataHeader *header = &slice->GetHashEntry().GetEntryOnDisk().GetDataHeader();
            char *data = (char *)slice->GetData();
            uint16_t data_len = slice->GetDataLen();

            iov_front[front_index].iov_base = header;
            iov_front[front_index].iov_len = IndexManager::SizeOfDataHeader();
            front_index++;
            front_nwriten += IndexManager::SizeOfDataHeader();

            if (slice->IsAlignedData())
            {
                iov_back[back_index].iov_base = data;
                iov_back[back_index].iov_len = data_len;
                back_index--;
                back_nwriten += data_len;
            }
            else
            {
                iov_front[front_index].iov_base = data;
                iov_front[front_index].iov_len = data_len;
                front_index++;
                front_nwriten += data_len;
            }
        }

        //Write segment head, dataheader
        bool wstat = true;
        if (bdev_->pWritev(iov_front, front_iovec_num, front_offset) != front_nwriten)
        {
            __ERROR("Write Segment front data error, seg_id:%u", segId_);
            wstat = false;
        }

        if (bdev_->pWritev(iov_back, back_iovec_num, back_offset) != back_nwriten)
        {
            __ERROR("Write Segment front data error, seg_id:%u", segId_);
            wstat = false;
        }

        delete[] iov_front;
        delete[] iov_back;
        //notifyAndClean(wstat);

        return wstat;

    }

    bool SegmentSlice::_writeToDevice()
    {
        //calculate iovec offset
        uint64_t offset = 0;
        segMgr_->ComputeSegOffsetFromId(segId_, offset);

        //alloc iovec
        uint32_t iovec_num = keyNum_ * 2 + 2;
        uint32_t index = 0;
        uint32_t back_index = iovec_num - 1;

        struct iovec *iov = new struct iovec[iovec_num];
        iov[index].iov_base = segOndisk_;
        iov[index].iov_len = SegmentManager::SizeOfSegOnDisk();
        __DEBUG("Segment size %lu, index: %u, total: %u", iov[index].iov_len, index, iovec_num);
        index ++ ;

        //aggregate iovec
        for(list<Request *>::iterator iter=reqList_.begin(); iter != reqList_.end(); iter++)
        {
            KVSlice *slice = &(*iter)->GetSlice();
            DataHeader *header = &slice->GetHashEntry().GetEntryOnDisk().GetDataHeader();
            char *data = (char *)slice->GetData();
            uint16_t data_len = slice->GetDataLen();

            iov[index].iov_base = header;
            iov[index].iov_len = IndexManager::SizeOfDataHeader();
            __DEBUG("Dataheader size %lu, index: %u", iov[index].iov_len, index);
            index++;

            if (slice->IsAlignedData())
            {
                iov[back_index].iov_base = data;
                iov[back_index].iov_len = data_len;
                __DEBUG("Aligned value size %lu, index: %u", iov[back_index].iov_len, back_index);
                back_index--;
            }
            else
            {
                iov[index].iov_base = data;
                iov[index].iov_len = data_len;
                index++;
            }
        }

        ssize_t hole_size = tailPos_ - headPos_;
        char *hole = segMgr_->GetZeros();

        iov[index].iov_base = hole;
        iov[index].iov_len = hole_size;
        __DEBUG("hole size %lu, index:%u", iov[index].iov_len, index);

        //Write segment head, dataheader
        bool wstat = true;
        if (bdev_->pWritev(iov, iovec_num, offset) != segSize_)
        {
            __ERROR("Write Segment front data error, seg_id:%u", segId_);
            wstat = false;
        }


        delete[] iov;
        //notifyAndClean(wstat);

        return wstat;

    }

    bool SegmentSlice::_writeDataToDevice()
    {
        char *data_buff =  new char[segSize_];
        copyToData(data_buff);

        uint64_t offset = 0;
        segMgr_->ComputeSegOffsetFromId(segId_, offset);

        bool wstat = true;
        if (bdev_->pWrite(data_buff, segSize_, offset) != segSize_)
        {
            __ERROR("Write Segment front data error, seg_id:%u", segId_);
            wstat = false;
        }

        delete[] data_buff;
        return wstat;
    }

    bool SegmentSlice::WriteSegToDevice(uint32_t seg_id)
    {

        segId_ = seg_id;
        fillSlice();

        __DEBUG("Begin write seg, free size %u, seg id: %d, key num: %d", tailPos_-headPos_ , segId_, keyNum_);

        //return _writeToDevice();

        return _writeDataToDevice();

        //uint32_t hole_size = tailPos_ - headPos_;
        //if ( hole_size > (segSize_ >> 1))
        //{
        //    return _writeToDeviceHugeHole();
        //}
        //else
        //{
        //    return _writeToDevice();
        //}
    }

    void SegmentSlice::fillSlice()
    {
        uint32_t head_pos = SegmentManager::SizeOfSegOnDisk();
        uint32_t tail_pos = segSize_;
        for(list<Request *>::iterator iter=reqList_.begin(); iter != reqList_.end(); iter++)
        {

            KVSlice *slice = &(*iter)->GetSlice();
            slice->SetSegId(segId_);
            if (slice->IsAlignedData())
            {
                uint32_t data_offset = tail_pos - ALIGNED_SIZE;
                uint32_t next_offset = head_pos + IndexManager::SizeOfDataHeader();

                DataHeader data_header(slice->GetDigest(), slice->GetDataLen(), data_offset, next_offset);

                uint64_t seg_offset = 0;
                segMgr_->ComputeSegOffsetFromId(segId_, seg_offset);
                uint64_t header_offset = seg_offset + head_pos;

                HashEntry hash_entry(data_header, header_offset, NULL);
                slice->SetHashEntry(&hash_entry);

                head_pos += IndexManager::SizeOfDataHeader();
                tail_pos -= ALIGNED_SIZE;
                __DEBUG("SegmentSlice: key=%s, data_offset=%u, header_offset=%lu, seg_id=%u, head_pos=%u, tail_pos = %u", slice->GetKey(), data_offset, header_offset, segId_, head_pos, tail_pos);
            }
            else
            {
                uint32_t data_offset = head_pos + IndexManager::SizeOfDataHeader();
                uint32_t next_offset = head_pos + IndexManager::SizeOfDataHeader() + slice->GetDataLen();

                DataHeader data_header(slice->GetDigest(), slice->GetDataLen(), data_offset, next_offset);

                uint64_t seg_offset = 0;
                segMgr_->ComputeSegOffsetFromId(segId_, seg_offset);
                uint64_t header_offset = seg_offset + head_pos;

                HashEntry hash_entry(data_header, header_offset, NULL);
                slice->SetHashEntry(&hash_entry);

                head_pos += IndexManager::SizeOfDataHeader() + slice->GetDataLen();
                __DEBUG("SegmentSlice: key=%s, data_offset=%u, header_offset=%lu, seg_id=%u, head_pos=%u, tail_pos = %u", slice->GetKey(), data_offset, header_offset, segId_, head_pos, tail_pos);

            }
        }
        if (head_pos != headPos_ || tail_pos != tailPos_)
        {
            __ERROR("Segment fillSlice  Failed!!! head_pos= %u, headPos_=%u, tail_pos= %u, tailPos_ = %u", head_pos, headPos_, tail_pos, tailPos_);
        }
    }

    void SegmentSlice::notifyAndClean(bool req_state)
    {
        //Set logic timestamp to hashentry
        reqCommited_.store(keyNum_);

        int32_t keyNo = 0;
        persistTime_.Update();
        for(list<Request *>::iterator iter=reqList_.begin(); iter != reqList_.end(); iter++)
        {
            keyNo ++;
            KVSlice *slice = &(*iter)->GetSlice();
            HashEntry &entry = slice->GetHashEntry();
            entry.SetLogicStamp(persistTime_, keyNo);

            if (!slice->GetData())
            {
                delReqList_.push_back(entry);
            }
        }

        //notify and clean req.
        while (!reqList_.empty())
        {
            Request *req = reqList_.front();
            reqList_.pop_front();
            req->Done();
            req->SetState(req_state);
            req->Signal();
        }

        isCanReap_.store(true);
    }


    void SegmentSlice::Complete()
    {
        //std::lock_guard<std::mutex> l(mtx_);
        if (isCompleted_)
        {
            return;
        }

        fillSegHead();
        isCompleted_ = true;
    }

    //bool SegmentSlice::CompleteIfExpired()
    //{
    //    //std::lock_guard<std::mutex> l(mtx_);
    //    if (isCompleted_)
    //    {
    //        return true;
    //    }
    //    if (!isExpire())
    //    {
    //        return false;
    //    }

    //    fillSegHead();
    //    isCompleted_ = true;
    //    return true;
    //}


    void SegmentSlice::Notify(bool stat)
    {
        notifyAndClean(stat);
    }

    bool SegmentSlice::IsExpired()
    {
        return isExpire();
    }

    void SegmentSlice::WaitForReap()
    {
        while(!isCanReap_.load())
        {
            usleep(100);
        }
    }

    bool SegmentSlice::isExpire()
    {
        KVTime nowTime;
        if (!hasReq_)
        {
            return false;
        }
        int64_t interval = nowTime - startTime_;
        return (interval > EXPIRED_TIME);
    }

    bool SegmentSlice::isCanFit(Request* req) const
    {
        KVSlice *slice = &req->GetSlice();
        uint32_t freeSize = tailPos_ - headPos_;
        uint32_t needSize = slice->GetDataLen() + IndexManager::SizeOfDataHeader();
        return freeSize > needSize;
    }

    void SegmentSlice::copyToData(char* data_buff)
    {
        uint64_t offset = 0;
        segMgr_->ComputeSegOffsetFromId(segId_, offset);

        uint32_t offset_begin = 0;
        uint32_t offset_end = segSize_;

        memcpy(data_buff, segOndisk_, SegmentManager::SizeOfSegOnDisk());
        offset_begin += SegmentManager::SizeOfSegOnDisk();

        //aggregate iovec
        for(list<Request *>::iterator iter=reqList_.begin(); iter != reqList_.end(); iter++)
        {
            KVSlice *slice = &(*iter)->GetSlice();
            DataHeader *header = &slice->GetHashEntry().GetEntryOnDisk().GetDataHeader();
            char *data = (char *)slice->GetData();
            uint16_t data_len = slice->GetDataLen();

            memcpy(&(data_buff[offset_begin]), header, IndexManager::SizeOfDataHeader());
            offset_begin += IndexManager::SizeOfDataHeader();

            if (slice->IsAlignedData())
            {
                offset_end -= data_len;
                memcpy(&(data_buff[offset_end]), data, data_len);
                __DEBUG("write key = %s, data position: %lu", slice->GetKey(), offset_end + offset);
            }
            else
            {
                memcpy(&(data_buff[offset_begin]), data, data_len);
                offset_begin += data_len;
                __DEBUG("write key = %s, data position: %lu", slice->GetKey(), offset_begin + offset);
            }
        }

    }

    void SegmentSlice::fillSegHead()
    {
        segOndisk_->SetKeyNum(keyNum_);
    }

}
