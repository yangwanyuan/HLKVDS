#include <sys/uio.h>
#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <errno.h>
#include <inttypes.h>

#include "DataHandle.h"

namespace kvdb{

    KVSlice::KVSlice()
        : key_(NULL), keyLength_(0), data_(NULL), dataLength_(0), digest_(NULL), isComputed_(false), header_(NULL), segId_(0), hasHeader_(false){}

    KVSlice::~KVSlice()
    {
        if (digest_)
        {
            delete digest_;
        }
        if (header_)
        {
            delete header_;
        }
    }

    KVSlice::KVSlice(const KVSlice& toBeCopied)
        : key_(NULL), keyLength_(0), data_(NULL), dataLength_(0), digest_(NULL), isComputed_(false), header_(NULL), segId_(0), hasHeader_(false)
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
        digest_ = new Kvdb_Digest(toBeCopied.GetDigest());
        isComputed_ = toBeCopied.IsDigestComputed();
        header_ = new DataHeader(toBeCopied.GetDataHeader());
        segId_ = toBeCopied.segId_;
        hasHeader_ = toBeCopied.hasHeader_;
    }

    KVSlice::KVSlice(const char* key, int key_len, const char* data, int data_len)
        : key_(key), keyLength_(key_len), data_(data), dataLength_(data_len), digest_(NULL), isComputed_(false), header_(NULL), segId_(0), hasHeader_(false){}

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

    void KVSlice::SetDataHeader(const DataHeader* data_header)
    {
        header_ = new DataHeader(*data_header); 
        hasHeader_ = true;
    }

    void KVSlice::SetSegId(uint32_t seg_id)
    {
        segId_ = seg_id;
    }

    Request::Request(): isDone_(0), writeStat_(false), slice_(NULL)
    {
        mtx_ = new Mutex;
        cond_ = new Cond(*mtx_);
    }

    Request::~Request()
    {
        delete cond_;
        delete mtx_;
    }

    Request::Request(const Request& toBeCopied)
        : isDone_(toBeCopied.isDone_),
            writeStat_(toBeCopied.writeStat_),
            slice_(toBeCopied.slice_)
    {
        mtx_ = new Mutex;
        cond_ = new Cond(*mtx_);
    }

    Request& Request::operator=(const Request& toBeCopied)
    {
        isDone_ = toBeCopied.isDone_;
        writeStat_ = toBeCopied.writeStat_;
        slice_ = toBeCopied.slice_;
        mtx_ = new Mutex;
        cond_ = new Cond(*mtx_);
        return *this;
    }

    Request::Request(KVSlice& slice) : isDone_(0), writeStat_(false), slice_(&slice)
    {
        mtx_ = new Mutex;
        cond_ = new Cond(*mtx_);
    }


    void Request::Done()
    {
        mtx_->Lock();
        isDone_ = 1;
        mtx_->Unlock();
    }

    void Request::SetState(bool state)
    {
        mtx_->Lock();
        writeStat_ = state;
        mtx_->Unlock();
    }

    void Request::Wait()
    {
        mtx_->Lock();
        cond_->Wait();
        mtx_->Unlock();
    }

    void Request::Signal()
    {
        mtx_->Lock();
        cond_->Signal();
        mtx_->Unlock();
    }

    SegmentSlice::SegmentSlice()
        :segId_(0), segMgr_(NULL), bdev_(NULL), segSize_(0),
        creTime_(KVTime()), headPos_(0), tailPos_(0), keyNum_(0),
        key4KNum_(0), isCompleted_(false), segOndisk_(NULL)
    {
        mtx_ = new Mutex;
        segOndisk_ = new SegmentOnDisk;
    }

    SegmentSlice::~SegmentSlice()

    {
        delete mtx_;
        delete segOndisk_;
    }

    SegmentSlice::SegmentSlice(const SegmentSlice& toBeCopied)
    {
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
        creTime_ = toBeCopied.creTime_;
        headPos_ = toBeCopied.headPos_;
        tailPos_ = toBeCopied.tailPos_;
        keyNum_ = toBeCopied.keyNum_;
        key4KNum_ = toBeCopied.key4KNum_;
        isCompleted_ = toBeCopied.isCompleted_;
        mtx_ = new Mutex;
        reqList_ = toBeCopied.reqList_;
        segOndisk_ = new SegmentOnDisk(*toBeCopied.segOndisk_);
    }

    SegmentSlice::SegmentSlice(uint32_t seg_id, SegmentManager* sm, BlockDevice* bdev)
        : segId_(seg_id), segMgr_(sm), bdev_(bdev),
        segSize_(segMgr_->GetSegmentSize()), creTime_(KVTime()),
        headPos_(sizeof(SegmentOnDisk)), tailPos_(segSize_),
        keyNum_(0), key4KNum_(0), isCompleted_(false), segOndisk_(NULL)
    {
        mtx_ = new Mutex;
        segOndisk_ = new SegmentOnDisk;
    }

    bool SegmentSlice::IsCanWrite(Request* req) const
    {
        if (IsCompleted())
        {
            return false;
        }
        if (isExpire())
        {
            return false;
        }
        return isCanFit(req);
    }

    bool SegmentSlice::Put(Request* req)
    {
        const KVSlice *slice = &req->GetSlice();
        if (!isCanFit(req))
        {
            return false;
        }
        if (slice->Is4KData())
        {
            put4K(req);
        }
        else
        {
            putNon4K(req);
        }
        req->GetSlice().SetSegId(segId_);
        return true;
    }

    void SegmentSlice::WriteSegToDevice()
    {
        //calculate iovec offset
        uint64_t front_offset = 0;
        segMgr_->ComputeSegOffsetFromId(segId_, front_offset);
        uint64_t back_offset = (front_offset + segSize_) - (SIZE_4K * key4KNum_);

        //alloc iovec
        uint32_t iovec_num = keyNum_ * 2 + 1;
        uint32_t front_iovec_num = iovec_num - key4KNum_;
        uint32_t back_iovec_num = key4KNum_;
        ssize_t front_nwriten = 0;
        ssize_t back_nwriten = 0;
        uint32_t front_index = 0;
        uint32_t back_index = key4KNum_ - 1;

        struct iovec *iov_front = new struct iovec[front_iovec_num];
        struct iovec *iov_back = new struct iovec[back_iovec_num];

        //aggregate SegmentOnDisk to iovec
        iov_front[front_index].iov_base = segOndisk_;
        iov_front[front_index].iov_len = sizeof(SegmentOnDisk);
        front_index++;
        front_nwriten += sizeof(SegmentOnDisk);

        //aggregate iovec
        for(list<Request *>::iterator iter=reqList_.begin(); iter != reqList_.end(); iter++)
        {
            const KVSlice *slice = &(*iter)->GetSlice();
            DataHeader *header = &slice->GetDataHeader();
            char *data = (char *)slice->GetData();
            uint16_t data_len = slice->GetDataLen();

            iov_front[front_index].iov_base = header;
            iov_front[front_index].iov_len = sizeof(DataHeader);
            front_index++;
            front_nwriten += sizeof(DataHeader);

            if (slice->Is4KData())
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
        notifyAndClean(wstat);
    }

    void SegmentSlice::notifyAndClean(bool req_state)
    {
        while (!reqList_.empty())
        {
            Request *req = reqList_.front();
            req->Done();
            req->SetState(req_state);
            req->Signal();
            reqList_.pop_front();
        }
    }


    uint64_t SegmentSlice::GetSegPhyOffset() const
    {
        uint64_t offset;
        segMgr_->ComputeSegOffsetFromId(segId_, offset);
        return offset;
    }

    void SegmentSlice::Complete()
    {
        fillSegHead();
        isCompleted_ = true;
    }

    bool SegmentSlice::isExpire() const
    {
        KVTime nowTime;
        //double interval = nowTime - creTime_;
        int64_t interval = nowTime - creTime_;
        return (interval > EXPIRED_TIME);
    }

    bool SegmentSlice::isCanFit(Request* req) const
    {
        const KVSlice *slice = &req->GetSlice();
        uint32_t freeSize = tailPos_ - headPos_;
        uint32_t needSize = slice->GetDataLen() + sizeof(DataHeader);
        return freeSize > needSize;
    }

    void SegmentSlice::put4K(Request* req)
    {
        const KVSlice *slice = &req->GetSlice();

        uint32_t data_offset = tailPos_ - SIZE_4K;
        uint32_t next_offset = headPos_ + sizeof(DataHeader);

        DataHeader data_header(slice->GetDigest(), slice->GetDataLen(), data_offset, next_offset);
        req->GetSlice().SetDataHeader(&data_header);

        headPos_ += sizeof(DataHeader);
        tailPos_ -= SIZE_4K;
        keyNum_++;
        reqList_.push_back(req);
        key4KNum_++;
    }

    void SegmentSlice::putNon4K(Request* req)
    {
        const KVSlice *slice = &req->GetSlice();

        uint32_t data_offset = headPos_ + sizeof(DataHeader);
        uint32_t next_offset = headPos_ + sizeof(DataHeader) + slice->GetDataLen();

        DataHeader data_header(slice->GetDigest(), slice->GetDataLen(), data_offset, next_offset);
        req->GetSlice().SetDataHeader(&data_header);

        headPos_ += sizeof(DataHeader) + slice->GetDataLen();
        keyNum_++;
        reqList_.push_back(req);
    }

    void SegmentSlice::fillSegHead()
    {
        segOndisk_->SetKeyNum(keyNum_);
    }
}

