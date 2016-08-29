#include <sys/uio.h>
#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <errno.h>
#include <inttypes.h>

#include "DataHandle.h"

namespace kvdb{

    SegmentSlice::SegmentSlice(uint32_t seg_id, SegmentManager* sm)
        : id_(seg_id), segMgr_(sm)
    {
        segSize_ = segMgr_->GetSegmentSize();
        data_ = new char[segSize_];
        //memset(data_, 0, segSize_);
        length_ = 0;
    }

    bool SegmentSlice::Put(const KVSlice *slice, DataHeader& header)
    {
        uint64_t seg_offset;
        segMgr_->ComputeSegOffsetFromId(id_, seg_offset);

        uint32_t head_offset = sizeof(SegmentOnDisk);
        uint32_t data_offset = head_offset + sizeof(DataHeader);
        uint32_t next_offset = data_offset + slice->GetDataLen();

        header.SetDigest(slice->GetDigest());
        header.SetDataSize(slice->GetDataLen());
        header.SetDataOffset(data_offset);
        header.SetNextHeadOffset(next_offset);

        SegmentOnDisk seg_ondisk;
        length_ = sizeof(SegmentOnDisk) + sizeof(DataHeader) + slice->GetDataLen();
        memcpy(data_, &seg_ondisk, sizeof(SegmentOnDisk));
        memcpy(&data_[sizeof(SegmentOnDisk)], &header, sizeof(DataHeader));
        memcpy(&data_[sizeof(SegmentOnDisk) + sizeof(DataHeader)], slice->GetData(), slice->GetDataLen());

        return true;
    }


    SegmentSlice::~SegmentSlice()
    {
        if(data_)
        {
            delete[] data_;
        }
    }


    KVSlice::KVSlice()
        : key_(NULL), keyLength_(0), data_(NULL), dataLength_(0), digest_(NULL), isComputed_(false) {}

    KVSlice::~KVSlice()
    {
        if (digest_)
        {
            delete digest_;
        }
    }

    KVSlice::KVSlice(const KVSlice& toBeCopied)
        : key_(NULL), keyLength_(0), data_(NULL), dataLength_(0), digest_(NULL), isComputed_(false)
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
        *digest_ = toBeCopied.GetDigest();
        isComputed_ = toBeCopied.IsDigestComputed();
    }

    KVSlice::KVSlice(const char* key, int key_len, const char* data, int data_len)
        : key_(key), keyLength_(key_len), data_(data), dataLength_(data_len), digest_(NULL), isComputed_(false){}

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

    SegmentData::SegmentData()
        :segId_(0), segMgr_(NULL), bdev_(NULL), segSize_(0),
        creTime_(KVTime()), headPos_(0), tailPos_(0), keyNum_(0),
        isCompleted_(false), segOndisk_(NULL)
    {
        mtx_ = new Mutex;
        cond_ = new Cond(*mtx_);
        segOndisk_ = new SegmentOnDisk;
    }

    SegmentData::~SegmentData()

    {
        delete cond_;
        delete mtx_;
        delete segOndisk_;
    }

    SegmentData::SegmentData(const SegmentData& toBeCopied)
    {
        copyHelper(toBeCopied);
    }

    SegmentData& SegmentData::operator=(const SegmentData& toBeCopied)
    {
        copyHelper(toBeCopied);
        return *this;
    }

    void SegmentData::copyHelper(const SegmentData& toBeCopied)
    {
        segId_ = toBeCopied.segId_;
        segMgr_ = toBeCopied.segMgr_;
        bdev_ = toBeCopied.bdev_;
        segSize_ = toBeCopied.segSize_;
        creTime_ = toBeCopied.creTime_;
        headPos_ = toBeCopied.headPos_;
        tailPos_ = toBeCopied.tailPos_;
        keyNum_ = toBeCopied.keyNum_;
        isCompleted_ = toBeCopied.isCompleted_;
        mtx_ = new Mutex;
        cond_ = new Cond(*mtx_);
        reqList_ = toBeCopied.reqList_;
        segOndisk_ = new SegmentOnDisk(*toBeCopied.segOndisk_);
    }

    SegmentData::SegmentData(uint32_t seg_id, SegmentManager* sm, BlockDevice* bdev)
        : segId_(seg_id), segMgr_(sm), bdev_(bdev),
        segSize_(segMgr_->GetSegmentSize()), creTime_(KVTime()),
        headPos_(sizeof(SegmentOnDisk)), tailPos_(segSize_),
        keyNum_(0), isCompleted_(false), segOndisk_(NULL)
    {
        mtx_ = new Mutex;
        cond_ = new Cond(*mtx_);
        segOndisk_ = new SegmentOnDisk;
    }

    bool SegmentData::IsCanWrite(Request* req) const
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

    bool SegmentData::Put(Request* req, DataHeader &header)
    {
        const KVSlice *slice = &req->GetSlice();
        if (!isCanFit(req))
        {
            return false;
        }
        if (slice->Is4KData())
        {
            put4K(req, header);
        }
        else
        {
            putNon4K(req, header);
        }
        return true;
    }

    void SegmentData::WriteSegToDevice()
    {
        uint64_t seg_offset = 0;
        segMgr_->ComputeSegOffsetFromId(segId_, seg_offset);
        bdev_->pWrite(segOndisk_, sizeof(SegmentOnDisk), seg_offset);
        uint32_t head_pos = sizeof(SegmentOnDisk);
        uint32_t tail_pos = segSize_;
        for(list<Request *>::iterator iter=reqList_.begin(); iter != reqList_.end(); iter++)
        {
            const KVSlice *slice = &(*iter)->GetSlice();
            if (slice->Is4KData())
            {
                uint32_t data_offset = tail_pos - 4096;
                uint32_t next_offset = head_pos + sizeof(DataHeader);

                DataHeader header(slice->GetDigest(), slice->GetDataLen(), data_offset, next_offset);

                bdev_->pWrite(&header, sizeof(DataHeader), seg_offset+head_pos);
                head_pos += sizeof(DataHeader);
                tail_pos -= 4096;
                bdev_->pWrite(slice->GetData(), slice->GetDataLen(), seg_offset+tail_pos);
            }
            else
            {
                uint32_t data_offset = head_pos + sizeof(DataHeader);
                uint32_t next_offset = data_offset + slice->GetDataLen();

                DataHeader header(slice->GetDigest(), slice->GetDataLen(), data_offset, next_offset);

                bdev_->pWrite(&header, sizeof(DataHeader), seg_offset+head_pos);
                head_pos += sizeof(DataHeader);
                bdev_->pWrite(slice->GetData(), slice->GetDataLen(), seg_offset+head_pos);
                head_pos += slice->GetDataLen();
            }

            (*iter)->Done();
            (*iter)->SetState(true);
            (*iter)->Signal();
        }
        while (!reqList_.empty())
        {
            reqList_.pop_front();
        }
    }

    uint64_t SegmentData::GetSegPhyOffset() const
    {
        uint64_t offset;
        segMgr_->ComputeSegOffsetFromId(segId_, offset);
        return offset;
    }

    void SegmentData::Complete()
    {
        fillSegHead();
        isCompleted_ = true;
    }

    bool SegmentData::isExpire() const
    {
        KVTime nowTime;
        double interval = nowTime - creTime_;
        return (interval > EXPIRED_TIME);
    }

    bool SegmentData::isCanFit(Request* req) const
    {
        const KVSlice *slice = &req->GetSlice();
        uint32_t freeSize = tailPos_ - headPos_;
        uint32_t needSize = slice->GetDataLen() + sizeof(DataHeader);
        return freeSize > needSize;
    }

    void SegmentData::put4K(Request* req, DataHeader &header)
    {
        const KVSlice *slice = &req->GetSlice();

        uint32_t data_offset = tailPos_ - 4096;
        uint32_t next_offset = headPos_ + sizeof(DataHeader);

        header.SetDigest(slice->GetDigest());
        header.SetDataSize(slice->GetDataLen());
        header.SetDataOffset(data_offset);
        header.SetNextHeadOffset(next_offset);

        headPos_ += sizeof(DataHeader);
        tailPos_ -= 4096;
        keyNum_++;
        reqList_.push_back(req);
    }

    void SegmentData::putNon4K(Request* req, DataHeader &header)
    {
        const KVSlice *slice = &req->GetSlice();

        uint32_t data_offset = headPos_ + sizeof(DataHeader);
        uint32_t next_offset = headPos_ + sizeof(DataHeader) + slice->GetDataLen();

        header.SetDigest(slice->GetDigest());
        header.SetDataSize(slice->GetDataLen());
        header.SetDataOffset(data_offset);
        header.SetNextHeadOffset(next_offset);

        headPos_ += sizeof(DataHeader) + slice->GetDataLen();
        keyNum_++;
        reqList_.push_back(req);
    }

    void SegmentData::fillSegHead()
    {
        segOndisk_->SetKeyNum(keyNum_);
    }


}

