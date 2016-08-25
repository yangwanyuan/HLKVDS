#include <sys/uio.h>
#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <errno.h>
#include <inttypes.h>

#include "DataHandle.h"

namespace kvdb{

    SegmentSlice::SegmentSlice(uint32_t seg_id, SegmentManager* sm)
        : m_id(seg_id), m_sm(sm)
    {
        m_seg_size = m_sm->GetSegmentSize();
        m_data = new char[m_seg_size];
        //memset(m_data, 0, m_seg_size);
        m_len = 0;
    }

    bool SegmentSlice::Put(DataHeader& header, const char* data, uint16_t length)
    {
        SegmentOnDisk seg_ondisk;
        m_len = sizeof(SegmentOnDisk) + sizeof(DataHeader) + length;
        memcpy(m_data, &seg_ondisk, sizeof(SegmentOnDisk));
        memcpy(&m_data[sizeof(SegmentOnDisk)], &header, sizeof(DataHeader));
        memcpy(&m_data[sizeof(SegmentOnDisk) + sizeof(DataHeader)], data, length);

        return true;
    }

    SegmentSlice::~SegmentSlice()
    {
        if(m_data)
        {
            delete[] m_data;
        }
    }


    KVSlice::KVSlice()
        : m_key(NULL), m_keyLen(0), m_data(NULL), m_dataLen(0), m_digest(NULL), m_Iscomputed(false) {}

    KVSlice::~KVSlice()
    {
        if (m_digest)
        {
            delete m_digest;
        }
    }

    KVSlice::KVSlice(const KVSlice& toBeCopied)
        : m_key(NULL), m_keyLen(0), m_data(NULL), m_dataLen(0), m_digest(NULL), m_Iscomputed(false)
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
        m_keyLen = toBeCopied.GetKeyLen();
        m_dataLen = toBeCopied.GetDataLen();
        m_key = toBeCopied.GetKey();
        m_data = toBeCopied.GetData();
        *m_digest = toBeCopied.GetDigest();
        m_Iscomputed = toBeCopied.IsDigestComputed();
    }

    KVSlice::KVSlice(const char* key, int key_len, const char* data, int data_len)
        : m_key(key), m_keyLen(key_len), m_data(data), m_dataLen(data_len), m_digest(NULL), m_Iscomputed(false){}

    void KVSlice::SetKeyValue(const char* key, int key_len, const char* data, int data_len)
    {
        m_Iscomputed = false;
        m_keyLen = key_len;
        m_dataLen = data_len;
        m_key = key;
        m_data = data;
    }

    bool KVSlice::ComputeDigest()
    {
        if (!m_key)
        {
            return false;
        }
        m_digest = new Kvdb_Digest();
        Kvdb_Key vkey(m_key, m_keyLen);
        KeyDigestHandle::ComputeDigest(&vkey, *m_digest);

        m_Iscomputed = true;

        return true;
    }

    string KVSlice::GetKeyStr() const
    {
        char * data = new char[m_keyLen + 1];
        memcpy(data, m_key, m_keyLen);
        data[m_keyLen] = '\0';
        string r(data);
        delete[] data;
        return r;
    }

    string KVSlice::GetDataStr() const
    {
        char * data = new char[m_dataLen + 1];
        memcpy(data, m_data, m_dataLen);
        data[m_dataLen] = '\0';
        string r(data);
        delete[] data;
        return r;
    }

    Request::Request(): m_done(0), m_write_stat(false), m_slice(NULL)
    {
        m_mutex = new Mutex;
        m_cond = new Cond(*m_mutex);
    }

    Request::~Request()
    {
        delete m_cond;
        delete m_mutex;
    }

    Request::Request(const Request& toBeCopied)
        : m_done(toBeCopied.m_done), m_write_stat(toBeCopied.m_write_stat), m_slice(toBeCopied.m_slice)
    {
        m_mutex = new Mutex;
        m_cond = new Cond(*m_mutex);
    }

    Request& Request::operator=(const Request& toBeCopied)
    {
        m_done = toBeCopied.m_done;
        m_write_stat = toBeCopied.m_write_stat;
        m_slice = toBeCopied.m_slice;
        m_mutex = new Mutex;
        m_cond = new Cond(*m_mutex);
        return *this;
    }

    Request::Request(KVSlice& slice) : m_done(0), m_write_stat(false), m_slice(&slice)
    {
        m_mutex = new Mutex;
        m_cond = new Cond(*m_mutex);
    }


    void Request::Done()
    {
        m_mutex->Lock();
        m_done = 1;
        m_mutex->Unlock();
    }

    void Request::SetState(bool state)
    {
        m_mutex->Lock();
        m_write_stat = state;
        m_mutex->Unlock();
    }

    void Request::Wait()
    {
        m_mutex->Lock();
        m_cond->Wait();
        m_mutex->Unlock();
    }

    void Request::Signal()
    {
        m_mutex->Lock();
        m_cond->Signal();
        m_mutex->Unlock();
    }

    SegmentData::SegmentData()
        :segId_(0), sm_(NULL), segSize_(0), creTime_(KVTime()),
        headPos_(0), tailPos_(0), keyNum_(0), data_(NULL),
        completed_(false){}

    SegmentData::~SegmentData()
    {
        if(data_)
        {
            delete data_;
        }
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
        sm_ = toBeCopied.sm_;
        segSize_ = toBeCopied.segSize_;
        creTime_ = toBeCopied.creTime_;
        headPos_ = toBeCopied.headPos_;
        tailPos_ = toBeCopied.tailPos_;
        keyNum_ = toBeCopied.keyNum_;
        completed_ = toBeCopied.completed_;
        data_ = new char[segSize_];
        memcpy(data_, toBeCopied.data_, segSize_);
    }

    SegmentData::SegmentData(uint32_t seg_id, SegmentManager* sm)
        : segId_(seg_id), sm_(sm), segSize_(sm_->GetSegmentSize()),
        creTime_(KVTime()), headPos_(sizeof(SegmentOnDisk)),
        tailPos_(segSize_), keyNum_(0), completed_(false)
    {
        data_ = new char[segSize_];
    }

    bool SegmentData::IsCanWrite(KVSlice *slice) const
    {
        if (isExpire())
        {
            return false;
        }
        return isCanFit(slice);
    }

    bool SegmentData::Put(KVSlice *slice, DataHeader &header)
    {
        if (!isCanFit(slice))
        {
            return false;
        }
        if (slice->Is4KData())
        {
            put4K(slice, header);
        }
        else
        {
            putNon4K(slice, header);
        }
        return true;
    }

    const char* SegmentData::GetCompleteSeg() const
    {
        if (!completed_)
        {
            return NULL;
        }
        return data_;
    }

    void SegmentData::Complete()
    {
        fillSegHead();
        completed_ = true;
    }

    bool SegmentData::isExpire() const
    {
        KVTime nowTime;
        double interval = nowTime - creTime_;
        return (interval > EXPIRED_TIME);
    }

    bool SegmentData::isCanFit(KVSlice *slice) const
    {
        uint32_t freeSize = tailPos_ - headPos_;
        uint32_t needSize = slice->GetDataLen() + sizeof(DataHeader);
        return freeSize > needSize;
    }

    void SegmentData::put4K(KVSlice *slice, DataHeader &header)
    {
        memcpy(&data_[headPos_], &header, sizeof(DataHeader));
        memcpy(&data_[tailPos_ - 4096], slice->GetData(), 4096);
        headPos_ += sizeof(DataHeader);
        tailPos_ -= 4096;
    }

    void SegmentData::putNon4K(KVSlice *slice, DataHeader &header)
    {
        memcpy(&data_[headPos_], &header, sizeof(DataHeader));
        headPos_ += sizeof(DataHeader);
        memcpy(&data_[headPos_], slice->GetData(), slice->GetDataLen());
        headPos_ += slice->GetDataLen();
    }

    void SegmentData::fillSegHead()
    {
        SegmentOnDisk seg(keyNum_);
        memcpy(data_, &seg, sizeof(SegmentOnDisk));
    }
////////////////////////////////////////////////////////////////////////////////
    bool DataHandle::ReadData(HashEntry* entry, string &data)
    {
        uint16_t data_len = entry->GetDataSize();
        if (data_len == 0)
        { 
            return true;
        }

        uint64_t data_offset = 0;
        if (!m_sm->ComputeDataOffsetPhyFromEntry(entry, data_offset))
        {
            return false;
        }

        char *mdata = new char[data_len];
        if (m_bdev->pRead(mdata, data_len, data_offset) != (ssize_t)data_len)
        {
            __ERROR("Could not read data at position");
            delete[] mdata;
            return false; 
        }
        data.assign(mdata, data_len);
        delete[] mdata;

        __DEBUG("get data offset %ld", data_offset);

        return true;
    }
    
    //bool DataHandle::WriteData(const Kvdb_Digest& digest, const char* data, uint16_t length)
    bool DataHandle::WriteData(const KVSlice *slice)
    {
        const Kvdb_Digest* digest = &slice->GetDigest();
        const char* data = slice->GetData();
        uint16_t length = slice->GetDataLen();

        uint32_t seg_id = 0;
        if (!m_sm->GetEmptySegId(seg_id))
        {
            return false;
        }

        uint64_t seg_offset; 
        m_sm->ComputeSegOffsetFromId(seg_id, seg_offset);

        uint32_t head_offset = sizeof(SegmentOnDisk);
        uint32_t data_offset = head_offset + sizeof(DataHeader);
        uint32_t next_offset = data_offset + length;

        SegmentOnDisk seg_ondisk(1);
        DataHeader data_header(*digest, length, data_offset, next_offset);

        //---Write data to device 3:
        SegmentSlice segSlice(seg_id, m_sm);
        segSlice.Put(data_header, data, length);

        if (m_bdev->pWrite(segSlice.GetSlice(), segSlice.GetLength(), seg_offset) != segSlice.GetLength()) {
            __ERROR("Could  write data to device: %s\n", strerror(errno));
            return false;
        }
        //---Write data to device 3 end


        m_sm->Update(seg_id);
        m_im->UpdateIndexFromInsert(&data_header, digest, head_offset, seg_offset);
        DBSuperBlock sb = m_sbm->GetSuperBlock();
        sb.current_segment = seg_id;
        m_sbm->SetSuperBlock(sb);


        __DEBUG("write seg_id:%d, seg_offset: %ld, head_offset: %d, data_offset:%d, header_len:%d, data_len:%d", 
                seg_id, seg_offset, head_offset, data_offset, (uint32_t)sizeof(DataHeader), (uint32_t)length );

        return true;

    }
    

    DataHandle::DataHandle(BlockDevice* bdev, SuperBlockManager* sbm, IndexManager* im, SegmentManager* sm):
        m_bdev(bdev), m_sbm(sbm), m_im(im), m_sm(sm){}

    DataHandle::~DataHandle(){}


}

