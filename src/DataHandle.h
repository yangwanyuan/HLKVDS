#ifndef _KV_DB_DATAHANDLE_H_
#define _KV_DB_DATAHANDLE_H_

#include <string>
#include <sys/types.h>

#include <list>

#include "Db_Structure.h"
#include "BlockDevice.h"
#include "KeyDigestHandle.h"
#include "IndexManager.h"
#include "SuperBlockManager.h"
#include "SegmentManager.h"

using namespace std;

namespace kvdb{

    class SegmentOnDisk;
    class DataHeader;
    class HashEntry;
    class IndexManager;
    class SegmentManager;
    class KVSlice;

    class SegmentSlice{
    public:
        SegmentSlice();
        ~SegmentSlice();
        SegmentSlice(const SegmentSlice& toBeCopied);
        SegmentSlice& operator=(const SegmentSlice& toBeCopied);

        SegmentSlice(uint32_t seg_id, SegmentManager* sm);

        bool Put(const KVSlice *slice, DataHeader& header);
        const void* GetSlice() const { return data_; }
        uint32_t GetLength() const { return length_; }
    private:
        uint32_t id_;
        SegmentManager* segMgr_;
        uint32_t segSize_;
        char* data_;
        uint32_t length_;
    };

    class KVSlice{
    public:
        KVSlice();
        ~KVSlice();
        KVSlice(const KVSlice& toBeCopied);
        KVSlice& operator=(const KVSlice& toBeCopied);

        KVSlice(const char* key, int key_len, const char* data, int data_len);

        const Kvdb_Digest& GetDigest() const { return *digest_; }
        const char* GetKey() const { return key_; }
        const char* GetData() const { return data_; }
        string GetKeyStr() const;
        string GetDataStr() const;
        uint32_t GetKeyLen() const { return keyLength_; }
        uint16_t GetDataLen() const { return dataLength_; }
        bool IsDigestComputed() const { return isComputed_; }
        bool Is4KData() const{ return GetDataLen() == 4096; }

        void SetKeyValue(const char* key, int key_len, const char* data, int data_len);
        bool ComputeDigest();

    private:
        const char* key_;
        uint32_t keyLength_;
        const char* data_;
        uint16_t dataLength_;
        Kvdb_Digest *digest_;
        bool isComputed_;

        void copy_helper(const KVSlice& toBeCopied);
    };

    class Request{
    public:
        Request();
        ~Request();
        Request(const Request& toBeCopied);
        Request& operator=(const Request& toBeCopied);
        Request(KVSlice& slice);

        const KVSlice& GetSlice() const { return *slice_; }
        int IsDone() const { return isDone_; }
        void Done();

        void SetState(bool state);
        bool GetState() const { return writeStat_; }

        void Wait();
        void Signal();

    private:
        int isDone_;
        bool writeStat_;
        KVSlice *slice_;
        //DataHeader header_;
        Mutex *mtx_;
        Cond *cond_;

    };

    class SegmentData{
    public:
        SegmentData();
        ~SegmentData();
        SegmentData(const SegmentData& toBeCopied);
        SegmentData& operator=(const SegmentData& toBeCopied);

        SegmentData(uint32_t seg_id, SegmentManager* sm, BlockDevice* bdev);

        bool IsCanWrite(Request* req) const;
        bool Put(Request* req, DataHeader& header);
        void WriteSegToDevice();
        uint64_t GetSegPhyOffset() const;
        uint32_t GetSegSize() const { return segSize_; }
        uint32_t GetSegId() const { return segId_; }
        void Complete();
        bool IsCompleted() const { return isCompleted_;};
        bool IsExpired() const { return isExpire(); }

    private:
        bool isExpire() const;
        bool isCanFit(Request* req) const;
        void put4K(Request* req, DataHeader& header);
        void putNon4K(Request* req, DataHeader& header);
        void copyHelper(const SegmentData& toBeCopied);
        void fillSegHead();

        uint32_t segId_;
        SegmentManager* segMgr_;
        BlockDevice* bdev_;
        uint32_t segSize_;
        KVTime creTime_;

        uint32_t headPos_;
        uint32_t tailPos_;

        uint32_t keyNum_;
        bool isCompleted_;

        Mutex *mtx_;
        Cond *cond_;

        std::list<Request *> reqList_;
        SegmentOnDisk *segOndisk_;

    };

} //end namespace kvdb


#endif //#ifndef _KV_DB_DATAHANDLE_H_
