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
        bool Is4KData() const{ return GetDataLen() == SIZE_4K; }
        DataHeader& GetDataHeader() const { return *header_; }
        uint32_t GetSegId() const { return segId_; }

        void SetKeyValue(const char* key, int key_len, const char* data, int data_len);
        bool ComputeDigest();
        void SetDataHeader(const DataHeader *data_header);
        void SetSegId(uint32_t seg_id);

    private:
        const char* key_;
        uint32_t keyLength_;
        const char* data_;
        uint16_t dataLength_;
        Kvdb_Digest *digest_;
        bool isComputed_;
        DataHeader *header_;
        uint32_t segId_;
        bool hasHeader_;

        void copy_helper(const KVSlice& toBeCopied);
    };

    class Request{
    public:
        Request();
        ~Request();
        Request(const Request& toBeCopied);
        Request& operator=(const Request& toBeCopied);
        Request(KVSlice& slice);

        KVSlice& GetSlice() const { return *slice_; }
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
        Mutex *mtx_;
        Cond *cond_;

    };

    class SegmentSlice{
    public:
        SegmentSlice();
        ~SegmentSlice();
        SegmentSlice(const SegmentSlice& toBeCopied);
        SegmentSlice& operator=(const SegmentSlice& toBeCopied);

        SegmentSlice(uint32_t seg_id, SegmentManager* sm, BlockDevice* bdev);

        bool IsCanWrite(Request* req) const;
        bool Put(Request* req);
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
        void put4K(Request* req);
        void putNon4K(Request* req);
        void copyHelper(const SegmentSlice& toBeCopied);
        void fillSegHead();
        void notifyAndClean(bool req_state);

        uint32_t segId_;
        SegmentManager* segMgr_;
        BlockDevice* bdev_;
        uint32_t segSize_;
        KVTime creTime_;

        uint32_t headPos_;
        uint32_t tailPos_;

        uint32_t keyNum_;
        uint32_t key4KNum_;
        bool isCompleted_;

        Mutex *mtx_;
        Cond *cond_;

        std::list<Request *> reqList_;
        SegmentOnDisk *segOndisk_;

    };

} //end namespace kvdb


#endif //#ifndef _KV_DB_DATAHANDLE_H_
