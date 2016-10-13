#ifndef _KV_DB_INDEXMANAGER_H_
#define _KV_DB_INDEXMANAGER_H_

#include <string>
#include <sys/time.h>
#include <mutex>

#include "Db_Structure.h"
#include "BlockDevice.h"
#include "Utils.h"
#include "KeyDigestHandle.h"
#include "LinkedList.h"
#include "DataHandle.h"
#include "SuperBlockManager.h"

using namespace std;

namespace kvdb{
    class KVSlice;
    //struct OpType;

    class DataHeader {
    private:
        Kvdb_Digest key_digest;
        uint16_t data_size;
        uint32_t data_offset;
        uint32_t next_header_offset;

    public:
        DataHeader();
        DataHeader(const Kvdb_Digest &digest, uint16_t data_size, uint32_t data_offset, uint32_t next_header_offset);
        DataHeader(const DataHeader& toBeCopied);
        ~DataHeader();
        DataHeader& operator=(const DataHeader& toBeCopied);

        uint16_t GetDataSize() const { return data_size; }
        uint32_t GetDataOffset() const { return data_offset; }
        uint32_t GetNextHeadOffset() const { return next_header_offset; }
        Kvdb_Digest GetDigest() const { return key_digest; }

        void SetDigest(const Kvdb_Digest& digest);
        void SetDataSize(uint16_t size) { data_size = size; }
        void SetDataOffset(uint32_t offset) { data_offset = offset; }
        void SetNextHeadOffset(uint32_t offset) { next_header_offset = offset; }

    } __attribute__((__packed__));

    class DataHeaderOffset{
    private:
        uint64_t physical_offset;

    public:
        DataHeaderOffset(): physical_offset(0){}
        DataHeaderOffset(uint64_t offset): physical_offset(offset){}
        DataHeaderOffset(const DataHeaderOffset& toBeCopied);
        ~DataHeaderOffset();
        DataHeaderOffset& operator=(const DataHeaderOffset& toBeCopied);

        uint64_t GetHeaderOffset() const { return physical_offset; }

    }__attribute__((__packed__));

    class HashEntryOnDisk {
    private:
        DataHeader header;
        DataHeaderOffset header_offset;

    public:
        HashEntryOnDisk();
        HashEntryOnDisk(DataHeader& dataheader, DataHeaderOffset& offset);
        HashEntryOnDisk(DataHeader& dataheader, uint64_t offset);
        HashEntryOnDisk(const HashEntryOnDisk& toBeCopied);
        ~HashEntryOnDisk();
        HashEntryOnDisk& operator=(const HashEntryOnDisk& toBeCopied);

        uint64_t GetHeaderOffsetPhy() const { return header_offset.GetHeaderOffset(); }
        uint16_t GetDataSize() const { return header.GetDataSize(); }
        uint32_t GetDataOffsetInSeg() const { return header.GetDataOffset(); }
        uint32_t GetNextHeadOffsetInSeg() const { return header.GetNextHeadOffset(); }
        Kvdb_Digest GetKeyDigest() const { return header.GetDigest(); }
        DataHeader& GetDataHeader() { return header; }

        void SetKeyDigest(const Kvdb_Digest& digest);

    } __attribute__((__packed__));


    class HashEntry
    {
    public:
        class LogicStamp
        {
        private:
            KVTime segTime_;
            int32_t keyNo_;
        public:
            LogicStamp() : segTime_(KVTime()), keyNo_(0){}
            LogicStamp(KVTime seg_time, int32_t key_no) : segTime_(seg_time), keyNo_(key_no){}
            LogicStamp(const LogicStamp& toBeCopied)
            {
                segTime_ = toBeCopied.segTime_;
                keyNo_ = toBeCopied.keyNo_;
            }
            ~LogicStamp(){}
            LogicStamp& operator=(const LogicStamp& toBeCopied)
            {
                segTime_ = toBeCopied.segTime_;
                keyNo_ = toBeCopied.keyNo_;
                return *this;
            }

            KVTime& GetSegTime() { return segTime_; }
            int32_t GetKeyNo() { return keyNo_; }
        };
        HashEntry();
        HashEntry(HashEntryOnDisk& entry_ondisk, KVTime time_stamp, void* read_ptr);
        HashEntry(DataHeader& data_header, uint64_t header_offset, void* read_ptr);
        HashEntry(const HashEntry&);
        ~HashEntry();
        bool operator==(const HashEntry& toBeCompare) const;
        HashEntry& operator=(const HashEntry& toBeCopied);

        uint64_t GetHeaderOffsetPhy() const { return entryPtr_->GetHeaderOffsetPhy(); }
        uint16_t GetDataSize() const { return entryPtr_->GetDataSize(); }
        uint32_t GetDataOffsetInSeg() const { return entryPtr_->GetDataOffsetInSeg(); }
        uint32_t GetNextHeadOffsetInSeg() const { return entryPtr_->GetNextHeadOffsetInSeg(); }
        void* GetReadCachePtr() const { return cachePtr_; }
        Kvdb_Digest GetKeyDigest() const { return entryPtr_->GetKeyDigest(); }
        HashEntryOnDisk& GetEntryOnDisk() { return *entryPtr_; }
        LogicStamp* GetLogicStamp() {return stampPtr_; }

        void SetKeyDigest(const Kvdb_Digest& digest);
        void SetLogicStamp(KVTime seg_time, int32_t seg_key_no);

    private:
        HashEntryOnDisk *entryPtr_;
        LogicStamp *stampPtr_;
        void* cachePtr_;

    }; //__attribute__((__packed__));

    

    class IndexManager{
    public:
        static inline size_t SizeOfDataHeader(){ return sizeof(DataHeader); }
        static inline size_t SizeOfHashEntryOnDisk(){ return sizeof(HashEntryOnDisk); }

        static uint64_t GetIndexSizeOnDevice(uint32_t ht_size);
        bool InitIndexForCreateDB(uint32_t numObjects);

        bool LoadIndexFromDevice(uint64_t offset, uint32_t ht_size);
        bool WriteIndexToDevice(uint64_t offset);

        //bool UpdateIndex(KVSlice* slice, OpType &op_type);
        bool UpdateIndex(KVSlice* slice);
        bool GetHashEntry(KVSlice *slice);
        //bool IsKeyExist(const KVSlice *slice);

        uint32_t GetHashTableSize() const { return htSize_; }

        IndexManager(BlockDevice* bdev, SuperBlockManager* sbMgr_);
        ~IndexManager();

    private:
        uint32_t computeHashSizeForCreateDB(uint32_t number);
        void createListIfNotExist(uint32_t index);

        bool initHashTable(uint32_t size);
        void destroyHashTable();

        bool rebuildHashTable(uint64_t offset);
        bool rebuildTime(uint64_t offset);
        bool loadDataFromDevice(void* data, uint64_t length, uint64_t offset); 
        bool convertHashEntryFromDiskToMem(int* counter, HashEntryOnDisk* entry_ondisk);

        bool persistHashTable(uint64_t offset);
        bool persistTime(uint64_t offset);
        bool writeDataToDevice(void* data, uint64_t length, uint64_t offset);


        LinkedList<HashEntry>** hashtable_;  
        uint32_t htSize_;
        uint32_t used_;
        BlockDevice* bdev_;
        SuperBlockManager* sbMgr_;

        KVTime* lastTime_;
        mutable std::mutex mtx_;

    //private:
    //    friend class EntryGCThd;
    //    class EntryGCThd : public Thread
    //    {
    //    public:
    //        EntryGCThd(): db_(NULL){}
    //        EntryGCThd(IndexManager* idxMgr): idxMgr_(idxMgr){}
    //        virtual ~EntryGCThd(){}
    //        EntryGCThd(EntryGCThd& toBeCopied) = delete;
    //        EntryGCThd& operator=(EntryGCThd& toBeCopied) = delete;

    //        virtual void* Entry() { idxMgr_->EntryGCThdEntry(); return 0; }

    //    private:
    //        friend class IndexManager;
    //        IndexManager* idxMgr_;
    //    };
    //    EntryGCThd EntryGCT_;
    //    std::list<HashEntry> EntryGCQue_;
    //    std::atomic<bool> EntryGCT_stop_;
    //    std::mutex EntryGCQueMtx_;

    //    void EntryGCThdEntry();

    //    void removeEntry();


    };

}// namespace kvdb
#endif //#ifndef _KV_DB_INDEXMANAGER_H_
