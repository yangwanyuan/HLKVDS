#ifndef _KV_DB_INDEXMANAGER_H_
#define _KV_DB_INDEXMANAGER_H_

#include <string>
#include <sys/time.h>

#include "Db_Structure.h"
#include "BlockDevice.h"
#include "Utils.h"
#include "KeyDigestHandle.h"
#include "LinkedList.h"
#include "DataHandle.h"

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
    private:
        HashEntryOnDisk *entryPtr_;
        void* cachePtr_;

    public:
        HashEntry();
        HashEntry(HashEntryOnDisk& entry_ondisk, void* read_ptr);
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

        void SetKeyDigest(const Kvdb_Digest& digest);

    } __attribute__((__packed__));

    

    class IndexManager{
    public:
        static inline size_t SizeOfDataHeader(){ return sizeof(DataHeader); }
        static inline size_t SizeOfHashEntryOnDisk(){ return sizeof(HashEntryOnDisk); }

        static uint64_t GetIndexSizeOnDevice(uint32_t ht_size);
        bool InitIndexForCreateDB(uint32_t numObjects);

        bool LoadIndexFromDevice(uint64_t offset, uint32_t ht_size);
        bool WriteIndexToDevice(uint64_t offset);

        bool UpdateIndex(KVSlice* slice, OpType &op_type);
        bool GetHashEntry(KVSlice *slice);
        //bool IsKeyExist(const KVSlice *slice);

        uint32_t GetHashTableSize() const { return htSize_; }

        IndexManager(BlockDevice* bdev);
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

        KVTime* lastTime_;
        Mutex mtx_;

    };

}// namespace kvdb
#endif //#ifndef _KV_DB_INDEXMANAGER_H_
