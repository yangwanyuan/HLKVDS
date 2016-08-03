#ifndef _KV_DB_INDEXMANAGER_H_
#define _KV_DB_INDEXMANAGER_H_

#include <string>
#include <sys/time.h>

#include "Db_Structure.h"
#include "BlockDevice.h"
#include "DataHandle.h"
#include "Utils.h"
#include "KeyDigestHandle.h"
#include "LinkedList.h"

using namespace std;

namespace kvdb{

    //class HashEntry;
    //template class LinkedList<HashEntry>;

    class HashEntry
    {
    public:
        //HashEntryOnDisk hash_entry;
        HashEntryOnDisk entryOndisk;
        uint32_t pointer;
        HashEntry();
        HashEntry(HashEntryOnDisk entry_ondisk);
        //HashEntry(const HashEntry&);
        ~HashEntry();
        bool operator== (const HashEntry&);
    } __attribute__((__packed__));

    

    class IndexManager{
    public:
        static uint64_t GetIndexSizeOnDevice(uint64_t ht_size);
        bool InitIndexForCreateDB(uint64_t ht_size);

        bool LoadIndexFromDevice(uint64_t offset, uint64_t ht_size);
        bool WriteIndexToDevice(uint64_t offset);

        bool UpdateIndexFromInsert(DataHeader *data_header, Kvdb_Digest *digest, uint32_t header_offset);
        bool GetHashEntry(Kvdb_Digest *digest, HashEntry &hash_entry);

        
        int GetHashTableSize(){return m_size;}

        IndexManager(DataHandle* data_handle, BlockDevice* bdev);
        ~IndexManager();

    private:
        uint32_t ComputeHashSizeForCreateDB(uint32_t number);
        void CreateListIfNotExist(int index);

        bool InitHashTable(int size);
        void DestroyHashTable();

        bool _RebuildHashTable(uint64_t offset);
        bool _LoadDataFromDevice(void* data, uint64_t length, uint64_t offset); 
        bool _ConvertHashEntryFromDiskToMem(int* counter, HashEntryOnDisk* entry_ondisk);

        bool _PersistHashTable(uint64_t offset);
        bool _WriteDataToDevice(void* data, uint64_t length, uint64_t offset);


        DataHandle* m_data_handle;
        LinkedList<HashEntry>** m_hashtable;  
        int m_size;
        BlockDevice* m_bdev;

        Timing* m_last_timestamp;

    };

}// namespace kvdb
#endif //#ifndef _KV_DB_INDEXMANAGER_H_
