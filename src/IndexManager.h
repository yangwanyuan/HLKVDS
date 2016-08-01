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
        void CheckLinkedList(int number);
        void InitHashTable(int size);
        void DestroyHashTable();


        DataHandle* m_data_handle;
        //vector< LinkedList* > m_hashtable;  
        LinkedList*  *m_hashtable;  
        int m_size;
        BlockDevice* m_bdev;

        Timing* m_last_timestamp;

    public:

        static const double EXCESS_BUCKET_FACTOR = 1.1;
    };

}// namespace kvdb
#endif //#ifndef _KV_DB_INDEXMANAGER_H_
