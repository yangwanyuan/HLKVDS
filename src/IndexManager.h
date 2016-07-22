#ifndef _KV_DB_INDEXMANAGER_H_
#define _KV_DB_INDEXMANAGER_H_

#include <string>
#include <sys/time.h>
#include "Db_Structure.h"
#include "BlockDevice.h"
#include "DataHandle.h"
#include "Utils.h"

using namespace std;

namespace kvdb{

    class IndexManager{
    public:
        static uint64_t GetIndexSizeOnDevice(uint64_t ht_size);
        bool InitIndexForCreateDB(uint64_t ht_size);

        bool LoadIndexFromDevice(uint64_t offset, uint64_t ht_size);
        bool WriteIndexToDevice(uint64_t offset);

        int32_t FindIndexToInsert(const char* key, uint32_t key_len, bool* newObj);
        int32_t FindItemIndexAndHeader(const char* key, uint32_t key_len, DataHeader* data_header) const;
        bool GetItemData(const char* key, uint32_t key_len, string &data);
        
        HashEntryOnDisk* GetHashTable(){return m_ht_ondisk;}
        int GetHashTableSize(){return m_size;}

        void UpdateIndexValid(const char* key,uint32_t key_len, int32_t hash_index, uint32_t offset);
        void UpdateIndexDeleted(uint32_t hash_index);

        IndexManager(DataHandle* data_handle, BlockDevice* bdev);
        ~IndexManager();
        
        uint16_t keyfragment_from_key(const char* key, uint32_t key_len) const;
        bool deleted(int32_t index) const;
        bool valid(int32_t index) const;
        bool exists(int32_t index) const;
        uint16_t verifykey(int32_t index) const;

    private:
        uint32_t ComputeHashSizeForCreateDB(uint32_t number);

        //uint16_t keyfragment_from_key(const char* key, uint32_t key_len) const;
        //bool deleted(int32_t index) const;
        //bool valid(int32_t index) const;
        //bool exists(int32_t index) const;
        //uint16_t verifykey(int32_t index) const;

        bool _LoadHashTableFromDevice(uint64_t readLength, off_t offset);
        bool _WriteHashTableToDevice(uint64_t writeLength, off_t offset);
        void ConvertHTDiskToMem();
        void ConvertHTMemToDisk();

        //KvdbDS *m_ds;
        DataHandle* m_data_handle;
        struct HashEntryOnDisk* m_ht_ondisk;
        struct HashEntry* m_hashtable;
        int m_size;
        BlockDevice* m_bdev;
        //time_t m_last_timestamp;
        Timing* m_last_timestamp;


    public:
        static const uint32_t KEYFRAGBITS = 14;
        static const uint32_t KEYFRAGMASK = (1 << KEYFRAGBITS) - 1;
        static const uint32_t DELETEDBITMASK = (2 << KEYFRAGBITS);
        static const uint32_t INDEXBITS = 16;
        static const uint32_t INDEXMASK = (1 << INDEXBITS) - 1;
        static const uint32_t VALIDBITMASK = KEYFRAGMASK+1;

        static const double EXCESS_BUCKET_FACTOR = 1.1;
    };

}// namespace kvdb
#endif //#ifndef _KV_DB_INDEXMANAGER_H_
