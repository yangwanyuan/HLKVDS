#include <stdlib.h>
#include <string.h>
#include <stdio.h>

#include "HashFun.h"
#include "IndexManager.h"


namespace kvdb{
    bool IndexManager::InitIndexForCreateDB(uint64_t numObjects)
    {
        m_size = ComputeHashSizeForCreateDB(numObjects);

        m_ht_ondisk = (struct HashEntryOnDisk*)malloc(sizeof(struct HashEntryOnDisk) * m_size);
        if(m_ht_ondisk == NULL){
            perror("could not malloc hash table file\n");
            return false;
        }
        memset(m_ht_ondisk, 0, sizeof(struct HashEntryOnDisk) * m_size);

        m_hashtable = (struct HashEntry*)malloc(sizeof(struct HashEntry) * m_size);
        if(m_ht_ondisk == NULL){
            perror("could not malloc hash table file\n");
            return false;
        }
        memset(m_hashtable, 0, sizeof(struct HashEntryOnDisk) * m_size);

        return true;
    }

    bool IndexManager::LoadIndexFromDevice(uint64_t offset, uint64_t ht_size)
    {
        //Read timestamp from device
        ssize_t timeLength = Timing::GetTimeSizeOf();
        if(!m_last_timestamp->LoadTimeFromDevice(offset))
            return false;
        __DEBUG("Load Hashtable timestamp: %s", m_last_timestamp->TimeToChar());
        offset += timeLength;
        
        //Read hashtable from device
        m_size = ht_size;

        uint64_t length = sizeof(struct HashEntryOnDisk) * m_size;
        m_ht_ondisk = (struct HashEntryOnDisk*)malloc(sizeof(struct HashEntryOnDisk) * m_size);
        if(m_ht_ondisk == NULL){
            perror("could not malloc hash table on disk file\n");
            return false;
        }
        if(!_LoadHashTableFromDevice(length, offset))
            return false;
       
        //Convert hashtable from device to memory
        m_hashtable = (struct HashEntry*)malloc(sizeof(struct HashEntry) * m_size);
        if(m_ht_ondisk == NULL){
            perror("could not malloc hash table file\n");
            return false;
        }
        ConvertHTDiskToMem();

        return true;
    }

    bool IndexManager::_LoadHashTableFromDevice(uint64_t readLength, off_t offset)
    {
        ssize_t nread;
        uint64_t h_offset = 0;
        while((nread = m_bdev->pRead(m_ht_ondisk + h_offset, readLength, offset)) > 0){
            readLength -= nread;
            offset += nread;
            h_offset += nread;
            if (nread < 0){
                perror("Error in reading hashtable from file\n");
                return false;
            }
        }
        return true;
    }

    bool IndexManager::WriteIndexToDevice(uint64_t offset)
    {
        //Write timestamp to device
        ssize_t timeLength = Timing::GetTimeSizeOf();
        m_last_timestamp->UpdateTimeToNow();
        if(!m_last_timestamp->WriteTimeToDevice(offset))
            return false;
        __DEBUG("Write Hashtable timestamp: %s", m_last_timestamp->TimeToChar());
        offset += timeLength;

        //Convert hashtable from memory to device
        ConvertHTMemToDisk();

        //Write hashtable to device
        uint64_t length = sizeof(struct HashEntryOnDisk) * m_size;
        if(!_WriteHashTableToDevice(length, offset))
            return false;

        return true;
    }

    bool IndexManager::_WriteHashTableToDevice(uint64_t writeLength, off_t offset)
    {
        ssize_t nwrite;
        while((nwrite = m_bdev->pWrite(m_ht_ondisk, writeLength, offset)) > 0){
            writeLength -= nwrite;
            offset += nwrite;
            if(nwrite < 0){
                perror("Error in Write hashtable to file\n");
                return false;
            }
        }
        return true;
    }

    void IndexManager::ConvertHTDiskToMem()
    {
        for(int index=0; index < (int)m_size; index++)
        {
            m_hashtable[index].present_key = m_ht_ondisk[index].present_key;
            m_hashtable[index].offset = m_ht_ondisk[index].offset;
            m_hashtable[index].pointer = 0;
        }
    }
    
    void IndexManager::ConvertHTMemToDisk()
    {
        for(int index=0; index < (int)m_size; index++)
        {
            m_ht_ondisk[index].present_key = m_hashtable[index].present_key;
            m_ht_ondisk[index].offset = m_hashtable[index].offset;
        }
    }

    uint64_t IndexManager::GetIndexSizeOnDevice(uint64_t ht_size)
    {
        uint64_t index_size = sizeof(time_t) + sizeof(struct HashEntryOnDisk) * ht_size;
        uint64_t index_size_pages = index_size / getpagesize();
        return (index_size_pages + 1) * getpagesize();
    }

    int32_t IndexManager::FindIndexToInsert(const char* key, uint32_t key_len, bool* newObj)
    {
        int32_t hash_index = 0;
        for (u_int hash_fn_index = 0; hash_fn_index < HASH_COUNT; hash_fn_index++){
            uint32_t indexkey = (*(Hashes::hashes[hash_fn_index]))(key, key_len);
            uint32_t hash_index_start = indexkey & (m_size - 1);

            hash_index_start &= (~(PROBES_BEFORE_REHASH-1));

            for(hash_index = hash_index_start; 
                (uint32_t)hash_index < hash_index_start + PROBES_BEFORE_REHASH; 
                hash_index++){
                
                uint16_t vkey = verifykey(hash_index);
                // Find first open spot or find the same key
                if(!exists(hash_index)){
                    *newObj = true; //This is a new object, not an updated object for an existing key.
                    return hash_index;
                }else if(vkey == keyfragment_from_key(key, key_len)){
                    off_t datapos = m_hashtable[hash_index].offset;
                    DataHeader data_header;
                    string ckey;
                    if(m_data_handle->ReadDataHeader(datapos, data_header, ckey) &&
                            ckey.length() == key_len &&
                            memcmp(ckey.data(), key, key_len) == 0)
                        return hash_index;
                }
            }
        }
        return ERR;
    }

    int32_t IndexManager::FindItemIndexAndHeader(const char* key, uint32_t key_len, DataHeader* data_header) const
    {
        DataHeader dummy_header;
        int32_t hash_index = 0;
        
        if (data_header == NULL)
            data_header = &dummy_header;
        
        for (u_int hash_fn_index = 0; hash_fn_index < HASH_COUNT; hash_fn_index++) {
            uint32_t indexkey = (*(Hashes::hashes[hash_fn_index]))(key, key_len);
            // calculate the starting index to probe. We linearly probe
            // PROBES_BEFORE_REHASH locations before rehashing the key to get a new
            // starting index.
        
            // Mask lower order bits to find hash index
            uint32_t hash_index_start = indexkey & (m_size - 1);
        
            hash_index_start &= (~(PROBES_BEFORE_REHASH-1));
        
            for (hash_index = hash_index_start;
                 (uint32_t)hash_index < hash_index_start + PROBES_BEFORE_REHASH;
                 ++hash_index) {
        
                uint16_t vkey = verifykey(hash_index);
        
                // If empty entry, key could not be found.
                if (!valid(hash_index)) {
                    return ERR;
                }
                // Skip over deleted entries
                else if (deleted(hash_index)) {
                    continue;
                }
                else if (vkey == keyfragment_from_key(key, key_len)) {
                    off_t datapos = m_hashtable[hash_index].offset;
                    DataHeader data_header;
                    string ckey;
                    if (m_data_handle->ReadDataHeader(datapos, data_header, ckey) &&
                        ckey.length() == key_len &&
                        memcmp(ckey.data(), key, key_len) == 0)
                        return hash_index;
                }
            }
        
        }
        return ERR;     
    }


    bool IndexManager::GetItemData(const char* key, uint32_t key_len, string &data) 
    {
        int32_t hash_index = 0;
        for (u_int hash_fn_index = 0; hash_fn_index < HASH_COUNT; hash_fn_index++) {
            uint32_t indexkey = (*(Hashes::hashes[hash_fn_index]))(key, key_len);

            // Mask lower order bits to find hash index
            uint32_t hash_index_start = indexkey & (m_size - 1);

            hash_index_start &= (~(PROBES_BEFORE_REHASH-1));

            for (hash_index = hash_index_start;
                 (uint32_t)hash_index < hash_index_start + PROBES_BEFORE_REHASH;
                 ++hash_index) {
                uint16_t vkey = verifykey(hash_index);
                if (!valid(hash_index)) {
                    return false;
                }
                else if (deleted(hash_index)) {
                    continue;
                }
                else if (vkey == keyfragment_from_key(key, key_len)) {
                    off_t datapos = m_hashtable[hash_index].offset;
                    if (m_data_handle->ReadData(key, key_len, datapos, data))
                        return true;
                }
            }

        }
        return false; 
    }

    void IndexManager::UpdateIndexValid(const char* key, uint32_t key_len, int32_t hash_index, uint32_t offset)
    {
        uint16_t key_in_hashtable = keyfragment_from_key(key, key_len);
        key_in_hashtable |= VALIDBITMASK; // set valid bit to 1
        m_hashtable[hash_index].present_key = key_in_hashtable;
        m_hashtable[hash_index].offset = offset;
    }

    void IndexManager::UpdateIndexDeleted(uint32_t hash_index)
    {
       m_hashtable[hash_index].present_key |= DELETEDBITMASK; 
    }

    IndexManager::IndexManager(DataHandle* data_handle, BlockDevice* bdev):
        m_data_handle(data_handle), m_ht_ondisk(NULL), 
        m_hashtable(NULL), m_size(0), m_bdev(bdev)
    {
        m_last_timestamp = new Timing(bdev);
        return ;
    }

    IndexManager::~IndexManager()
    {
        if(m_ht_ondisk)
            free(m_ht_ondisk);
        if(m_hashtable)
            free(m_hashtable);
        if(m_last_timestamp)
            free(m_last_timestamp);
    }

    uint32_t IndexManager::ComputeHashSizeForCreateDB(uint32_t number)
    {
        // Gets the next highest power of 2 larger than number
        number--;
        number = (number >> 1) | number;
        number = (number >> 2) | number;
        number = (number >> 4) | number;
        number = (number >> 8) | number;
        number = (number >> 16) | number;
        number++;
        return number;
    }

    // Grab lowest order KEYFRAG bits to use as keyfragment
    // Key should be at least 2 bytes long for this to work properly
    uint16_t IndexManager::keyfragment_from_key(const char *key, uint32_t key_len) const 
    {
        if (key_len < sizeof(uint16_t)) {
            return (uint16_t) (key[0] & KEYFRAGMASK);
        }
        return (uint16_t) ((key[key_len-2]<<8) + key[key_len-1]) & KEYFRAGMASK;
    }
    
    bool IndexManager::deleted(int32_t index) const
    {
        return (m_hashtable[index].present_key & DELETEDBITMASK);
    }

    bool IndexManager::valid(int32_t index) const
    {
        return (m_hashtable[index].present_key & VALIDBITMASK);
    }

    bool IndexManager::exists(int32_t index) const
    {
        return valid(index) && !deleted(index);
    }

    uint16_t IndexManager::verifykey(int32_t index) const
    {
        return (m_hashtable[index].present_key & KEYFRAGMASK);
    }

}
