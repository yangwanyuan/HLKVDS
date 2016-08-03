#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <unistd.h>


#include "IndexManager.h"


namespace kvdb{

    //template class LinkedList<HashEntry>;

    HashEntry::HashEntry()
    {
        return;
    }

    HashEntry::HashEntry(HashEntryOnDisk entry_ondisk)
    {
        entryOndisk = entry_ondisk;
        pointer = 0;
    }

    HashEntry::~HashEntry()
    {
        return;
    }

    bool HashEntry::operator==(const HashEntry& compare)
    {
        if (!memcmp(&(entryOndisk.header.key), &(compare.entryOndisk.header.key), KEYDIGEST_SIZE))
            return true;
        return false;
    }

    bool IndexManager::InitIndexForCreateDB(uint64_t numObjects)
    {
        m_size = ComputeHashSizeForCreateDB(numObjects);
        
        if(!InitHashTable(m_size))
            return false;

        return true;
    }

    bool IndexManager::LoadIndexFromDevice(uint64_t offset, uint64_t ht_size)
    {
        m_size = ht_size;

        //Read timestamp from device
        ssize_t timeLength = Timing::GetTimeSizeOf();
        if(!m_last_timestamp->LoadTimeFromDevice(offset))
            return false;
        __DEBUG("Load Hashtable timestamp: %s", m_last_timestamp->TimeToChar());
        offset += timeLength;
        
        if(!_RebuildHashTable(offset))
            return false;
        __DEBUG("Rebuild Hashtable Success");

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


        if(!_PersistHashTable(offset))
            return false;
        __DEBUG("Persist Hashtable Success");
    
        return true;
    }

    bool IndexManager::UpdateIndexFromInsert(DataHeader *data_header, Kvdb_Digest *digest, uint32_t header_offset)
    {
        HashEntryOnDisk entry_ondisk;
        memcpy(&entry_ondisk.header, data_header, sizeof(DataHeader));
        entry_ondisk.header_offset.physical_offset = header_offset;

        HashEntry entry;
        memcpy(&entry.entryOndisk, &entry_ondisk, sizeof(HashEntryOnDisk));

        uint32_t hash_index = KeyDigestHandle::Hash(digest) % m_size;

        CreateListIfNotExist(hash_index);

        LinkedList<HashEntry> *entry_list = m_hashtable[hash_index];
        entry_list->insert(entry);

        return true; 
    }

    bool IndexManager::GetHashEntry(Kvdb_Digest *digest, HashEntry &entry)
    {
        uint32_t hash_index = KeyDigestHandle::Hash(digest) % m_size;

        CreateListIfNotExist(hash_index);

        LinkedList<HashEntry> *entry_list = m_hashtable[hash_index];

        memcpy(&entry.entryOndisk.header.key, &digest->value, sizeof(Kvdb_Digest) );
        
        if(entry_list->search(entry))
        {
             vector<HashEntry> tmp_vec = entry_list->get();
             for(vector<HashEntry>::iterator iter = tmp_vec.begin(); iter!=tmp_vec.end(); iter++)
             {
                 if(!memcmp(iter->entryOndisk.header.key, entry.entryOndisk.header.key, sizeof(Kvdb_Digest)))
                 {
                     memcpy(&entry, (const void*)(&*iter), sizeof(HashEntry));
                     return true;
                 }
             }
        }
        return false;
    }

    uint64_t IndexManager::GetIndexSizeOnDevice(uint64_t ht_size)
    {
        uint64_t index_size = sizeof(time_t) + sizeof(HashEntryOnDisk) * ht_size;
        uint64_t index_size_pages = index_size / getpagesize();
        return (index_size_pages + 1) * getpagesize();
    }


    IndexManager::IndexManager(DataHandle* data_handle, BlockDevice* bdev):
        m_data_handle(data_handle), m_hashtable(NULL), m_size(0), m_bdev(bdev)
    {
        m_last_timestamp = new Timing(bdev);
        return ;
    }

    IndexManager::~IndexManager()
    {
        if(m_last_timestamp)
            delete m_last_timestamp;
        if(m_hashtable)
        {
            DestroyHashTable();
        }
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

    void IndexManager::CreateListIfNotExist(int index)
    {
        if(!m_hashtable[index])
        {
            LinkedList<HashEntry> *entry_list = new LinkedList<HashEntry>;
            m_hashtable[index] = entry_list;
        }
        return;
    }

    bool IndexManager::InitHashTable(int size)
    {
        m_hashtable =  new LinkedList<HashEntry>*[m_size];

        //memset(m_hashtable, 0, sizeof(LinkedList*)*m_size);
        for(int i=0; i<size; i++)
            m_hashtable[i]=NULL;
        return true;
    }

    void IndexManager::DestroyHashTable()
    {
        for(int i = 0; i < m_size; i++)
        {
            if(m_hashtable[i])
            {
                delete m_hashtable[i];
                m_hashtable[i] = NULL;
            }
        }
        delete[] m_hashtable;
        m_hashtable = NULL;
        return;
    }

    bool IndexManager::_RebuildHashTable(uint64_t offset)
    {
        //Init hashtable
        if(!InitHashTable(m_size))
            return false;
        
        //Read hashtable 
        uint64_t table_length = sizeof(int) * m_size;
        int* counter = new int[m_size];
        if(!_LoadDataFromDevice((void*)counter, table_length, offset))
            return false;
        offset += table_length;

        //Read all hash_entry
        uint64_t length = sizeof(HashEntryOnDisk) * m_size;
        HashEntryOnDisk *entry_ondisk = new HashEntryOnDisk[m_size];
        if(!_LoadDataFromDevice((void*)entry_ondisk, length, offset))
            return false;
        offset += length;
       
        //Convert hashtable from device to memory
        if(!_ConvertHashEntryFromDiskToMem(counter, entry_ondisk))
            return false;

        delete entry_ondisk;
        delete counter;

        return true; 
    }

    bool IndexManager::_LoadDataFromDevice(void* data, uint64_t length, uint64_t offset)
    {
        ssize_t nread;
        uint64_t h_offset = 0;
        while((nread = m_bdev->pRead((uint64_t *)data + h_offset, length, offset)) > 0){
            length -= nread;
            offset += nread;
            h_offset += nread;
            if (nread < 0){
                perror("Error in reading hashtable from file\n");
                return false;
            }
        }
        return true;
    }

    bool IndexManager::_WriteDataToDevice(void* data, uint64_t length, uint64_t offset)
    {
        ssize_t nwrite;
        uint64_t h_offset = 0;
        while((nwrite = m_bdev->pWrite((uint64_t *)data + h_offset, length, offset)) > 0){
            length -= nwrite;
            offset += nwrite;
            h_offset += nwrite;
            if (nwrite < 0){
                perror("Error in reading hashtable from file\n");
                return false;
            }
        }
        return true;
    }

    bool IndexManager::_ConvertHashEntryFromDiskToMem(int* counter, HashEntryOnDisk* entry_ondisk)
    {
        //Convert hashtable from device to memory
        int entry_index = 0;
        for(int i = 0; i < m_size; i++)
        {
            
            int entry_num = counter[i];
            for(int j=0; j< entry_num; j++)
            {
                HashEntry entry;
                entry.entryOndisk = entry_ondisk[entry_index];
                entry.pointer = 0;

                CreateListIfNotExist(i);
                m_hashtable[i]->insert(entry);
                entry_index++;
            }
            __DEBUG("read hashtable[%d]=%d", i, entry_num);
        }
        return true;
    }

    bool IndexManager::_PersistHashTable(uint64_t offset)
    {
        //write hashtable to device
        uint64_t table_length = sizeof(int) * m_size;
        int* counter = new int[m_size];
        for(int i=0; i<m_size; i++)
        {
            counter[i] = (m_hashtable[i]? m_hashtable[i]->get_size(): 0);
        }
        if(!_WriteDataToDevice((void*)counter, table_length, offset))
            return false;
        offset += table_length;
        delete[] counter;


        //write hash entry to device
        uint64_t length = sizeof(HashEntryOnDisk) * m_size;
        HashEntryOnDisk *entry_ondisk = new HashEntryOnDisk[m_size];
        for(int i=0; i<m_size; i++)
        {
            if (!m_hashtable[i])
                continue;
            vector<HashEntry> tmp_vec = m_hashtable[i]->get();
            for(vector<HashEntry>::iterator iter = tmp_vec.begin(); iter!=tmp_vec.end(); iter++)
            {
                entry_ondisk[i] = (iter->entryOndisk);
            }
        }
        if(!_WriteDataToDevice((void *)entry_ondisk, length, offset))
            return false;
        delete[] entry_ondisk;

        return true;
    
    }

}// namespace kvdb
