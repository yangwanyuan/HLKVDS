#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <unistd.h>


#include "IndexManager.h"


namespace kvdb{

    bool IndexManager::InitIndexForCreateDB(uint64_t numObjects)
    {
        m_size = ComputeHashSizeForCreateDB(numObjects);
        
        InitHashTable(m_size); 
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
        
        //Init hashtable
        m_size = ht_size;
        InitHashTable(m_size);
        
        //Read hashtable 
        uint64_t table_length = sizeof(int) * m_size;
        int* counter = (int *)malloc(table_length);
        ssize_t nread;
        uint64_t h_offset = 0;
        while((nread = m_bdev->pRead(counter + h_offset, table_length, offset)) > 0){
            table_length -= nread;
            offset += nread;
            h_offset += nread;
            if (nread < 0){
                perror("Error in reading hashtable from file\n");
                return false;
            }
        }

        //Read hash_entry
        uint64_t length = sizeof(struct HashEntryOnDisk) * m_size;
        struct HashEntryOnDisk *entry_ondisk = (struct HashEntryOnDisk*)malloc(sizeof(struct HashEntryOnDisk) * m_size);
        if(entry_ondisk == NULL){
            perror("could not malloc hash table on disk file\n");
            return false;
        }

        h_offset = 0;
        while((nread = m_bdev->pRead(entry_ondisk + h_offset, length, offset)) > 0){
            length -= nread;
            offset += nread;
            h_offset += nread;
            if (nread < 0){
                perror("Error in reading hashtable from file\n");
                return false;
            }
        }
       
        //Convert hashtable from device to memory
        int entry_index = 0;
        for(int i = 0; i < m_size; i++)
        {
            
            int entry_num = counter[i];
            for(int j=0; j< entry_num; j++)
            {
                struct HashEntry entry;
                entry.hash_entry = entry_ondisk[entry_index];
                entry.pointer = 0;

                CheckLinkedList(i);
                m_hashtable[i]->insert(entry);

                entry_index++;
            }
            __DEBUG("read hashtable[%d]=%d", i, entry_num);
        }

        free(entry_ondisk);
        free(counter);
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


        int entry_num;
        //Write hashtable to device
        for(int i=0; i<m_size; i++)
        {
            if (!m_hashtable[i])
                entry_num = 0;
            else
                entry_num = (m_hashtable[i])->get_size();

            m_bdev->pWrite(&entry_num, sizeof(int), offset);
            offset += sizeof(int);
            __DEBUG("write hashtable[%d]=%d on %d", i, entry_num, (int)offset);
        }

        //Write all hash_entry to device
        for(int i=0; i<m_size; i++)
        {
            if (!m_hashtable[i])
                continue;
            vector<HashEntry> tmp_vec = m_hashtable[i]->get();
            for(vector<struct HashEntry>::iterator iter = tmp_vec.begin(); iter!=tmp_vec.end(); iter++)
            {
                //struct HashEntry *entry = *iter;
                struct HashEntryOnDisk *entry_ondisk = NULL;
                entry_ondisk = &(iter->hash_entry);
                m_bdev->pWrite(entry_ondisk, sizeof(HashEntryOnDisk), offset);
                offset +=  sizeof(HashEntryOnDisk);
                //__DEBUG("write entry_ondisk on %d", (int)offset);
            }
            //__DEBUG("write hashtable[%d] hash entry complete", i);
        }
    
        return true;
    }

    bool IndexManager::UpdateIndexFromInsert(DataHeader *data_header, Kvdb_Digest *digest, uint32_t header_offset)
    {
        struct HashEntryOnDisk entry_ondisk;
        memcpy(&entry_ondisk.dataheader, data_header, sizeof(DataHeader));
        entry_ondisk.offset.header_offset = header_offset;
        struct HashEntry entry;
        memcpy(&entry.hash_entry, &entry_ondisk, sizeof(HashEntryOnDisk));

        uint32_t hash_index = KeyDigestHandle::Hash(digest) % m_size;

        CheckLinkedList(hash_index);
        LinkedList *entry_list = m_hashtable[hash_index];

        entry_list->insert(entry);

        return true; 
    }

    bool IndexManager::GetHashEntry(Kvdb_Digest *digest, HashEntry &entry)
    {
        uint32_t hash_index = KeyDigestHandle::Hash(digest) % m_size;

        CheckLinkedList(hash_index);
        LinkedList *entry_list = m_hashtable[hash_index];

        memcpy(&entry.hash_entry.dataheader.key, &digest->value, 20 );
        
        if(entry_list->search(entry))
        {
             vector<HashEntry> tmp_vec = entry_list->get();
             for(vector<struct HashEntry>::iterator iter = tmp_vec.begin(); iter!=tmp_vec.end(); iter++)
             {
                 if(!memcmp(iter->hash_entry.dataheader.key, entry.hash_entry.dataheader.key, 20))
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
        uint64_t index_size = sizeof(time_t) + sizeof(struct HashEntryOnDisk) * ht_size;
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

    void IndexManager::CheckLinkedList(int number)
    {
        if(!m_hashtable[number])
        {
            LinkedList *entry_list = new LinkedList;
            m_hashtable[number] = entry_list;
        }
        return;
    }

    void IndexManager::InitHashTable(int size)
    {
        m_hashtable =  new LinkedList*[m_size];
        //memset(m_hashtable, 0, sizeof(LinkedList*)*m_size);
        for(int i=0; i<size; i++)
            m_hashtable[i]=NULL;
        return;
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

}// namespace kvdb
