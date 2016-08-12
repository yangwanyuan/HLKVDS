#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <unistd.h>

#include <iostream>


#include "IndexManager.h"


namespace kvdb{

    DataHeader::DataHeader() : key_digest(Kvdb_Digest()), data_size(0), data_offset(0), next_header_offset(0){}

    DataHeader::DataHeader(Kvdb_Digest &digest, uint16_t size, uint32_t offset, uint32_t next_offset) : key_digest(digest), data_size(size), data_offset(offset), next_header_offset(next_offset){}

    DataHeader::DataHeader(const DataHeader& toBeCopied)
    {
        key_digest = toBeCopied.key_digest;
        data_size = toBeCopied.data_size;
        data_offset = toBeCopied.data_offset;
        next_header_offset = toBeCopied.next_header_offset;
    }

    DataHeader::~DataHeader(){}

    DataHeader& DataHeader::operator=(const DataHeader& toBeCopied)
    {
        key_digest = toBeCopied.key_digest;
        data_size = toBeCopied.data_size;
        data_offset = toBeCopied.data_offset;
        next_header_offset = toBeCopied.next_header_offset;
        return *this;
    }

    void DataHeader::SetDigest(Kvdb_Digest& digest)
    {
        key_digest = digest;
    }


    DataHeaderOffset::DataHeaderOffset(const DataHeaderOffset& toBeCopied)
    {
        physical_offset = toBeCopied.physical_offset;
    }

    DataHeaderOffset::~DataHeaderOffset()
    {
        return;
    }

    DataHeaderOffset& DataHeaderOffset::operator=(const DataHeaderOffset& toBeCopied)
    {
        physical_offset = toBeCopied.physical_offset;
        return *this;
    }

    HashEntryOnDisk::HashEntryOnDisk() : header(DataHeader()), header_offset(DataHeaderOffset()){}

    HashEntryOnDisk::HashEntryOnDisk(DataHeader& dataheader, DataHeaderOffset& offset) : header(dataheader), header_offset(offset){}

    HashEntryOnDisk::HashEntryOnDisk(DataHeader& dataheader, uint64_t offset)
    {
        header = dataheader;
        DataHeaderOffset h_off(offset);
        header_offset = h_off;
        return;
    }

    HashEntryOnDisk::HashEntryOnDisk(const HashEntryOnDisk& toBeCopied)
    {
        header = toBeCopied.header;
        header_offset = toBeCopied.header_offset;
    }

    HashEntryOnDisk::~HashEntryOnDisk(){}


    HashEntryOnDisk& HashEntryOnDisk::operator= (const HashEntryOnDisk& toBeCopied)
    {
        header = toBeCopied.header;
        header_offset = toBeCopied.header_offset;
        return *this;
    }

    void HashEntryOnDisk::SetKeyDigest(Kvdb_Digest& digest)
    {
        header.SetDigest(digest);
    }

    HashEntry::HashEntry(): entryOndisk(HashEntryOnDisk()), pointer(NULL){}

    HashEntry::HashEntry(HashEntryOnDisk& entry_ondisk, void* read_ptr): entryOndisk(entry_ondisk), pointer(read_ptr){}

    HashEntry::HashEntry(DataHeader& data_header, uint64_t header_offset, void* read_ptr)
    {
        HashEntryOnDisk entry_ondisk(data_header, header_offset);
        entryOndisk = entry_ondisk;
        pointer = read_ptr;
    }

    HashEntry::HashEntry(const HashEntry& toBeCopied)
    {
        entryOndisk = toBeCopied.entryOndisk;
        pointer = toBeCopied.pointer;
    }

    HashEntry::~HashEntry()
    {
        pointer = NULL;
        return;
    }

    bool HashEntry::operator==(const HashEntry& toBeCompare)
    {
        if (this->GetKeyDigest() == toBeCompare.GetKeyDigest())
        {
            return true;
        }
        return false;
    }

    HashEntry& HashEntry::operator=(const HashEntry& toBeCopied)
    {
        entryOndisk = toBeCopied.entryOndisk;
        pointer = toBeCopied.pointer;
        return *this;
    }

    void HashEntry::SetKeyDigest(Kvdb_Digest& digest)
    {
        entryOndisk.SetKeyDigest(digest);
    }

    bool IndexManager::InitIndexForCreateDB(uint32_t numObjects)
    {
        m_size = computeHashSizeForCreateDB(numObjects);
        
        if (!initHashTable(m_size))
        {
            return false;
        }

        return true;
    }

    bool IndexManager::LoadIndexFromDevice(uint64_t offset, uint32_t ht_size)
    {
        m_size = ht_size;

        //Read timestamp from device
        //ssize_t timeLength = Timing::GetTimeSizeOf();
        int64_t timeLength = Timing::GetTimeSizeOf();
        if (!rebuildTime(offset))
        {
            return false;
        }
        __DEBUG("Load Hashtable timestamp: %s", Timing::TimeToChar(*m_last_timestamp));
        offset += timeLength;
        
        if (!rebuildHashTable(offset))
        {
            return false;
        }
        __DEBUG("Rebuild Hashtable Success");

        return true;
    }

    bool IndexManager::rebuildTime(uint64_t offset)
    {
        //ssize_t timeLength = Timing::GetTimeSizeOf();
        int64_t timeLength = Timing::GetTimeSizeOf();
        time_t _time;
        if (m_bdev->pRead(&_time, timeLength, offset) != timeLength)
        {
            perror("Error in reading timestamp from file\n");
            return false;
        }
        m_last_timestamp->SetTime(_time);
        return true;
    }

    bool IndexManager::WriteIndexToDevice(uint64_t offset)
    {
        //Write timestamp to device
        //ssize_t timeLength = Timing::GetTimeSizeOf();
        int64_t timeLength = Timing::GetTimeSizeOf();
        if (!persistTime(offset))
        {
            return false;
        }
        __DEBUG("Write Hashtable timestamp: %s", Timing::TimeToChar(*m_last_timestamp));
        offset += timeLength;


        if (!persistHashTable(offset))
        {
            return false;
        }
        __DEBUG("Persist Hashtable Success");
    
        return true;
    }

    bool IndexManager::persistTime(uint64_t offset)
    {
        //ssize_t timeLength = Timing::GetTimeSizeOf();
        int64_t timeLength = Timing::GetTimeSizeOf();
        m_last_timestamp->Update();
        time_t _time =m_last_timestamp->GetTime();

        if (m_bdev->pWrite((void *)&_time, timeLength, offset ) != timeLength)
        {
            perror("Error write timestamp to file\n");
            return false;
        }
        return true;
    }

    bool IndexManager::UpdateIndexFromInsert(DataHeader *data_header, Kvdb_Digest *digest, uint32_t header_offset, uint64_t seg_offset)
    {
        uint64_t phy_offset = header_offset + seg_offset;
        HashEntry entry(*data_header, phy_offset, NULL);


        uint32_t hash_index = KeyDigestHandle::Hash(digest) % m_size;
        createListIfNotExist(hash_index);
        LinkedList<HashEntry> *entry_list = m_hashtable[hash_index];

        if (!entry_list->search(entry))
        {
            entry_list->insert(entry);
        }
        else
        {
            entry_list->remove(entry);
            //TODO:: need do something for gc.
            entry_list->insert(entry);
        }
        return true; 
    }

    bool IndexManager::GetHashEntry(Kvdb_Digest *digest, HashEntry &entry)
    {
        uint32_t hash_index = KeyDigestHandle::Hash(digest) % m_size;
        createListIfNotExist(hash_index);
        LinkedList<HashEntry> *entry_list = m_hashtable[hash_index];

        //entry.entryOndisk.header.key_digest = *digest;
        entry.SetKeyDigest(*digest);
        
        if (entry_list->search(entry))
        {
             vector<HashEntry> tmp_vec = entry_list->get();
             for (vector<HashEntry>::iterator iter = tmp_vec.begin(); iter!=tmp_vec.end(); iter++)
             {
                 if (iter->GetKeyDigest() == entry.GetKeyDigest())
                 {
                     entry = *iter;
                     return true;
                 }
             }
        }
        return false;
    }

    bool IndexManager::IsKeyExist(Kvdb_Digest* digest)
    {
        uint32_t hash_index = KeyDigestHandle::Hash(digest) % m_size;
        createListIfNotExist(hash_index);
        LinkedList<HashEntry> *entry_list = m_hashtable[hash_index];

        HashEntry entry;
        entry.SetKeyDigest(*digest);

        if(entry_list->search(entry))
        {
            return true;
        }
        return false;
    }

    uint64_t IndexManager::GetIndexSizeOnDevice(uint32_t ht_size)
    {
        uint64_t index_size = sizeof(time_t) + sizeof(HashEntryOnDisk) * ht_size;
        uint64_t index_size_pages = index_size / getpagesize();
        return (index_size_pages + 1) * getpagesize();
    }


    IndexManager::IndexManager(BlockDevice* bdev):
        m_hashtable(NULL), m_size(0), m_bdev(bdev)
    {
        m_last_timestamp = new Timing();
        return ;
    }

    IndexManager::~IndexManager()
    {
        if (m_last_timestamp)
        {
            delete m_last_timestamp;
        }
        if (m_hashtable)
        {
            destroyHashTable();
        }
    }

    uint32_t IndexManager::computeHashSizeForCreateDB(uint32_t number)
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

    void IndexManager::createListIfNotExist(uint32_t index)
    {
        if (!m_hashtable[index])
        {
            LinkedList<HashEntry> *entry_list = new LinkedList<HashEntry>;
            m_hashtable[index] = entry_list;
        }
        return;
    }

    bool IndexManager::initHashTable(uint32_t size)
    {
        m_hashtable =  new LinkedList<HashEntry>*[m_size];

        for (uint32_t i = 0; i < size; i++)
        {
            m_hashtable[i]=NULL;
        }
        return true;
    }

    void IndexManager::destroyHashTable()
    {
        for (uint32_t i = 0; i < m_size; i++)
        {
            if (m_hashtable[i])
            {
                delete m_hashtable[i];
                m_hashtable[i] = NULL;
            }
        }
        delete[] m_hashtable;
        m_hashtable = NULL;
        return;
    }

    bool IndexManager::rebuildHashTable(uint64_t offset)
    {
        //Init hashtable
        if (!initHashTable(m_size))
        {
            return false;
        }
        
        __DEBUG("initHashTable success");

        //Read hashtable 
        uint64_t table_length = sizeof(int) * m_size;
        int* counter = new int[m_size];
        if (!loadDataFromDevice((void*)counter, table_length, offset))
        {
            return false;
        }
        offset += table_length;
        __DEBUG("Read hashtable success");

        //Read all hash_entry
        uint64_t length = sizeof(HashEntryOnDisk) * m_size;
        HashEntryOnDisk *entry_ondisk = new HashEntryOnDisk[m_size];
        if (!loadDataFromDevice((void*)entry_ondisk, length, offset))
        {
            return false;
        }
        offset += length;
        __DEBUG("Read all hash_entry success");
       
        //Convert hashtable from device to memory
        if (!convertHashEntryFromDiskToMem(counter, entry_ondisk))
        {
            return false;
        }
        __DEBUG("rebuild hash_table success");

        delete[] entry_ondisk;
        delete[] counter;

        return true; 
    }

    bool IndexManager::loadDataFromDevice(void* data, uint64_t length, uint64_t offset)
    {
        //ssize_t nread;
        int64_t nread;
        uint64_t h_offset = 0;
        while ((nread = m_bdev->pRead((uint64_t *)data + h_offset, length, offset)) > 0){
            length -= nread;
            offset += nread;
            h_offset += nread;
            if (nread < 0)
            {
                perror("Error in reading hashtable from file\n");
                return false;
            }
        }
        return true;
    }

    bool IndexManager::writeDataToDevice(void* data, uint64_t length, uint64_t offset)
    {
        //ssize_t nwrite;
        int64_t nwrite;
        uint64_t h_offset = 0;
        while ((nwrite = m_bdev->pWrite((uint64_t *)data + h_offset, length, offset)) > 0){
            length -= nwrite;
            offset += nwrite;
            h_offset += nwrite;
            if (nwrite < 0)
            {
                perror("Error in reading hashtable from file\n");
                return false;
            }
        }
        return true;
    }

    bool IndexManager::convertHashEntryFromDiskToMem(int* counter, HashEntryOnDisk* entry_ondisk)
    {
        //Convert hashtable from device to memory
        int entry_index = 0;
        for (uint32_t i = 0; i < m_size; i++)
        {
            
            int entry_num = counter[i];
            for (int j = 0; j < entry_num; j++)
            {
                HashEntry entry(entry_ondisk[entry_index], 0);

                createListIfNotExist(i);
                m_hashtable[i]->insert(entry);
                entry_index++;
            }
            if (entry_num > 0)
            {
                __DEBUG("read hashtable[%d]=%d", i, entry_num);
            }
        }
        return true;
    }

    bool IndexManager::persistHashTable(uint64_t offset)
    {
        uint64_t entry_total = 0;

        //write hashtable to device
        uint64_t table_length = sizeof(int) * m_size;
        int* counter = new int[m_size];
        for (uint32_t i = 0; i < m_size; i++)
        {
            counter[i] = (m_hashtable[i]? m_hashtable[i]->get_size(): 0);
            entry_total += counter[i];
        }
        if (!writeDataToDevice((void*)counter, table_length, offset))
        {
            return false;
        }
        offset += table_length;
        delete[] counter;
        __DEBUG("write hashtable to device success");


        //write hash entry to device
        uint64_t length = sizeof(HashEntryOnDisk) * entry_total;
        HashEntryOnDisk *entry_ondisk = new HashEntryOnDisk[entry_total];
        int entry_index = 0;
        for (uint32_t i = 0; i < m_size; i++)
        {
            if (!m_hashtable[i])
            {
                continue;
            }
            vector<HashEntry> tmp_vec = m_hashtable[i]->get();
            for (vector<HashEntry>::iterator iter = tmp_vec.begin(); iter!=tmp_vec.end(); iter++)
            {
                entry_ondisk[entry_index++] = (iter->GetEntryOnDisk());
            }
        }

        if (!writeDataToDevice((void *)entry_ondisk, length, offset))
        {
            return false;
        }
        delete[] entry_ondisk;
        __DEBUG("write all hash entry to device success");

        return true;
    
    }
}

