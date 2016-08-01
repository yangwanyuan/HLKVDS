/* -*- Mode: C++; c-basic-offset: 4; indent-tabs-mode: nil -*- */
#include <stdio.h>
#include <string.h>
#include <inttypes.h>

#ifndef __STDC_FORMAT_MACROS
#define __STDC_FORMAT_MACROS
#endif

#include "Kvdb_Impl.h"
#include "KeyDigestHandle.h"


namespace kvdb {

    KvdbDS* KvdbDS::Create_KvdbDS(const char* filename,
                                        uint64_t hash_table_size,
                                        double max_deleted_ratio,
                                        double max_load_factor,
                                        uint64_t segment_size)
    {
        if (filename == NULL)
            return NULL;

        uint64_t num_entries = 0;
        uint64_t deleted_entries = 0;
        //uint64_t segment_size = 0;
        uint64_t number_segments = 0;
        uint64_t max_entries = hash_table_size * KvdbDS::EXCESS_BUCKET_FACTOR;

        KvdbDS* ds = new KvdbDS(filename);

        uint64_t db_meta_size = SuperBlockManager::GetSuperBlockSizeOnDevice() + IndexManager::GetIndexSizeOnDevice(max_entries);

        int r = 0;
        r = ds->m_bdev->CreateNewDB(filename, db_meta_size);
        if (r < 0){
            delete ds;
            return NULL;
        }

        if(!ds->m_sb_manager->InitSuperBlockForCreateDB()){
            delete ds;
            return NULL;
        }

        if (!ds->m_index_manager->InitIndexForCreateDB(max_entries)){
            delete ds;
            return NULL;
        }

        hash_table_size = ds->m_index_manager->GetHashTableSize();
        // REMEMBER TO WRITE OUT HEADER/HASHTABLE AFTER SPLIT/MERGE/REWRITE!

        // populate the database header.
        DBSuperBlock sb = {MAGIC_NUMBER,
                            hash_table_size, 
                            num_entries, 
                            deleted_entries, 
                            max_deleted_ratio, 
                            max_load_factor, 
                            (off_t)db_meta_size, 
                            (off_t)db_meta_size,
                            segment_size,
                            number_segments};

        ds->m_sb_manager->SetSuperBlock(sb);

        printf("CreateKvdbDS table information:\n"
               "\t hashtablesize: %"PRIu64"\n"
               "\t num_entries: %"PRIu64"\n"
               "\t deleted entries: %"PRIu64"\n"
               "\t Database Superblock size %"PRIu64"B\n"
               "\t Total DB Meta Region size %"PRIu64"B\n"
               "\t Maximum number of entries: %"PRIu64"\n"
               "\t Per Segment Size: %"PRIu64"\n"
               "\t Number of segments: %"PRIu64"\n",
               hash_table_size, num_entries, deleted_entries,
               SuperBlockManager::GetSuperBlockSizeOnDevice(), 
               db_meta_size, max_entries, 
               segment_size, number_segments);


        return ds; 

    }


    // Write out Malloc'd Header/Hashtable to Disk once DB values have been inserted sequentially
    bool KvdbDS::WriteMetaDataToDevice()
    {
        uint64_t offset = 0;
        if(!m_sb_manager->WriteSuperBlockToDevice(offset)){
            return false;
        }

        offset = SuperBlockManager::GetSuperBlockSizeOnDevice();
        if(!m_index_manager->WriteIndexToDevice(offset)){
            return false;
        }
        return true;
    }

    // This assumes that the file was closed properly.
    bool KvdbDS::ReadMetaDataFromDevice()
    {
        printf("Reading hashtable from file...\n");
        

        uint64_t offset = 0;
        m_sb_manager = new SuperBlockManager(m_bdev);
        if(!m_sb_manager->LoadSuperBlockFromDevice(offset)){
            return false;
        }


        DBSuperBlock sb = m_sb_manager->GetSuperBlock();
        m_index_manager = new IndexManager(m_data_handle, m_bdev);

        offset += SuperBlockManager::GetSuperBlockSizeOnDevice();

        if (!m_index_manager->LoadIndexFromDevice(offset, sb.hashtable_size)){
            return false;
        }

        return true;
    }


    KvdbDS* KvdbDS::Open_KvdbDS(const char* filename)
    {
        KvdbDS* ds = new KvdbDS(filename);
        if (!ds->ReopenKvdbDS()) {
            delete ds;
            return NULL;
        }
        return ds;
    }

    bool KvdbDS::ReopenKvdbDS()
    {
        int r = 0;
        r = m_bdev->Open(m_filename);
        if (r < 0){
            perror("could not open device\n");
            return false;
        }

        if (m_sb_manager)
        {
            delete m_sb_manager;
            m_sb_manager = NULL;
        }

        if (m_index_manager)
        {
            delete m_index_manager;
            m_index_manager = NULL;
        }

        // Now read entries from file into memory
        if (!this->ReadMetaDataFromDevice()) {
            perror("could not read  hash table file\n");
            return false;
        }
        return true;
    }


    KvdbDS::~KvdbDS()
    {
        if(!WriteMetaDataToDevice())
            perror("could not to write metadata to device\n");
        delete m_index_manager;
        delete m_sb_manager;
        delete m_bdev;
        delete m_data_handle;
    }

    KvdbDS::KvdbDS(const string& filename) :
        m_filename(filename)
    {
        m_bdev = BlockDevice::CreateDevice();
        m_data_handle = new DataHandle(m_bdev);
        m_sb_manager = new SuperBlockManager(m_bdev);
        m_index_manager = new IndexManager(m_data_handle, m_bdev);
    }


    bool KvdbDS::Insert(const char* key, uint32_t key_len, const char* data, uint32_t length)
    {
        if (key == NULL)
            return false;

        DBSuperBlock sb = m_sb_manager->GetSuperBlock();
        if (m_sb_manager->IsElementFull()) {
            fprintf(stderr, "Error: hash table full!\n");
            return false;
        }
        
        Kvdb_Key vkey;
        vkey.value = (char *)key;
        vkey.len = key_len;
        
        Kvdb_Digest digest;

        KeyDigestHandle::ComputeDigest(&vkey, digest);

        struct DataHeader data_header;
        memcpy(&(data_header.key), &(digest.value), 20);
        data_header.data_size = length;
        data_header.data_offset = sb.data_insertion_point + sizeof(struct DataHeader);
        data_header.next_header_offset = sb.data_insertion_point + sizeof(struct DataHeader) + length;


        if (!m_data_handle->WriteData(&data_header, data, length, sb.data_insertion_point)) {
            fprintf(stderr, "Can't write to underlying datastore\n");
            return false;
        }

        // update the hashtable (since the data was successfully written).
        m_index_manager->UpdateIndexFromInsert(&data_header, &digest, sb.data_insertion_point);


        sb.data_insertion_point += sizeof(struct DataHeader) + length;
        sb.number_elements++;


        m_sb_manager->SetSuperBlock(sb);

        return true;
    }

    // External deletes always write a writeLog
    bool KvdbDS::Delete(const char* key, uint32_t key_len)
    {
        return Insert(key, key_len, NULL, 0);
    }

    bool KvdbDS::Get(const char* key, uint32_t key_len, string &data) const
    {
        if (key == NULL)
            return false;

        Kvdb_Key vkey;
        vkey.value = (char *)key;
        vkey.len = key_len;
        
        Kvdb_Digest digest;

        KeyDigestHandle::ComputeDigest(&vkey, digest);

        struct HashEntry entry;

        if(!m_index_manager->GetHashEntry(&digest, entry))
            return false;

        if(entry.hash_entry.dataheader.data_size == 0 )
            return false;

        if (!m_data_handle->ReadData(&(entry.hash_entry.dataheader), data)) {
            fprintf(stderr, "Can't write to underlying datastore\n");
            return false;
        }
        __DEBUG("get data offset %d", entry.hash_entry.dataheader.data_offset);

        return true;
    }
    
}// namespace kvdb 

