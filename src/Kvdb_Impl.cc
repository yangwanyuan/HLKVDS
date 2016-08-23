/* -*- Mode: C++; c-basic-offset: 4; indent-tabs-mode: nil -*- */
#include <stdio.h>
#include <string.h>
#include <inttypes.h>
#include <thread>

//#ifndef __STDC_FORMAT_MACROS
//#define __STDC_FORMAT_MACROS
//#endif

#include "Kvdb_Impl.h"
#include "KeyDigestHandle.h"


namespace kvdb {

    KvdbDS* KvdbDS::Create_KvdbDS(const char* filename,
                                    uint32_t hash_table_size,
                                    uint32_t segment_size)
    {
        if (filename == NULL)
        {
            return NULL;
        }

        uint32_t num_entries = 0;
        uint32_t deleted_entries = 0;
        uint32_t number_segments = 0;
        uint64_t db_sb_size = 0;
        uint64_t db_index_size = 0;
        uint64_t db_meta_size = 0;
        uint64_t db_data_size = 0;
        uint64_t db_size = 0;
        uint64_t device_size = 0;
        
        KvdbDS* ds = new KvdbDS(filename);

        db_sb_size = SuperBlockManager::GetSuperBlockSizeOnDevice();
        db_index_size = IndexManager::GetIndexSizeOnDevice(hash_table_size);
        db_meta_size = db_sb_size + db_index_size;

        int r = 0;
        r = ds->m_bdev->CreateNewDB(filename, db_meta_size);
        if (r < 0)
        {
            delete ds;
            return NULL;
        }

        if (!ds->m_sb_manager->InitSuperBlockForCreateDB())
        {
            delete ds;
            return NULL;
        }

        if (!ds->m_index_manager->InitIndexForCreateDB(hash_table_size))
        {
            delete ds;
            return NULL;
        }

        device_size = ds->m_bdev->GetDeviceCapacity();
        if (!ds->m_segment_manager->InitSegmentForCreateDB(device_size,
                                                          db_meta_size, 
                                                          segment_size))
        {
            delete ds;
            return NULL;
        }


        hash_table_size = ds->m_index_manager->GetHashTableSize();
        number_segments = ds->m_segment_manager->GetNumberOfSeg();
        db_data_size = ds->m_segment_manager->GetDataRegionSize();
        db_size = db_meta_size + db_data_size;

        DBSuperBlock sb(MAGIC_NUMBER, hash_table_size, num_entries, 
                        deleted_entries, segment_size, number_segments, 
                        0, db_sb_size, db_index_size, db_data_size, 
                        device_size);

        ds->m_sb_manager->SetSuperBlock(sb);

        printf("CreateKvdbDS table information:\n"
               "\t hashtable_size            : %d\n"
               "\t num_entries               : %d\n"
               "\t deleted_entries           : %d\n"
               "\t segment_size              : %d Bytes\n"
               "\t number_segments           : %d\n"
               "\t Database Superblock Size  : %ld Bytes\n"
               "\t Database Index Size       : %ld Bytes\n"
               "\t Total DB Meta Region Size : %ld Bytes\n"
               "\t Total DB Data Region Size : %ld Bytes\n"
               "\t Total DB Total Size       : %ld Bytes\n"
               "\t Total Device Size         : %ld Bytes\n",
               hash_table_size, num_entries, deleted_entries,
               segment_size, number_segments, db_sb_size, 
               db_index_size, db_meta_size, db_data_size, 
               db_size, device_size);

        ds->startThreads();

        return ds; 

    }


    bool KvdbDS::WriteMetaDataToDevice()
    {
        uint64_t offset = 0;
        if (!m_sb_manager->WriteSuperBlockToDevice(offset))
        {
            return false;
        }

        offset = SuperBlockManager::GetSuperBlockSizeOnDevice();
        if (!m_index_manager->WriteIndexToDevice(offset))
        {
            return false;
        }
        return true;
    }

    bool KvdbDS::ReadMetaDataFromDevice()
    {
        printf("Reading hashtable from file...\n");
        

        uint64_t offset = 0;
        //m_sb_manager = new SuperBlockManager(m_bdev);
        if (!m_sb_manager->LoadSuperBlockFromDevice(offset))
        {
            return false;
        }


        DBSuperBlock sb = m_sb_manager->GetSuperBlock();
        //m_index_manager = new IndexManager(m_bdev);

        offset += SuperBlockManager::GetSuperBlockSizeOnDevice();

        if (!m_index_manager->LoadIndexFromDevice(offset, sb.hashtable_size))
        {
            return false;
        }

        offset += sb.db_index_size;
        //m_segment_manager = new SegmentManager(m_bdev);
        if (!m_segment_manager->LoadSegmentTableFromDevice(offset, sb.segment_size, sb.number_segments, sb.current_segment))
        {
            return false;
        }



        return true;
    }


    KvdbDS* KvdbDS::Open_KvdbDS(const char* filename)
    {
        KvdbDS* ds = new KvdbDS(filename);
        //if (!ds->reopen())
        //{
        //    delete ds;
        //    return NULL;
        //}
        if (ds->m_bdev->Open(ds->m_filename) < 0)
        {
            perror("could not open device\n");
            delete ds;
            return NULL;
        }
        if (!ds->ReadMetaDataFromDevice())
        {
            perror("could not read  hash table file\n");
            delete ds;
            return NULL;
        }
        ds->startThreads();
        return ds;
    }

    //bool KvdbDS::reopen()
    //{
    //    int r = 0;
    //    r = m_bdev->Open(m_filename);
    //    if (r < 0)
    //    {
    //        perror("could not open device\n");
    //        return false;
    //    }

    //    if (m_sb_manager)
    //    {
    //        delete m_sb_manager;
    //        m_sb_manager = NULL;
    //    }

    //    if (m_index_manager)
    //    {
    //        delete m_index_manager;
    //        m_index_manager = NULL;
    //    }

    //    if (m_segment_manager)
    //    {
    //        delete m_segment_manager;
    //        m_segment_manager = NULL;
    //    }

    //    if (m_data_handle)
    //    {
    //        delete m_data_handle;
    //        m_data_handle = NULL;
    //    }

    //    //if (m_reqs_t)
    //    //{
    //    //    delete m_reqs_t;
    //    //    m_reqs_t = NULL;
    //    //}

    //    if (!this->ReadMetaDataFromDevice())
    //    {
    //        perror("could not read  hash table file\n");
    //        return false;
    //    }

    //    m_data_handle = new DataHandle(m_bdev, m_sb_manager, m_index_manager, m_segment_manager);

    //    //m_reqs_t =  ReqsThread(this);
    //    startThreads();
    //    return true;

    //}

    void KvdbDS::startThreads()
    {
        //startReqsThread();
        //m_reqs_t.Kill();
        m_stop_reqs_t = false;
        m_reqs_t.Start();
        //m_reqs_t.Detach();
        //m_reqs_t.Join();
    }

    void KvdbDS::stopThreads()
    {
        m_stop_reqs_t = true;
        m_reqs_t.Join();
    }

    KvdbDS::~KvdbDS()
    {
        if (!WriteMetaDataToDevice())
        {
            perror("could not to write metadata to device\n");
        }
        delete m_index_manager;
        delete m_sb_manager;
        delete m_data_handle;
        delete m_segment_manager;
        delete m_bdev;

        stopThreads();

        delete m_reqs_mutex;

    }

    KvdbDS::KvdbDS(const string& filename) :
        //m_filename(filename)
        m_filename(filename), m_reqs_t(this), m_stop_reqs_t(false)
    {
        m_bdev = BlockDevice::CreateDevice();
        m_segment_manager = new SegmentManager(m_bdev);
        m_sb_manager = new SuperBlockManager(m_bdev);
        m_index_manager = new IndexManager(m_bdev);
        m_data_handle = new DataHandle(m_bdev, m_sb_manager, m_index_manager, m_segment_manager);

        m_reqs_mutex = new Mutex;
    }


    bool KvdbDS::Insert(const char* key, uint32_t key_len, const char* data, uint16_t length)
    {
        if (key == NULL)
        {
            return false;
        }

        ////----------  sync write begin ----------------------/
        //Kvdb_Key vkey(key, key_len);
        //Kvdb_Digest digest;
        //KeyDigestHandle::ComputeDigest(&vkey, digest);

        //if (!m_index_manager->IsKeyExist(&digest))
        //{
        //    if (!insertNewKey(&digest, data, length))
        //    {
        //        return false;
        //    }
        //}
        //else
        //{
        //    if (!updateExistKey(&digest, data, length))
        //    {
        //        return false;
        //    }
        //}
        //return true;
        ////----------  sync write end   ----------------------/

        //----------  async write begin ----------------------/
        KVSlice slice(key, key_len, data, length);
        slice.ComputeDigest();
        Request *req = new Request(slice);
        m_reqs_mutex->Lock();
        m_reqs.push_back(req);
        m_reqs_mutex->Unlock();

        req->Wait();
        bool result = req->GetState();
        delete req;
        return result;
        //----------  async write end ----------------------/
    }

    bool KvdbDS::Delete(const char* key, uint32_t key_len)
    {
        if (key == NULL)
        {
            return true;
        }

        ////----------  sync write begin ----------------------/
        //Kvdb_Key vkey(key, key_len);
        //Kvdb_Digest digest;
        //KeyDigestHandle::ComputeDigest(&vkey, digest);

        //if (!m_index_manager->IsKeyExist(&digest))
        //{
        //    return true;
        //}

        //if (!updateExistKey(&digest, NULL, 0))
        //{
        //    return false;
        //}

        //
        //DBSuperBlock sb = m_sb_manager->GetSuperBlock();
        //sb.deleted_elements++;
        //sb.number_elements--;
        //m_sb_manager->SetSuperBlock(sb);
        //
        //return true;
        ////----------  sync write end   ----------------------/

        //----------  async write begin ----------------------/
        KVSlice slice(key, key_len, NULL, 0);
        slice.ComputeDigest();
        Request *req = new Request(slice);
        m_reqs_mutex->Lock();
        m_reqs.push_back(req);
        m_reqs_mutex->Unlock();

        req->Wait();

        DBSuperBlock sb = m_sb_manager->GetSuperBlock();
        sb.deleted_elements++;
        sb.number_elements--;
        m_sb_manager->SetSuperBlock(sb);

        bool result  = req->GetState();
        delete req;
        return result;
        //----------  async write end ----------------------/
    }

    bool KvdbDS::Get(const char* key, uint32_t key_len, string &data) const
    {
        if (key == NULL)
        {
            return false;
        }

        Kvdb_Key vkey(key, key_len);
        Kvdb_Digest digest;
        KeyDigestHandle::ComputeDigest(&vkey, digest);

        if (!m_index_manager->IsKeyExist(&digest))
        {
            return false;
        }

        HashEntry entry;
        m_index_manager->GetHashEntry(&digest, entry);

        if (entry.GetDataSize() == 0 )
        {
            return false;
        }

        if (!m_data_handle->ReadData(&entry, data))
        {
            return false;
        }

        return true;
    }

    bool KvdbDS::insertNewKey(Kvdb_Digest* digest, const char* data, uint16_t length)
    {
        if (m_sb_manager->IsElementFull())
        {
            fprintf(stderr, "Error: hash table full!\n");
            return false;
        }

        if (!m_data_handle->WriteData(*digest, data, length))
        {
            fprintf(stderr, "Can't write to underlying datastore\n");
            return false;
        }

        DBSuperBlock sb = m_sb_manager->GetSuperBlock();
        sb.number_elements++;
        m_sb_manager->SetSuperBlock(sb);

        return true;
    }

    bool KvdbDS::updateExistKey(Kvdb_Digest* digest, const char* data, uint16_t length)
    {
        if (m_sb_manager->IsElementFull())
        {
            fprintf(stderr, "Error: hash table full!\n");
            return false;
        }

        if (!m_data_handle->WriteData(*digest, data, length))
        {
            fprintf(stderr, "Can't write to underlying datastore\n");
            return false;
        }

        //TODO: do something for gc

        return true;
    }

    void* KvdbDS::ReqsThreadEntry()
    {
        __DEBUG("requests thread startttttttttttttttt!!");
        bool result = false;
        while (!m_stop_reqs_t)
        //while (m_reqs_t.m_running)
        {
            if (!m_reqs.empty())
            {
                m_reqs_mutex->Lock();
                Request *req = m_reqs.front();

                Kvdb_Digest *digest = (Kvdb_Digest *)&req->GetSlice().GetDigest();

                if (!m_index_manager->IsKeyExist(digest))
                {
                    result = insertNewKey(digest, req->GetSlice().GetData(), req->GetSlice().GetDataLen()); 
                }
                else
                {
                    result = updateExistKey(digest, req->GetSlice().GetData(), req->GetSlice().GetDataLen());
                }

                req->SetState(result);
                req->Done();
                m_reqs.pop_front();
                m_reqs_mutex->Unlock();
                __DEBUG("!!!!!!!!Requests thread get req:  The key is %s", (req->GetSlice().GetKeyStr()).c_str());
                req->Signal();
            }
            else
            {
                std::this_thread::yield();
            }
        }
        __DEBUG("requests thread stopppppppppppppppp!!");
        return NULL;
    }

}
