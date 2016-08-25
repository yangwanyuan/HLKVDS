/* -*- Mode: C++; c-basic-offset: 4; indent-tabs-mode: nil -*- */
#include <stdio.h>
#include <string.h>
#include <inttypes.h>
#include <thread>

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

        if (!ds->m_sb_mgr->InitSuperBlockForCreateDB())
        {
            delete ds;
            return NULL;
        }

        if (!ds->m_idx_mgr->InitIndexForCreateDB(hash_table_size))
        {
            delete ds;
            return NULL;
        }

        device_size = ds->m_bdev->GetDeviceCapacity();
        if (!ds->m_seg_mgr->InitSegmentForCreateDB(device_size,
                                                          db_meta_size, 
                                                          segment_size))
        {
            delete ds;
            return NULL;
        }


        hash_table_size = ds->m_idx_mgr->GetHashTableSize();
        number_segments = ds->m_seg_mgr->GetNumberOfSeg();
        db_data_size = ds->m_seg_mgr->GetDataRegionSize();
        db_size = db_meta_size + db_data_size;

        DBSuperBlock sb(MAGIC_NUMBER, hash_table_size, num_entries, 
                        deleted_entries, segment_size, number_segments, 
                        0, db_sb_size, db_index_size, db_data_size, 
                        device_size);

        ds->m_sb_mgr->SetSuperBlock(sb);

        __INFO("\nCreateKvdbDS table information:\n"
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
               "\t Total Device Size         : %ld Bytes",
               hash_table_size, num_entries, deleted_entries,
               segment_size, number_segments, db_sb_size, 
               db_index_size, db_meta_size, db_data_size, 
               db_size, device_size);

        ds->startThreads();

        return ds; 

    }


    bool KvdbDS::writeMetaDataToDevice()
    {
        uint64_t offset = 0;
        if (!m_sb_mgr->WriteSuperBlockToDevice(offset))
        {
            return false;
        }

        offset = SuperBlockManager::GetSuperBlockSizeOnDevice();
        if (!m_idx_mgr->WriteIndexToDevice(offset))
        {
            return false;
        }
        return true;
    }

    bool KvdbDS::readMetaDataFromDevice()
    {
        __INFO("\nReading hashtable from file...");
        

        uint64_t offset = 0;
        if (!m_sb_mgr->LoadSuperBlockFromDevice(offset))
        {
            return false;
        }

        DBSuperBlock sb = m_sb_mgr->GetSuperBlock();

        offset += SuperBlockManager::GetSuperBlockSizeOnDevice();

        if (!m_idx_mgr->LoadIndexFromDevice(offset, sb.hashtable_size))
        {
            return false;
        }

        offset += sb.db_index_size;
        if (!m_seg_mgr->LoadSegmentTableFromDevice(offset, sb.segment_size, sb.number_segments, sb.current_segment))
        {
            return false;
        }



        return true;
    }


    KvdbDS* KvdbDS::Open_KvdbDS(const char* filename)
    {
        KvdbDS* ds = new KvdbDS(filename);
        if (!ds->openDB())
        {
            delete ds;
            return NULL;
        }
        return ds;
    }

    bool KvdbDS::openDB()
    {
        if (m_bdev->Open(m_filename) < 0)
        {
            __ERROR("Could not open device\n");
            return false;
        }
        if (!readMetaDataFromDevice())
        {
            __ERROR("Could not read  hash table file\n");
            return false;
        }
        startThreads();
        return true;
    }

    bool KvdbDS::closeDB()
    {
        if (!writeMetaDataToDevice())
        {
            __ERROR("Could not to write metadata to device\n");
            return false;
        }
        stopThreads();
        return true;
    }

    void KvdbDS::startThreads()
    {
        m_stop_reqs_t = false;
        m_reqs_t.Start();
    }

    void KvdbDS::stopThreads()
    {
        m_stop_reqs_t = true;
        m_reqs_t.Join();
    }

    KvdbDS::~KvdbDS()
    {
        closeDB();
        delete m_idx_mgr;
        delete m_sb_mgr;
        delete m_seg_mgr;
        delete m_bdev;
        delete m_reqsQueue_mutex;

    }

    KvdbDS::KvdbDS(const string& filename) :
        m_filename(filename), m_reqs_t(this), m_stop_reqs_t(false)
    {
        m_bdev = BlockDevice::CreateDevice();
        m_seg_mgr = new SegmentManager(m_bdev);
        m_sb_mgr = new SuperBlockManager(m_bdev);
        m_idx_mgr = new IndexManager(m_bdev);

        m_reqsQueue_mutex = new Mutex;
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

        //if (!m_idx_mgr->IsKeyExist(&digest))
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
        if (!slice.ComputeDigest())
        {
            return false;
        }
        Request *req = new Request(slice);
        m_reqsQueue_mutex->Lock();
        m_reqs_list.push_back(req);
        m_reqsQueue_mutex->Unlock();

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

        //if (!m_idx_mgr->IsKeyExist(&digest))
        //{
        //    return true;
        //}

        //if (!updateExistKey(&digest, NULL, 0))
        //{
        //    return false;
        //}

        //
        //DBSuperBlock sb = m_sb_mgr->GetSuperBlock();
        //sb.deleted_elements++;
        //sb.number_elements--;
        //m_sb_mgr->SetSuperBlock(sb);
        //
        //return true;
        ////----------  sync write end   ----------------------/

        //----------  async write begin ----------------------/
        KVSlice slice(key, key_len, NULL, 0);
        if (!slice.ComputeDigest())
        {
            return false;
        }
        Request *req = new Request(slice);
        m_reqsQueue_mutex->Lock();
        m_reqs_list.push_back(req);
        m_reqsQueue_mutex->Unlock();

        req->Wait();

        DBSuperBlock sb = m_sb_mgr->GetSuperBlock();
        sb.deleted_elements++;
        sb.number_elements--;
        m_sb_mgr->SetSuperBlock(sb);

        bool result  = req->GetState();
        delete req;
        return result;
        //----------  async write end ----------------------/
    }

    bool KvdbDS::Get(const char* key, uint32_t key_len, string &data) 
    {
        if (key == NULL)
        {
            return false;
        }

        KVSlice slice(key, key_len, NULL, 0);
        if (!slice.ComputeDigest())
        {
            return false;
        }

        if (!m_idx_mgr->IsKeyExist(&slice))
        {
            return false;
        }

        HashEntry entry;
        m_idx_mgr->GetHashEntry(&slice, entry);

        if (entry.GetDataSize() == 0 )
        {
            return false;
        }

        if (!readData(&entry, data))
        {
            return false;
        }

        return true;
    }

    bool KvdbDS::insertNewKey(const KVSlice *slice)
    {
        if (m_sb_mgr->IsElementFull())
        {
            __ERROR("Error: hash table full!\n");
            return false;
        }

        if (!writeData(slice))
        {
            __ERROR("Can't write to underlying datastore\n");
            return false;
        }

        DBSuperBlock sb = m_sb_mgr->GetSuperBlock();
        sb.number_elements++;
        m_sb_mgr->SetSuperBlock(sb);

        return true;
    }

    bool KvdbDS::updateExistKey(const KVSlice *slice)
    {
        if (m_sb_mgr->IsElementFull())
        {
            __ERROR("Error: hash table full!\n");
            return false;
        }

        if (!writeData(slice))
        {
            __ERROR("Can't write to underlying datastore\n");
            return false;
        }

        //TODO: do something for gc

        return true;
    }

    bool KvdbDS::readData(HashEntry* entry, string &data)
    {
        uint16_t data_len = entry->GetDataSize();
        if (data_len == 0)
        {
            return true;
        }

        uint64_t data_offset = 0;
        if (!m_seg_mgr->ComputeDataOffsetPhyFromEntry(entry, data_offset))
        {
            return false;
        }

        char *mdata = new char[data_len];
        if (m_bdev->pRead(mdata, data_len, data_offset) != (ssize_t)data_len)
        {
            __ERROR("Could not read data at position");
            delete[] mdata;
            return false;
        }
        data.assign(mdata, data_len);
        delete[] mdata;

        __DEBUG("get data offset %ld", data_offset);

        return true;
    }

    bool KvdbDS::writeData(const KVSlice *slice)
    {
        uint32_t seg_id = 0;
        if (!m_seg_mgr->GetEmptySegId(seg_id))
        {
            __ERROR("Cann't get a new Empty Segment.\n");
            return false;
        }

        uint64_t seg_offset;
        if (!m_seg_mgr->ComputeSegOffsetFromId(seg_id, seg_offset))
        {
            __ERROR("Cann't compute segment offset from id: %d.\n", seg_id);
            return false;
        }

        SegmentSlice segSlice(seg_id, m_seg_mgr);
        DataHeader data_header;
        segSlice.Put(slice, data_header);

        if (m_bdev->pWrite(segSlice.GetSlice(), segSlice.GetLength(), seg_offset) != segSlice.GetLength()) {
            __ERROR("Could  write data to device: %s\n", strerror(errno));
            return false;
        }


        m_seg_mgr->Update(seg_id);
        m_idx_mgr->UpdateIndexFromInsert(&data_header, &slice->GetDigest(), sizeof(SegmentOnDisk), seg_offset);
        DBSuperBlock sb = m_sb_mgr->GetSuperBlock();
        sb.current_segment = seg_id;
        m_sb_mgr->SetSuperBlock(sb);


        __DEBUG("write seg_id:%u, seg_offset: %lu, head_offset: %ld, data_offset:%u, header_len:%ld, data_len:%u", 
                seg_id, seg_offset, sizeof(SegmentOnDisk), data_header.GetDataOffset(), sizeof(DataHeader), slice->GetDataLen());

        return true;
    }

    void KvdbDS::ReqsThreadEntry()
    {
        __DEBUG("Requests thread start!!");
        while (true)
        {
            while (!m_reqs_list.empty())
            {
                bool result = false;

                m_reqsQueue_mutex->Lock();
                Request *req = m_reqs_list.front();

                const KVSlice *slice = &req->GetSlice();

                if (!m_idx_mgr->IsKeyExist(slice))
                {
                    result = insertNewKey(slice);
                }
                else
                {
                    result = updateExistKey(slice);
                }

                req->SetState(result);
                req->Done();
                m_reqs_list.pop_front();
                m_reqsQueue_mutex->Unlock();
                __DEBUG("Requests thread get req:  The key is %s", (req->GetSlice().GetKeyStr()).c_str());
                req->Signal();
            }

            if (m_stop_reqs_t)
            {
                break;
            }

            std::this_thread::yield();
        }
        __DEBUG("Requests thread stop!!");
    }

} //namespace kvdb
