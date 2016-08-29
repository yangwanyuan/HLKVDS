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
        r = ds->bdev_->CreateNewDB(filename, db_meta_size);
        if (r < 0)
        {
            delete ds;
            return NULL;
        }

        if (!ds->sbMgr_->InitSuperBlockForCreateDB())
        {
            delete ds;
            return NULL;
        }

        if (!ds->idxMgr_->InitIndexForCreateDB(hash_table_size))
        {
            delete ds;
            return NULL;
        }

        device_size = ds->bdev_->GetDeviceCapacity();
        if (!ds->segMgr_->InitSegmentForCreateDB(device_size,
                                                          db_meta_size, 
                                                          segment_size))
        {
            delete ds;
            return NULL;
        }


        hash_table_size = ds->idxMgr_->GetHashTableSize();
        number_segments = ds->segMgr_->GetNumberOfSeg();
        db_data_size = ds->segMgr_->GetDataRegionSize();
        db_size = db_meta_size + db_data_size;

        DBSuperBlock sb(MAGIC_NUMBER, hash_table_size, num_entries, 
                        deleted_entries, segment_size, number_segments, 
                        0, db_sb_size, db_index_size, db_data_size, 
                        device_size);

        ds->sbMgr_->SetSuperBlock(sb);

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

        ds->startThds();

        return ds; 

    }


    bool KvdbDS::writeMetaDataToDevice()
    {
        uint64_t offset = 0;
        if (!sbMgr_->WriteSuperBlockToDevice(offset))
        {
            return false;
        }

        offset = SuperBlockManager::GetSuperBlockSizeOnDevice();
        if (!idxMgr_->WriteIndexToDevice(offset))
        {
            return false;
        }
        return true;
    }

    bool KvdbDS::readMetaDataFromDevice()
    {
        __INFO("\nReading hashtable from file...");
        

        uint64_t offset = 0;
        if (!sbMgr_->LoadSuperBlockFromDevice(offset))
        {
            return false;
        }

        DBSuperBlock sb = sbMgr_->GetSuperBlock();

        offset += SuperBlockManager::GetSuperBlockSizeOnDevice();

        if (!idxMgr_->LoadIndexFromDevice(offset, sb.hashtable_size))
        {
            return false;
        }

        offset += sb.db_index_size;
        if (!segMgr_->LoadSegmentTableFromDevice(offset, sb.segment_size, sb.number_segments, sb.current_segment))
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
        if (bdev_->Open(fileName_) < 0)
        {
            __ERROR("Could not open device\n");
            return false;
        }
        if (!readMetaDataFromDevice())
        {
            __ERROR("Could not read  hash table file\n");
            return false;
        }
        startThds();
        return true;
    }

    bool KvdbDS::closeDB()
    {
        if (!writeMetaDataToDevice())
        {
            __ERROR("Could not to write metadata to device\n");
            return false;
        }
        stopThds();
        return true;
    }

    void KvdbDS::startThds()
    {
        segThd_stop_ = false;
        segThd_.Start();
    }

    void KvdbDS::stopThds()
    {
        segThd_stop_ = true;
        segThd_.Join();
    }

    KvdbDS::~KvdbDS()
    {
        closeDB();
        delete idxMgr_;
        delete sbMgr_;
        delete segMgr_;
        delete bdev_;
        delete segQueMtx_;

    }

    KvdbDS::KvdbDS(const string& filename) :
        fileName_(filename), segThd_(this), segThd_stop_(false)
    {
        bdev_ = BlockDevice::CreateDevice();
        segMgr_ = new SegmentManager(bdev_);
        sbMgr_ = new SuperBlockManager(bdev_);
        idxMgr_ = new IndexManager(bdev_);

        segQueMtx_ = new Mutex;
    }


    bool KvdbDS::Insert(const char* key, uint32_t key_len, const char* data, uint16_t length)
    {
        if (key == NULL)
        {
            return false;
        }

        KVSlice slice(key, key_len, data, length);
        slice.ComputeDigest();

        if (!idxMgr_->IsKeyExist(&slice))
        {
            return insertKey(slice, INSERT);
        }
        else
        {
            return insertKey(slice, UPDATE);
        }
    }

    bool KvdbDS::Delete(const char* key, uint32_t key_len)
    {
        if (key == NULL)
        {
            return true;
        }

        KVSlice slice(key, key_len, NULL, 0);
        slice.ComputeDigest();

        if (!idxMgr_->IsKeyExist(&slice))
        {
            return true;
        }
        return insertKey(slice, DELETE);

    }

    bool KvdbDS::Get(const char* key, uint32_t key_len, string &data) 
    {
        if (key == NULL)
        {
            return false;
        }

        KVSlice slice(key, key_len, NULL, 0);
        slice.ComputeDigest();

        if (!idxMgr_->IsKeyExist(&slice))
        {
            return false;
        }

        HashEntry entry;
        idxMgr_->GetHashEntry(&slice, entry);

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

    bool KvdbDS::insertKey(KVSlice& slice, OpType op_type)
    {
        if (sbMgr_->IsElementFull())
        {
            __ERROR("Error: hash table full!\n");
            return false;
        }

        Request *req = new Request(slice);

        if (!writeData(req))
        {
            __ERROR("Can't write to underlying datastore\n");
            delete req;
            return false;
        }

        req->Wait();
        bool result = req->GetState();

        if (result)
        {
            updateMeta(req, op_type);
        }

        delete req;
        return result;
    }

    void KvdbDS::updateMeta(Request *req, OpType op_type)
    {
        switch (op_type)
        {
            case INSERT:
            {
                //Update new key meta
                DBSuperBlock sb = sbMgr_->GetSuperBlock();
                sb.number_elements++;
                sbMgr_->SetSuperBlock(sb);
            }
            case UPDATE:
            {
                //Update existed key meta
                //TODO: do something for gc
                ;
            }
            case DELETE:
            {
                //Update deleted key meta
                DBSuperBlock sb = sbMgr_->GetSuperBlock();
                sb.deleted_elements++;
                sb.number_elements--;
                sbMgr_->SetSuperBlock(sb);
                //TODO: do something for gc
            }
            default:
                break;
        }
    }


    bool KvdbDS::readData(HashEntry* entry, string &data)
    {
        uint16_t data_len = entry->GetDataSize();
        if (data_len == 0)
        {
            return true;
        }

        uint64_t data_offset = 0;
        if (!segMgr_->ComputeDataOffsetPhyFromEntry(entry, data_offset))
        {
            return false;
        }

        char *mdata = new char[data_len];
        if (bdev_->pRead(mdata, data_len, data_offset) != (ssize_t)data_len)
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

    bool KvdbDS::writeData(Request *req)
    {
        segQueMtx_->Lock();
        uint32_t seg_id = 0;
        if (!segMgr_->GetEmptySegId(seg_id))
        {
            __ERROR("Cann't get a new Empty Segment.\n");
            return false;
        }
        SegmentData *seg = new SegmentData(seg_id, segMgr_, bdev_);
        segQue_.push_back(seg);
        segQueMtx_->Unlock();

        uint64_t seg_offset = seg->GetSegPhyOffset();
        DataHeader data_header;
        seg->Put(req, data_header);

        //segMgr_->Update(seg_id);
        idxMgr_->UpdateIndexFromInsert(&data_header, &req->GetSlice().GetDigest(), sizeof(SegmentOnDisk), seg_offset);
        DBSuperBlock sb = sbMgr_->GetSuperBlock();
        sb.current_segment = seg_id;
        sbMgr_->SetSuperBlock(sb);

        seg->Complete();


        __DEBUG("write seg_id:%u, seg_offset: %lu, head_offset: %ld, data_offset:%u, header_len:%ld, data_len:%u", 
                seg_id, seg_offset, sizeof(SegmentOnDisk), data_header.GetDataOffset(), sizeof(DataHeader), req->GetSlice().GetDataLen());

        return true;
    }

    void KvdbDS::SegThdEntry()
    {
        __DEBUG("Segment write thread start!!");
        while (true)
        {
            while (!segQue_.empty())
            {
                segQueMtx_->Lock();
                SegmentData *seg = segQue_.front();
                segQueMtx_->Unlock();
                if (!seg->IsCompleted())
                {
                    if (seg->IsExpired())
                    {
                        seg->Complete();
                    }
                    else
                    {
                        break;
                    }
                }

                seg->WriteSegToDevice();

                segQueMtx_->Lock();
                segQue_.pop_front();
                segQueMtx_->Unlock();

                __DEBUG("Segment thread write seg to device, seg_id:%d", seg->GetSegId());

                //Update Segmenttable
                segMgr_->Update(seg->GetSegId());
                delete seg;
            }

            if (segThd_stop_)
            {
                break;
            }

            std::this_thread::yield();
        }
        __DEBUG("Segment write thread stop!!");
    }

} //namespace kvdb
