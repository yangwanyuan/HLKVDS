/* -*- Mode: C++; c-basic-offset: 4; indent-tabs-mode: nil -*- */
#include <stdio.h>
#include <string.h>
#include <inttypes.h>
#include <thread>

#include "Kvdb_Impl.h"
#include "KeyDigestHandle.h"


namespace kvdb {

    KvdbDS* KvdbDS::instance_ = NULL;

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
        uint64_t offset = 0;
        if (!sbMgr_->LoadSuperBlockFromDevice(offset))
        {
            return false;
        }


        offset += sbMgr_->GetSbSize();
        if (!idxMgr_->LoadIndexFromDevice(offset, sbMgr_->GetHTSize()))
        {
            return false;
        }

        offset += sbMgr_->GetIndexSize();
        if (!segMgr_->LoadSegmentTableFromDevice(offset, sbMgr_->GetSegmentSize(), sbMgr_->GetSegmentNum(), sbMgr_->GetCurSegmentId()))
        {
            return false;
        }

        __INFO("\nReading meta information from file:\n"
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
               sbMgr_->GetHTSize(), sbMgr_->GetElementNum(),
               sbMgr_->GetDeletedNum(), sbMgr_->GetSegmentSize(),
               sbMgr_->GetSegmentNum(), sbMgr_->GetSbSize(),
               sbMgr_->GetIndexSize(),
               (sbMgr_->GetSbSize() + sbMgr_->GetIndexSize()),
               sbMgr_->GetDataRegionSize(),
               (sbMgr_->GetSbSize() + sbMgr_->GetIndexSize() + sbMgr_->GetDataRegionSize()),
               sbMgr_->GetDeviceSize());

        return true;
    }


    KvdbDS* KvdbDS::Open_KvdbDS(const char* filename)
    {
        if (instance_)
        {
            return instance_;
        }
        instance_ = new KvdbDS(filename);

        if (!instance_->openDB())
        {
            delete instance_;
            return NULL;
        }
        return instance_;

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
        segWriteT_stop_ = false;
        segWriteT_.Start();
    }

    void KvdbDS::stopThds()
    {
        segWriteT_stop_ = true;
        segQueMtx_.Lock();
        segQueCond_.Signal();
        segQueMtx_.Unlock();
        segWriteT_.Join();
    }

    KvdbDS::~KvdbDS()
    {
        closeDB();
        delete idxMgr_;
        delete sbMgr_;
        delete segMgr_;
        delete bdev_;

    }

    KvdbDS::KvdbDS(const string& filename) :
        fileName_(filename), segQueMtx_(Mutex()), segQueCond_(Cond(segQueMtx_)),
        segWriteT_(this), segWriteT_stop_(false)
    {
        bdev_ = BlockDevice::CreateDevice();
        segMgr_ = new SegmentManager(bdev_);
        sbMgr_ = new SuperBlockManager(bdev_);
        idxMgr_ = new IndexManager(bdev_);
    }


    bool KvdbDS::Insert(const char* key, uint32_t key_len, const char* data, uint16_t length)
    {
        bool res = false;
        if (key == NULL)
        {
            return res;
        }

        KVSlice slice(key, key_len, data, length);
        slice.ComputeDigest();

        idxMgr_->Lock();
        res = idxMgr_->IsKeyExist(&slice);
        idxMgr_->Unlock();

        OpType op_type = OpType::UNKOWN;
        if (!res)
        {
            op_type = OpType::INSERT;
        }
        else
        {
            op_type = OpType::UPDATE;
        }

        //Update SuperBlock
        sbMgr_->Lock();
        res = sbMgr_->IsElementFull();
        if (res)
        {
            __ERROR("Error: hash table full!\n");
            sbMgr_->Unlock();
            return false;
        }
        switch (op_type)
        {
            case OpType::INSERT:
                sbMgr_->AddElement();
                break;
            case OpType::UPDATE:
                break;
            default:
                break;
        }
        sbMgr_->Unlock();

        res = insertKey(slice, op_type);

        //recovery SuperBlock if failed
        if (!res)
        {
            sbMgr_->Lock();
            switch (op_type)
            {
                case OpType::INSERT:
                    sbMgr_->DeleteElement();
                    break;
                case OpType::UPDATE:
                    break;
                default:
                    break;
            }
            sbMgr_->Unlock();
        }
        return res;

    }

    bool KvdbDS::Delete(const char* key, uint32_t key_len)
    {
        bool res = false;
        if (key == NULL)
        {
            return res;
        }

        //Update SuperBlock
        sbMgr_->Lock();
        res = sbMgr_->IsElementFull();
        if (res)
        {
            __ERROR("Error: hash table full!\n");
            sbMgr_->Unlock();
            return false;
        }
        sbMgr_->AddDeleted();
        sbMgr_->DeleteElement();
        sbMgr_->Unlock();

        KVSlice slice(key, key_len, NULL, 0);
        slice.ComputeDigest();

        idxMgr_->Lock();
        res = idxMgr_->IsKeyExist(&slice);
        idxMgr_->Unlock();

        //the key is not exist
        if (!res)
        {
            return true;
        }

        res = insertKey(slice, OpType::DELETE);

        //recovery SuperBlock if failed
        if (!res)
        {
            sbMgr_->Lock();
            sbMgr_->DeleteDeleted();
            sbMgr_->AddElement();
            sbMgr_->Unlock();
        }
        return res;
    }

    bool KvdbDS::Get(const char* key, uint32_t key_len, string &data) 
    {
        bool res = false;
        if (key == NULL)
        {
            return res;
        }

        KVSlice slice(key, key_len, NULL, 0);
        slice.ComputeDigest();

        idxMgr_->Lock();
        res = idxMgr_->IsKeyExist(&slice);
        //idxMgr_->Unlock();

        if (!res)
        {
            idxMgr_->Unlock();
            return res;
        }


        //idxMgr_->Lock();
        idxMgr_->GetHashEntry(&slice);
        idxMgr_->Unlock();

        HashEntry entry;
        entry = slice.GetHashEntry();

        if (entry.GetDataSize() == 0 )
        {
            return false;
        }

        res = readData(&entry, data);
        return res;

    }

    bool KvdbDS::insertKey(KVSlice& slice, OpType op_type)
    {
        Request *req = new Request(slice, op_type);

        if (!enqueReqs(req))
        {
            __ERROR("Can't write to underlying datastore\n");
            delete req;
            return false;
        }

        req->Wait();
        bool result = req->GetState();

        if (result)
        {
            updateIndex(req);
        }

        delete req;
        return result;
    }

    void KvdbDS::updateIndex(Request *req)
    {
        //Update Index
        KVSlice *slice = &req->GetSlice();
        OpType op_type = req->GetOpType();


        idxMgr_->Lock();
        idxMgr_->UpdateIndexFromInsert(slice, op_type);
        idxMgr_->Unlock();

        __DEBUG("Update Index: key:%s, data_len:%u, seg_id:%u, data_offset:%u",
                slice->GetKeyStr().c_str(), slice->GetDataLen(),
                slice->GetSegId(), slice->GetHashEntry().GetDataOffsetInSeg());
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

        __DEBUG("get data offset %ld,!!!!!!!head_offset is %ld", data_offset, entry->GetHeaderOffsetPhy());

        return true;
    }

    bool KvdbDS::enqueReqs(Request *req)
    {
        SegmentSlice *seg;
        findAndLockSeg(req, seg);

        seg->Put(req);
        //seg->Complete();
        __DEBUG("Put request key = %s to seg_id:%u", req->GetSlice().GetKeyStr().c_str(), seg->GetSegId());
        seg->Unlock();
        //__DEBUG("Put request key = %s to seg_id:%u", req->GetSlice().GetKeyStr().c_str(), seg->GetSegId());

        return true;
    }

    bool KvdbDS::findAndLockSeg(Request *req, SegmentSlice*& seg)
    {
        bool found = false;
        segQueMtx_.Lock();
        for (list<SegmentSlice*>::iterator iter = segWriteQue_.begin(); iter != segWriteQue_.end(); iter++)
        {
            seg = *iter;
            seg->Lock();
            if (seg->IsCanWrite(req))
            {
                found = true;
                break;
            }
            else
            {
                seg->Complete();
                seg->Unlock();
            }
        }
        segQueMtx_.Unlock();
        if (found)
        {
            return true;
        }

        uint32_t seg_id = 0;
        segMgr_->Lock();
        bool res = segMgr_->GetEmptySegId(seg_id);
        if (!res)
        {
            __ERROR("Cann't get a new Empty Segment.\n");
            segMgr_->Unlock();
            return false;
        }
        segMgr_->Update(seg_id);
        segMgr_->Unlock();

        seg = new SegmentSlice(seg_id, segMgr_, bdev_);
        seg->Lock();
        segQueMtx_.Lock();
        segWriteQue_.push_back(seg);
        segQueCond_.Signal();
        segQueMtx_.Unlock();
        return true;

    }

    void KvdbDS::SegWriteThdEntry()
    {
        __DEBUG("Segment write thread start!!");
        while (true)
        {
            while (!segWriteQue_.empty())
            //segQueMtx_.Lock();
            //if (!segWriteQue_.empty())
            {
                segQueMtx_.Lock();
                SegmentSlice *seg = segWriteQue_.front();
                seg->Lock();
                segQueMtx_.Unlock();
                if (seg->IsCompleted())
                {

                    seg->Unlock();
                    seg->WriteSegToDevice();

                    segQueMtx_.Lock();
                    segWriteQue_.pop_front();
                    segQueMtx_.Unlock();

                    __DEBUG("Segment thread write seg to device, seg_id:%d", seg->GetSegId());

                    //Update Superblock
                    uint32_t seg_id = seg->GetSegId();
                    sbMgr_->Lock();
                    sbMgr_->SetCurSegId(seg_id);
                    sbMgr_->Unlock();
                    __DEBUG("Segment thread write seg, free size %u, seg id: %d, key num: %d", seg->GetFreeSize(), seg->GetSegId(), seg->GetKeyNum());

                    delete seg;
                }
                else
                {
                    if (seg->IsExpired())
                    {
                        seg->Complete();
                        __DEBUG("Segment thread: seg expired,complete it, seg_id:%d", seg->GetSegId());
                    }

                    seg->Unlock();
                } 
            }
            //segQueMtx_.Unlock();
            if (segWriteT_stop_)
            {
                break;
            }

            //std::this_thread::yield();
            segQueMtx_.Lock();
            segQueCond_.Wait();
            segQueMtx_.Unlock();
        }
        __DEBUG("Segment write thread stop!!");
    }

}
