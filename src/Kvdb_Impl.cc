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

        ds->seg_ = new SegmentSlice(ds->segMgr_,ds->idxMgr_, ds->bdev_);
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

        seg_ = new SegmentSlice(segMgr_, idxMgr_,  bdev_);

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
               "\t Total Device Size         : %ld Bytes\n"
               "\t Current Segment ID        : %d",
               sbMgr_->GetHTSize(), sbMgr_->GetElementNum(),
               sbMgr_->GetDeletedNum(), sbMgr_->GetSegmentSize(),
               sbMgr_->GetSegmentNum(), sbMgr_->GetSbSize(),
               sbMgr_->GetIndexSize(),
               (sbMgr_->GetSbSize() + sbMgr_->GetIndexSize()),
               sbMgr_->GetDataRegionSize(),
               (sbMgr_->GetSbSize() + sbMgr_->GetIndexSize() + sbMgr_->GetDataRegionSize()),
               sbMgr_->GetDeviceSize(), sbMgr_->GetCurSegmentId());

        return true;
    }


    KvdbDS* KvdbDS::Open_KvdbDS(const char* filename)
    {
        KvdbDS *instance_ = new KvdbDS(filename);

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
        reqForwardT_stop_.store(false);
        reqForwardT_ = std::thread(&KvdbDS::ReqForwardThdEntry,this);

        segWriteT_stop_.store(false);
        for(int i = 0; i<SEG_POOL_SIZE; i++)
        {
            segWriteTP_.push_back(std::thread(&KvdbDS::SegWriteThdEntry, this));
        }

        segTimeoutT_stop_.store(false);
        segTimeoutT_ = std::thread(&KvdbDS::SegTimeoutThdEntry, this);

        segReaperT_stop_.store(false);
        segReaperT_ = std::thread(&KvdbDS::SegReaperThdEntry, this);

    }

    void KvdbDS::stopThds()
    {
        reqForwardT_stop_.store(true);
        reqForwardT_.join();

        segWriteT_stop_.store(true);
        for(auto &th : segWriteTP_)
        {
            th.join();
        }

        segTimeoutT_stop_.store(true);
        segTimeoutT_.join();

        segReaperT_stop_.store(true);
        segReaperT_.join();

    }

    KvdbDS::~KvdbDS()
    {
        closeDB();
        delete idxMgr_;
        delete sbMgr_;
        delete segMgr_;
        delete bdev_;
        delete seg_;

    }

    KvdbDS::KvdbDS(const string& filename) :
        fileName_(filename),
        seg_(NULL),
        reqForwardT_stop_(false),
        segWriteT_stop_(false),
        segTimeoutT_stop_(false),
        segReaperT_stop_(false)
    {
        bdev_ = BlockDevice::CreateDevice();
        segMgr_ = new SegmentManager(bdev_);
        sbMgr_ = new SuperBlockManager(bdev_);
        idxMgr_ = new IndexManager(bdev_, sbMgr_);

    }


    bool KvdbDS::Insert(const char* key, uint32_t key_len, const char* data, uint16_t length)
    {
        bool res = false;
        if (key == NULL)
        {
            return res;
        }

        KVSlice slice(key, key_len, data, length);
        res = slice.ComputeDigest();

        if (!res)
        {
            __DEBUG("Compute key Digest failed!");
            return res;
        }

        res = insertKey(slice);

        return res;

    }

    bool KvdbDS::Delete(const char* key, uint32_t key_len)
    {
        return Insert(key, key_len, NULL, 0);
    }

    bool KvdbDS::Get(const char* key, uint32_t key_len, string &data) 
    {
        bool res = false;
        if (key == NULL)
        {
            return res;
        }

        KVSlice slice(key, key_len, NULL, 0);
        res =  slice.ComputeDigest();
        if (!res)
        {
            __DEBUG("Compute key Digest failed!");
            return res;
        }

        res = idxMgr_->GetHashEntry(&slice);
        if (!res)
        {
            //The key is not exist
            return res;
        }

        res = readData(slice, data);
        return res;

    }

    bool KvdbDS::insertKey(KVSlice& slice)
    {
        Request *req = new Request(slice);
        reqQue_.Enqueue_Notify(req);
        req->Wait();
        bool res = updateMeta(req);
        delete req;
        return res;
    }

    bool KvdbDS::updateMeta(Request *req)
    {
        bool res = req->GetWriteStat();
        // update index
        if ( res )
        {
            KVSlice *slice = &req->GetSlice();
            res = idxMgr_->UpdateIndex(slice);
        }
        // minus the segment delete counter
        SegmentSlice *seg = req->GetSeg();
        if (!seg->CommitedAndGetNum())
        {
            segReaperQue_.Enqueue_Notify(seg);
        }
        return res;
    }


    bool KvdbDS::readData(KVSlice &slice, string &data)
    {
        HashEntry *entry;
        entry = &slice.GetHashEntry();

        uint64_t data_offset = 0;
        if (!segMgr_->ComputeDataOffsetPhyFromEntry(entry, data_offset))
        {
            return false;
        }

        uint16_t data_len = entry->GetDataSize();
        char *mdata = new char[data_len];
        if (bdev_->pRead(mdata, data_len, data_offset) != (ssize_t)data_len)
        {
            __ERROR("Could not read data at position");
            delete[] mdata;
            return false;
        }
        data.assign(mdata, data_len);
        delete[] mdata;

        __DEBUG("get key: %s, data offset %ld, head_offset is %ld", slice.GetKeyStr().c_str(), data_offset, entry->GetHeaderOffsetPhy());

        return true;
    }


    void KvdbDS::ReqForwardThdEntry()
    {
        __DEBUG("Requests Forward thread start!!");
        std::unique_lock<std::mutex> lck_seg(segMtx_, std::defer_lock);
        while(!reqForwardT_stop_.load())
        {
            Request  *req = reqQue_.Wait_Dequeue();
            if ( req )
            {
                lck_seg.lock();
                if(seg_->TryPut(req))
                {
                    seg_->Put(req);
                }
                else
                {
                     seg_->Complete();

                     SegmentSlice *temp = seg_;
                     seg_ = new SegmentSlice(segMgr_, idxMgr_, bdev_);
                     seg_->Put(req);

                     segWriteQue_.Enqueue_Notify(temp);
                }
                lck_seg.unlock();
            }

        }
        __DEBUG("Requests Forward thread stop!!");
    }

    void KvdbDS::SegWriteThdEntry()
    {
        __DEBUG("Segment write thread start!!");
        while (!segWriteT_stop_)
        {
            SegmentSlice *seg = segWriteQue_.Wait_Dequeue();
            if ( seg )
            {
                uint32_t seg_id = 0;
                bool res = segMgr_->AllocSeg(seg_id);
                if (!res)
                {
                    __ERROR("Cann't get a new Empty Segment.\n");
                    seg->Notify(res);
                }
                else
                {
                    res = seg->WriteSegToDevice(seg_id);
                    seg->Notify(res);
                    if (!res)
                    {
                        //Free the segment if write failed
                        segMgr_->FreeSeg(seg_id);
                    }

                    __DEBUG("Segment thread write seg to device, seg_id:%d %s", seg_id, res==true? "Success":"Failed");

                    //Update Superblock
                    sbMgr_->SetCurSegId(seg_id);
                }
            }
        }
        __DEBUG("Segment write thread stop!!");
    }


    void KvdbDS::SegTimeoutThdEntry()
    {
        __DEBUG("Segment Timeout thread start!!");
        std::unique_lock<std::mutex> lck(segMtx_, std::defer_lock);
        while (!segTimeoutT_stop_)
        {
            lck.lock();
            if (seg_->IsExpired())
            {

                seg_->Complete();

                SegmentSlice *temp = seg_;
                seg_ = new SegmentSlice(segMgr_, idxMgr_, bdev_);

                segWriteQue_.Enqueue_Notify(temp);
            }
            lck.unlock();

            usleep(EXPIRED_TIME);
        }
        __DEBUG("Segment Timeout thread stop!!");
    }

    void KvdbDS::SegReaperThdEntry()
    {
        __DEBUG("Segment reaper thread start!!");

        while (!segReaperT_stop_)
        {
            SegmentSlice *seg = segReaperQue_.Wait_Dequeue();
            if ( seg )
            {
                seg->CleanDeletedEntry();
                __DEBUG("Segment reaper delete seg_id = %d", seg->GetSegId());
                delete seg;
            }
        }
        __DEBUG("Segment write thread stop!!");
    }
}

