#include "GcManager.h"

namespace kvdb{
    GCSegment::GCSegment()
        : segId_(0), segMgr_(NULL), idxMgr_(NULL), bdev_(NULL),
          segSize_(0), persistTime_(KVTime()), headPos_(0),
          //tailPos_(0), freeSize_(0), keyNum_(0), keyAlignedNum_(0),
          tailPos_(0), keyNum_(0), keyAlignedNum_(0),
          segOndisk_(NULL)
    {
        segOndisk_ = new SegmentOnDisk();
        dataBuf_ = new char[segSize_];
        //writeBuf_ = new char[segSize_];
        //tempBuf_ = new char[segSize_];
    }

    GCSegment::~GCSegment()
    {
        deleteKVList(sliceList_);
        delete segOndisk_;
        delete[] dataBuf_;
        //delete[] writeBuf_;
        //delete[] tempBuf_;
    }

    GCSegment::GCSegment(const GCSegment& toBeCopied)
    {
        segOndisk_ = new SegmentOnDisk();
        dataBuf_ = new char[segSize_];
        //writeBuf_ = new char[segSize_];
        //tempBuf_ = new char[segSize_];
        copyHelper(toBeCopied);
    }

    GCSegment& GCSegment::operator=(const GCSegment& toBeCopied)
    {
        copyHelper(toBeCopied);
        return *this;
    }

    void GCSegment::copyHelper(const GCSegment& toBeCopied)
    {
        segId_ = toBeCopied.segId_;
        segMgr_ = toBeCopied.segMgr_;
        idxMgr_ = toBeCopied.idxMgr_;
        bdev_ = toBeCopied.bdev_;
        segSize_ = toBeCopied.segSize_;
        persistTime_ = toBeCopied.persistTime_;
        headPos_ = toBeCopied.headPos_;
        tailPos_ = toBeCopied.tailPos_;
        //freeSize_ = toBeCopied.freeSize_;
        keyNum_ = toBeCopied.keyNum_;
        keyAlignedNum_ = toBeCopied.keyAlignedNum_;
        *segOndisk_ = *toBeCopied.segOndisk_;
        memcpy(dataBuf_, toBeCopied.dataBuf_, segSize_);
        //memcpy(writeBuf_, toBeCopied.writeBuf_, segSize_);
    }

    GCSegment::GCSegment(SegmentManager* sm, IndexManager* im, BlockDevice* bdev)
        : segId_(0), segMgr_(sm), idxMgr_(im), bdev_(bdev),
          segSize_(segMgr_->GetSegmentSize()), persistTime_(KVTime()),
          headPos_(SegmentManager::SizeOfSegOnDisk()), tailPos_(segSize_),
          keyNum_(0), keyAlignedNum_(0), segOndisk_(NULL)
          //freeSize_(0), keyNum_(0), keyAlignedNum_(0), segOndisk_(NULL)
    {
        segOndisk_ = new SegmentOnDisk();
        dataBuf_ = new char[segSize_];
        //writeBuf_ = new char[segSize_];
        //tempBuf_ = new char[segSize_];
    }

    void GCSegment::MergeSeg(vector<uint32_t> &cands)
    {
        for (vector<uint32_t>::iterator iter = cands.begin(); iter!= cands.end(); iter++)
        {
            uint32_t seg_id = *iter;
            if (mergeSeg(seg_id))
            {
                segVec_.push_back(seg_id);
            }
            __DEBUG("Merge seg id=%d completed", seg_id);
        }

        __DEBUG("Merge Segment Finished!");
        //freeSize_ = tailPos_ - headPos_;
    }

    bool GCSegment::mergeSeg(uint32_t seg_id)
    {
        uint64_t seg_phy_off;
        segMgr_->ComputeSegOffsetFromId(seg_id, seg_phy_off);

        if (!readSegFromDevice(seg_phy_off))
        {
            return false;
        }

        SegmentOnDisk seg_disk;
        //memcpy(&seg_disk, tempBuf_, SegmentManager::SizeOfSegOnDisk());
        memcpy(&seg_disk, dataBuf_, SegmentManager::SizeOfSegOnDisk());

        uint32_t num_keys = seg_disk.number_keys;
        list<KVSlice*> slice_list;

        __DEBUG("read seg_id = %d, have keys number = %d!", seg_id, num_keys);

        loadSegKV(slice_list, num_keys, seg_phy_off);

        if (!tryPutBatch(slice_list))
        {
            deleteKVList(slice_list);
            return false;
        }
        putBatch(slice_list);

        return true;
    }

    bool GCSegment::readSegFromDevice(uint64_t seg_offset)
    {
        //if (bdev_->pRead(tempBuf_, segSize_, seg_offset) != segSize_)
        if (bdev_->pRead(dataBuf_, segSize_, seg_offset) != segSize_)
        {
            __ERROR("GC read segment data error!!!");
            return false;
        }
        return true;
    }


    void GCSegment::loadSegKV(list<KVSlice*> &slice_list, uint32_t num_keys, uint64_t phy_offset)
    {
        uint32_t head_offset = SegmentManager::SizeOfSegOnDisk();

        for(uint32_t index = 0; index < num_keys; index++)
        {
            DataHeader header;
            //memcpy(&header, &tempBuf_[head_offset], IndexManager::SizeOfDataHeader());
            memcpy(&header, &dataBuf_[head_offset], IndexManager::SizeOfDataHeader());

            HashEntry hash_entry(header, phy_offset + (uint64_t)head_offset, NULL);
            __DEBUG("load hash_entry from seg_offset = %ld, header_offset = %d", phy_offset, head_offset );


            if (idxMgr_->IsSameInMem(hash_entry))
            {

                Kvdb_Digest digest = header.GetDigest();
                uint16_t data_len = header.GetDataSize();
                if (data_len != 0)
                {
                    uint32_t data_offset = header.GetDataOffset();
                    char* data = new char[data_len];
                    //memcpy(data, &tempBuf_[data_offset], data_len);
                    memcpy(data, &dataBuf_[data_offset], data_len);

                    KVSlice *slice = new KVSlice(&digest, data, data_len);
                    slice_list.push_back(slice);

                    __DEBUG("the slice key_digest = %s, value = %s, seg_offset = %ld, head_offset = %d is valid, need to write", digest.GetDigest(), data, phy_offset, head_offset);
                }
            }

            head_offset = header.GetNextHeadOffset();
        }

    }

    bool GCSegment::tryPutBatch(list<KVSlice*> &slice_list)
    {
        uint32_t head_pos = headPos_;
        uint32_t tail_pos = tailPos_;

        for (list<KVSlice*>::iterator iter = slice_list.begin(); iter != slice_list.end(); iter++)
        {
            KVSlice *slice = *iter;
            if ( !isCanFit(slice, head_pos, tail_pos) )
            {
                __DEBUG("tryPutBatch Failed!");
                return false;
            }
            if (slice->IsAlignedData())
            {
                head_pos += IndexManager::SizeOfDataHeader();
                tail_pos -= ALIGNED_SIZE;
            }
            else
            {
                head_pos += IndexManager::SizeOfDataHeader() + slice->GetDataLen();
            }

        }
        __DEBUG("tryPutBatch Success!");
        return true;
    }

    void GCSegment::putBatch(list<KVSlice*> &slice_list)
    {
        for (list<KVSlice*>::iterator iter = slice_list.begin(); iter != slice_list.end(); iter++)
        {
            KVSlice *slice = *iter;
            if (slice->IsAlignedData())
            {
                headPos_ += IndexManager::SizeOfDataHeader();
                tailPos_ -= ALIGNED_SIZE;
                keyAlignedNum_++;
            }
            else
            {
                headPos_ += IndexManager::SizeOfDataHeader() + slice->GetDataLen();
            }
            keyNum_++;
        }
        sliceList_.splice(sliceList_.end(), slice_list);
        __DEBUG("PutBatch Success!");

    }

    bool GCSegment::isCanFit(KVSlice *slice, uint32_t head_pos, uint32_t tail_pos)
    {
        uint32_t freeSize = tail_pos - head_pos;
        uint32_t needSize = slice->GetDataLen() + IndexManager::SizeOfDataHeader();
        return freeSize > needSize;
    }

    void GCSegment::deleteKVList(list<KVSlice*> &slice_list)
    {
        while( !slice_list.empty() )
        {
            KVSlice *slice = slice_list.front();
            slice_list.pop_front();
            const char* data = slice->GetData();
            delete[] data;
            delete slice;
        }
    }


    bool GCSegment::WriteSegToDevice(uint32_t seg_id)
    {
        segId_ = seg_id;
        fillSlice();
        __DEBUG("Begin write seg, free size %u, seg id: %d, key num: %d", tailPos_-headPos_ , segId_, keyNum_);

        return _writeDataToDevice();
    }

    void GCSegment::fillSlice()
    {
        uint32_t head_pos = SegmentManager::SizeOfSegOnDisk();
        uint32_t tail_pos = segSize_;
        for(list<KVSlice *>::iterator iter=sliceList_.begin(); iter != sliceList_.end(); iter++)
        {

            KVSlice *slice = *iter;
            slice->SetSegId(segId_);
            if (slice->IsAlignedData())
            {
                uint32_t data_offset = tail_pos - ALIGNED_SIZE;
                uint32_t next_offset = head_pos + IndexManager::SizeOfDataHeader();

                DataHeader data_header(slice->GetDigest(), slice->GetDataLen(), data_offset, next_offset);

                uint64_t seg_offset = 0;
                segMgr_->ComputeSegOffsetFromId(segId_, seg_offset);
                uint64_t header_offset = seg_offset + head_pos;

                HashEntry hash_entry(data_header, header_offset, NULL);
                slice->SetHashEntry(&hash_entry);

                head_pos += IndexManager::SizeOfDataHeader();
                tail_pos -= ALIGNED_SIZE;
                __DEBUG("SegmentSlice: key=%s, data_offset=%u, header_offset=%lu, seg_id=%u, head_pos=%u, tail_pos = %u", slice->GetKey(), data_offset, header_offset, segId_, head_pos, tail_pos);
            }
            else
            {
                uint32_t data_offset = head_pos + IndexManager::SizeOfDataHeader();
                uint32_t next_offset = head_pos + IndexManager::SizeOfDataHeader() + slice->GetDataLen();

                DataHeader data_header(slice->GetDigest(), slice->GetDataLen(), data_offset, next_offset);

                uint64_t seg_offset = 0;
                segMgr_->ComputeSegOffsetFromId(segId_, seg_offset);
                uint64_t header_offset = seg_offset + head_pos;

                HashEntry hash_entry(data_header, header_offset, NULL);
                slice->SetHashEntry(&hash_entry);

                head_pos += IndexManager::SizeOfDataHeader() + slice->GetDataLen();
                __DEBUG("SegmentSlice: key=%s, data_offset=%u, header_offset=%lu, seg_id=%u, head_pos=%u, tail_pos = %u", slice->GetKey(), data_offset, header_offset, segId_, head_pos, tail_pos);

            }
        }
        if (head_pos != headPos_ || tail_pos != tailPos_)
        {
            __ERROR("Segment fillSlice  Failed!!! head_pos= %u, headPos_=%u, tail_pos= %u, tailPos_ = %u", head_pos, headPos_, tail_pos, tailPos_);
        }
    }


    void GCSegment::copyToData()
    {
        uint64_t offset = 0;
        segMgr_->ComputeSegOffsetFromId(segId_, offset);

        uint32_t offset_begin = 0;
        uint32_t offset_end = segSize_;

        //memcpy(writeBuf_, segOndisk_, SegmentManager::SizeOfSegOnDisk());
        memcpy(dataBuf_, segOndisk_, SegmentManager::SizeOfSegOnDisk());
        offset_begin += SegmentManager::SizeOfSegOnDisk();

        //aggregate iovec
        for(list<KVSlice *>::iterator iter=sliceList_.begin(); iter != sliceList_.end(); iter++)
        {
            KVSlice *slice = *iter;
            DataHeader *header = &slice->GetHashEntry().GetEntryOnDisk().GetDataHeader();
            char *data = (char *)slice->GetData();
            uint16_t data_len = slice->GetDataLen();

            //memcpy(&(writeBuf_[offset_begin]), header, IndexManager::SizeOfDataHeader());
            memcpy(&(dataBuf_[offset_begin]), header, IndexManager::SizeOfDataHeader());
            offset_begin += IndexManager::SizeOfDataHeader();

            if (slice->IsAlignedData())
            {
                offset_end -= data_len;
                memcpy(&(dataBuf_[offset_end]), data, data_len);
                //memcpy(&(writeBuf_[offset_end]), data, data_len);
                __DEBUG("write key = %s, data position: %lu", slice->GetKey(), offset_end + offset);
            }
            else
            {
                memcpy(&(dataBuf_[offset_begin]), data, data_len);
                //memcpy(&(writeBuf_[offset_begin]), data, data_len);
                offset_begin += data_len;
                __DEBUG("write key = %s, data position: %lu", slice->GetKey(), offset_begin + offset);
            }
        }
        memset(&(dataBuf_[offset_begin]), 0, (offset_end - offset_begin));
        //memset(&(writeBuf_[offset_begin]), 0, (offset_end - offset_begin));
    }

    bool GCSegment::_writeDataToDevice()
    {
        copyToData();

        uint64_t offset = 0;
        segMgr_->ComputeSegOffsetFromId(segId_, offset);

        //if (bdev_->pWrite(writeBuf_, segSize_, offset) != segSize_)
        if (bdev_->pWrite(dataBuf_, segSize_, offset) != segSize_)
        {
            __ERROR("Write Segment front data error, seg_id:%u", segId_);
            return false;
        }
        return true;
    }

    void GCSegment::UpdateToIndex()
    {
        for(list<KVSlice *>::iterator iter=sliceList_.begin(); iter != sliceList_.end(); iter++)
        {
            KVSlice *slice = *iter;
            idxMgr_->UpdateIndex(slice);
        }
        __DEBUG("UpdateToIndex Success!");
    }

    void GCSegment::FreeSegs()
    {
        for(vector<uint32_t>::iterator iter=segVec_.begin(); iter != segVec_.end(); iter++)
        {
            segMgr_->FreeForGC(*iter);
        }
        __DEBUG("FreeSegs Success!");
    }


    GcManager::GcManager()
        :segMgr_(NULL), idxMgr_(NULL), bdev_(NULL) {}

    GcManager::~GcManager(){}

    GcManager::GcManager(const GcManager& toBeCopied)
    {
        bdev_ = toBeCopied.bdev_;
        idxMgr_ = toBeCopied.idxMgr_;
        segMgr_ = toBeCopied.segMgr_;
    }

    GcManager& GcManager::operator=(const GcManager& toBeCopied)
    {
        bdev_ = toBeCopied.bdev_;
        idxMgr_ = toBeCopied.idxMgr_;
        segMgr_ = toBeCopied.segMgr_;
        return *this;
    }

    GcManager::GcManager(BlockDevice* bdev, IndexManager* im, SegmentManager* sm)
    {
        bdev_ = bdev;
        idxMgr_ = im;
        segMgr_ = sm;
    }

    bool GcManager::ForeGC()
    {
        __INFO("foreground call GC!!!!!");
        BackGC(0.5);
        return true;
    }

    void GcManager::BackGC(float utils)
    {
        std::lock_guard<std::mutex> lck_gc(gcMtx_);

        //get seg utilization rank
        multimap<uint32_t, uint32_t > segs_map;
        segMgr_->SortSegsByUtils(segs_map, utils);

        //find out seg candidates for gc
        uint32_t used_size = 0;
        uint32_t seg_size = segMgr_->GetSegmentSize();
        std::vector<uint32_t> cands;
        for (std::multimap<uint32_t, uint32_t>::iterator iter = segs_map.begin(); iter != segs_map.end(); iter++)
        {
            used_size += iter->first;
            if (used_size > seg_size)
            {
                break;
            }
            cands.push_back(iter->second);
        }

        //do gc by candidates
        if (cands.size() > 1)
        {
            doGC(cands);
            cands.clear();
            used_size = 0;
        }
    }

    void GcManager::FullGC()
    {
        std::lock_guard<std::mutex> lck_gc(gcMtx_);

        float utils = 1.0;
        __INFO("Now Free Segment total %d after do GC",segMgr_->GetTotalFreeSegs());


        multimap<uint32_t, uint32_t > segs_map;
        segMgr_->SortSegsByUtils(segs_map, utils);
        //__INFO("Find %lu segments under the utils %f", segs_map.size(), utils);

        //find out seg candidates for gc
        uint32_t used_size = 0;
        uint32_t seg_size = segMgr_->GetSegmentSize();
        uint32_t merge_count = 0;
        uint32_t free_count = 0;
        std::vector<uint32_t> cands;
        for (std::multimap<uint32_t, uint32_t>::iterator iter = segs_map.begin(); iter != segs_map.end(); iter++)
        {
            used_size += iter->first;
            if (used_size > seg_size)
            {
                if (cands.size() > 1)
                {
                    doGC(cands);
                    cands.clear();
                    used_size = 0;
                    merge_count++ ;
                }
                else
                {
                    __INFO("Can not find more then 2 segment to merge, GC Finish utils = %f", utils);
                    break;
                }
            }
            else
            {
                __DEBUG("seg index=%d, used_size = %d need do GC", iter->second, iter->first);
                cands.push_back(iter->second);
                free_count++;
            }
        }

        if (cands.size() > 1)
        {
            doGC(cands);
            cands.clear();
            used_size = 0;
            merge_count++ ;
        }
    
    
        __INFO("Now Free Segment total %d after do GC, this gc total free %d segment, and merge them to %d segment",segMgr_->GetTotalFreeSegs(), free_count, merge_count);
        ;
    }

    void GcManager::doGC(std::vector<uint32_t> &cands)
    {
        //do_gc
        GCSegment *seg_gc = new GCSegment(segMgr_, idxMgr_, bdev_);
        seg_gc->MergeSeg(cands);

        bool ret;
        uint32_t seg_id;
        ret = segMgr_->AllocForGC(seg_id);
        if ( !ret )
        {
            __ERROR("Cann't get a new Empty Segment, GC FAILED!\n");
            return;
        }

        ret = seg_gc->WriteSegToDevice(seg_id);
        if (ret)
        {
            uint32_t free_size = seg_gc->GetFreeSize();
            segMgr_->Use(seg_id, free_size);

            seg_gc->UpdateToIndex();
            seg_gc->FreeSegs();
        }
        else
        {
            segMgr_->FreeForFailed(seg_id);
        }

        delete seg_gc;
        
    }

}//namespace kvdb
