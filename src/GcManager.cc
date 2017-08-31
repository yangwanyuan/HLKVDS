#include "GcManager.h"

namespace hlkvds {
GcManager::~GcManager() {
    if (dataBuf_) {
        delete[] dataBuf_;
    }
}

GcManager::GcManager(BlockDevice* bdev, IndexManager* im, SegmentManager* sm,
                     Options &opt) :
    options_(opt), dataBuf_(NULL) {
    bdev_ = bdev;
    idxMgr_ = im;
    segMgr_ = sm;
}

bool GcManager::ForeGC() {
    std::lock_guard < std::mutex > lck_gc(gcMtx_);
    //check free segment;
    uint32_t free_seg_num = segMgr_->GetTotalFreeSegs();
    if (free_seg_num > SEG_RESERVED_FOR_GC) {
        __DEBUG("some thread already called doForeGC, ignore this call!");
        return true;
    }

    free_seg_num = 0;
    __DEBUG("Begin Fore GC !");
    while (!free_seg_num) {
        std::multimap < uint32_t, uint32_t > cands_map;
        segMgr_->SortSegsByUtils(cands_map, options_.seg_full_rate);
        if (cands_map.empty()) {
            break;
        }
        free_seg_num = doMerge(cands_map);
        cands_map.clear();
        __DEBUG("Once merge total free %d segments!", free_seg_num);
    }
    return free_seg_num ? true : false;
}

void GcManager::BackGC() {
    std::unique_lock < std::mutex > lck_gc(gcMtx_, std::defer_lock);

    uint32_t seg_size = segMgr_->GetSegmentSize();
    uint64_t data_theory_size = idxMgr_->GetDataTheorySize();
    uint32_t theory_seg_num = data_theory_size / seg_size + 1;

    uint32_t free_seg_num = segMgr_->GetTotalFreeSegs();
    uint32_t total_seg_num = segMgr_->GetNumberOfSeg();
    uint32_t used_seg_num = total_seg_num - free_seg_num;
    if (used_seg_num == 0 || theory_seg_num > used_seg_num) {
        return;
    }

    //upper water threshold
    if ( (double)free_seg_num / (double)total_seg_num > CAPACITY_THRESHOLD_TODO_GC) {
        return;
    }

    double waste_rate = 1 - ((double) theory_seg_num / (double) used_seg_num);

    __DEBUG("Begin do Background GC! waste_rate is %f, data_theory_size = %lu", waste_rate, data_theory_size);
    if (waste_rate > options_.gc_upper_level) {
        while (waste_rate > options_.gc_lower_level && used_seg_num > 1) {
            lck_gc.lock();
            std::multimap < uint32_t, uint32_t > cands_map;
            segMgr_->SortSegsByUtils(cands_map, options_.seg_full_rate);

            //TODO: Maybe some bug here!!!!!
            if (cands_map.empty() || theory_seg_num > used_seg_num) {
                lck_gc.unlock();
                break;
            }
            uint32_t total_free = doMerge(cands_map);
            cands_map.clear();

            data_theory_size = idxMgr_->GetDataTheorySize();
            theory_seg_num = data_theory_size / seg_size + 1;

            used_seg_num = segMgr_->GetTotalUsedSegs();

            waste_rate = 1 - ((double) theory_seg_num / (double) used_seg_num);
            __DEBUG("done once backgroud GC, total free %u segments, data_theory_size = %lu, theory_seg_num = %u, used_seg_num = %u, seg_size = %u, waste_rate is %f",total_free, data_theory_size, theory_seg_num, used_seg_num, seg_size, waste_rate);
            lck_gc.unlock();
        } __DEBUG("waste_rate lower than the GC_LOWER_LEVEL, end Background GC! waste_rate = %f", waste_rate);
    } __DEBUG("Finsh do Background GC! waste_rate is %f", waste_rate);
}

void GcManager::FullGC() {
    std::unique_lock < std::mutex > lck_gc(gcMtx_, std::defer_lock);

    __DEBUG("Begin do Full GC!");
    uint32_t free_before = segMgr_->GetTotalFreeSegs();
    uint32_t free_after = 0;
    uint32_t used_total = 0;

    uint32_t seg_num = segMgr_->GetNumberOfSeg();
    uint32_t total_free = 0;
    uint64_t theory_size = 0;
    uint32_t free_seg_num = 0;
    uint32_t used_seg_num = 0;
    uint64_t used_size = 0;

    while (true) {
        lck_gc.lock();
        std::multimap < uint32_t, uint32_t > cands_map;
        segMgr_->SortSegsByUtils(cands_map, options_.seg_full_rate);
        if (cands_map.size() < SEG_RESERVED_FOR_GC) {
            free_after = segMgr_->GetTotalFreeSegs();
            used_total = segMgr_->GetNumberOfSeg() - free_after;
            __DEBUG("End do Full GC! befor have %u segment free, after do full GC now have %u segments free, now used %u segments!", free_before, free_after, used_total);
            break;
        }

        total_free = doMerge(cands_map);
        theory_size = idxMgr_->GetDataTheorySize();
        free_seg_num = segMgr_->GetTotalFreeSegs();
        used_seg_num = seg_num - free_seg_num;
        used_size = (uint64_t) segMgr_->GetSegmentSize()
                * (uint64_t) used_seg_num;

        __DEBUG("complete once merge, and this merge free %u segments, theory_size is %lu Byte, used Segments total is %u, occur space is %lu, total seg is %u, free seg is %u", total_free, theory_size, used_seg_num, used_size, seg_num, free_seg_num);
        cands_map.clear();
        lck_gc.unlock();
    } __DEBUG("End do Full GC! total free %u segments", free_after - free_before);
}

uint32_t GcManager::doMerge(std::multimap<uint32_t, uint32_t> &cands_map) {
    if (!dataBuf_) {
        uint32_t seg_size = segMgr_->GetSegmentSize();
        //dataBuf_ = new char[seg_size];
        posix_memalign((void **)&dataBuf_, 4096, seg_size);
    }

    bool ret;
    std::vector < uint32_t > free_seg_vec;
    uint32_t last_seg_id = 0;
    bool need_flag = false;
    uint32_t total_free = 0;

    std::list<KVSlice*> slice_list;
    std::list<KVSlice*> recycle_list;

    std::list<KVSlice*>::iterator slice_iter;

    //handle first segment
    SegForSlice *seg_first = new SegForSlice(segMgr_, idxMgr_, bdev_);
    for (std::multimap<uint32_t, uint32_t>::iterator iter = cands_map.begin(); iter
            != cands_map.end(); ++iter) {
        uint32_t seg_id = iter->second;
        ret = loadKvList(seg_id, slice_list);
        if (!ret) {
            continue;
        }

        for (slice_iter = slice_list.begin(); slice_iter != slice_list.end();) {
            KVSlice* slice = *slice_iter;
            if (seg_first->TryPut(slice)) {
                seg_first->Put(slice);
                recycle_list.push_back(*slice_iter);
                slice_list.erase(slice_iter++);
            } else {
                need_flag = true;
                break;
            }
        }

        if (need_flag) {
            last_seg_id = seg_id;
            break;
        } else {
            free_seg_vec.push_back(seg_id);
        }
    }

    uint32_t seg_first_id;
    segMgr_->AllocForGC(seg_first_id);
    seg_first->SetSegId(seg_first_id);
    ret = seg_first->WriteSegToDevice();
    if (!ret) {
        __ERROR("Write First GC segment to device failed, free 0 segments");
        delete seg_first;
        segMgr_->FreeForFailed(seg_first_id);
        cleanKvList(recycle_list);
        cleanKvList(slice_list);
        return total_free;
    }

    uint32_t free_size = seg_first->GetFreeSize();
    segMgr_->Use(seg_first_id, free_size);

    seg_first->UpdateToIndex();

    //clean work for first segment
    for (std::vector<uint32_t>::iterator iter = free_seg_vec.begin(); iter
            != free_seg_vec.end(); ++iter) {
        segMgr_->FreeForGC(*iter);
    }
    delete seg_first;
    cleanKvList(recycle_list);

    total_free = free_seg_vec.size() - 1;

    if (!need_flag) {
        __DEBUG("All fragment segment merge to 1 segment, and free %d segments", total_free);
        return total_free;
    }

    //handle second segment
    SegForSlice *seg_second = new SegForSlice(segMgr_, idxMgr_, bdev_);
    uint32_t seg_second_id;
    segMgr_->AllocForGC(seg_second_id);

    for (slice_iter = slice_list.begin(); slice_iter != slice_list.end(); ++slice_iter) {
        KVSlice* slice = *slice_iter;
        seg_second->Put(slice);
    }

    seg_second->SetSegId(seg_second_id);
    ret = seg_second->WriteSegToDevice();
    if (!ret) {
        __ERROR("Write Second GC segment to device failed, but First Segment is completed, free %d segments", total_free);
        delete seg_second;
        segMgr_->FreeForFailed(seg_second_id);
        cleanKvList(slice_list);
        return total_free;
    }

    free_size = seg_second->GetFreeSize();
    segMgr_->Use(seg_second_id, free_size);

    seg_second->UpdateToIndex();
    segMgr_->FreeForGC(last_seg_id);

    //clean work for second segment
    delete seg_second;
    cleanKvList(slice_list);

    __DEBUG("Multiple fragment segments merge to 2 segment, and free %d segments", total_free);

    return total_free;
}

bool GcManager::readSegment(uint64_t seg_offset) {
    uint32_t seg_size = segMgr_->GetSegmentSize();
    if (bdev_->pRead(dataBuf_, seg_size, seg_offset) != seg_size) {
        __ERROR("GC read segment data error!!!");
        return false;
    }
    return true;
}

void GcManager::loadSegKV(list<KVSlice*> &slice_list, uint32_t num_keys,
                          uint64_t phy_offset) {
    uint32_t head_offset = SegmentManager::SizeOfSegOnDisk();

    for (uint32_t index = 0; index < num_keys; index++) {
        DataHeader header;
        memcpy(&header, &dataBuf_[head_offset],
               IndexManager::SizeOfDataHeader());

        HashEntry hash_entry(header, phy_offset + (uint64_t) head_offset, NULL);
        __DEBUG("load hash_entry from seg_offset = %ld, header_offset = %d", phy_offset, head_offset );

        if (idxMgr_->IsSameInMem(hash_entry)) {

            Kvdb_Digest digest = header.GetDigest();
            uint16_t data_len = header.GetDataSize();
            if (data_len != 0) {
                uint32_t data_offset = header.GetDataOffset();
                char* data = new char[data_len];
                memcpy(data, &dataBuf_[data_offset], data_len);

#ifdef WITH_ITERATOR
                //memcpy key to KVSlice.GetNextHeadOffsetGetNextHeadOffset
                uint16_t key_len =header.GetKeySize();
                uint32_t next_head_offset = header.GetNextHeadOffset();
                uint32_t key_offset;
                if (ALIGNED_SIZE == data_len) {
                    key_offset = next_head_offset - key_len;
                } else {
                    key_offset = next_head_offset - data_len - key_len;
                }
                char* key = new char[key_len+1];
                memcpy(key, &dataBuf_[key_offset], key_len);
                key[key_len] = '\0';
                KVSlice *slice = new KVSlice(&digest, key, key_len, data, data_len);
#else
                KVSlice *slice = new KVSlice(&digest, data, data_len);
#endif

                slice_list.push_back(slice);
                __DEBUG("the slice key_digest = %s, value = %s, seg_offset = %ld, head_offset = %d is valid, need to write", digest.GetDigest(), data, phy_offset, head_offset);
            }
        }

        head_offset = header.GetNextHeadOffset();
    }

}

bool GcManager::loadKvList(uint32_t seg_id, std::list<KVSlice*> &slice_list) {
    uint64_t seg_phy_off;
    segMgr_->ComputeSegOffsetFromId(seg_id, seg_phy_off);

    if (!readSegment(seg_phy_off)) {
        return false;
    }

    SegmentOnDisk seg_disk;
    memcpy(&seg_disk, dataBuf_, SegmentManager::SizeOfSegOnDisk());

    uint32_t num_keys = seg_disk.number_keys;

    loadSegKV(slice_list, num_keys, seg_phy_off);
    return true;
}

void GcManager::cleanKvList(std::list<KVSlice*> &slice_list) {
    while (!slice_list.empty()) {
        KVSlice *slice = slice_list.front();
        slice_list.pop_front();
#ifdef WITH_ITERATOR
        const char* key = slice->GetKey();
        delete[] key;
#else
#endif
        const char* data = slice->GetData();
        delete[] data;
        delete slice;
    }
}

}//namespace hlkvds
