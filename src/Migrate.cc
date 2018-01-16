#include "Migrate.h"
#include "Db_Structure.h"
#include "IndexManager.h"
#include "Volume.h"
#include "Segment.h"
#include "Tier.h"

using namespace std;

namespace hlkvds {
Migrate::~Migrate() {
    delete[] ftDataBuf_;
}

Migrate::Migrate(IndexManager* im, FastTier* ft, MediumTier* mt, Options &opt) :
    ft_(ft), mt_(mt), idxMgr_(im), options_(opt), ftDataBuf_(NULL) {

        ftSegSize_ = ft_->GetSegSize();
        posix_memalign((void **)&ftDataBuf_, 4096, ftSegSize_);
}

void Migrate::DoMigrate(uint32_t mt_vol_id) {
    //Get the sorted Segments
    Volume *ft_vol = ft_->GetVolume();
    Volume *mt_vol = mt_->GetVolume(mt_vol_id);

    std::multimap < uint32_t, uint32_t > cands_map;
    ft_vol->SortSegsByUtils(cands_map, options_.seg_full_rate);

    //do merge
    bool ret = false;
    std::vector <uint32_t> free_seg_vec;

    std::list<KVSlice*> slice_list;
    std::list<KVSlice*> recycle_list;

    SegForMigrate *mig_seg = new SegForMigrate(mt_vol, idxMgr_);
    for (std::multimap<uint32_t, uint32_t>::iterator iter = cands_map.begin(); iter
            != cands_map.end(); ++iter) {
        uint32_t seg_id = iter->second;

        ret = loadKvList(seg_id, slice_list);
        if (!ret) {
            continue;
        }

        if (mig_seg->TryPutList(slice_list)) {
            mig_seg->PutList(slice_list);
            free_seg_vec.push_back(seg_id);
            recycle_list.splice(recycle_list.end(), slice_list);
        }
        else {
            recycle_list.splice(recycle_list.end(), slice_list);
            break;
        }
    }
    
    uint32_t mig_seg_id;
    mt_vol->AllocForGC(mig_seg_id);
    mig_seg->SetSegId(mig_seg_id);
    ret = mig_seg->WriteSegToDevice();
    if (!ret) {
        __ERROR("Write First GC segment to device failed, free 0 segments");
        delete mig_seg;
        mt_vol->FreeForFailed(mig_seg_id);
        cleanKvList(recycle_list);
        return ;
    }

    uint32_t free_size = mig_seg->GetFreeSize();
    mt_vol->Use(mig_seg_id, free_size);

    mig_seg->UpdateToIndex();

    //clean work for first segment
    for (std::vector<uint32_t>::iterator iter = free_seg_vec.begin(); iter
            != free_seg_vec.end(); ++iter) {
        ft_vol->FreeForGC(*iter);
    }
    delete mig_seg;
    cleanKvList(recycle_list);

    uint32_t total_free = free_seg_vec.size();

    cands_map.clear();
    __DEBUG("Once DoMigrate total free %d segments in Fast Tier!", total_free);
}

void Migrate::loadSegKV(list<KVSlice*> &slice_list, uint32_t num_keys,
                          uint64_t phy_offset) {
    uint32_t head_offset = Volume::SizeOfSegOnDisk();

    Volume *ft_vol = ft_->GetVolume();
    int vol_id = ft_vol->GetId();

    for (uint32_t index = 0; index < num_keys; index++) {
        DataHeader header;
        memcpy(&header, &ftDataBuf_[head_offset],
               IndexManager::SizeOfDataHeader());

        DataHeaderAddress addrs(vol_id, phy_offset + (uint64_t)head_offset);
        HashEntry hash_entry(header, addrs, NULL);

        __DEBUG("load hash_entry from seg_offset = %ld, header_offset = %d", phy_offset, head_offset );

        if (idxMgr_->IsSameInMem(hash_entry)) {

            Kvdb_Digest digest = header.GetDigest();
            uint16_t data_len = header.GetDataSize();
            if (data_len != 0) {
                uint32_t data_offset = header.GetDataOffset();
                char* data = new char[data_len];
                memcpy(data, &ftDataBuf_[data_offset], data_len);

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
                memcpy(key, &ftDataBuf_[key_offset], key_len);
                key[key_len] = '\0';
                KVSlice *slice = new KVSlice(&digest, key, key_len, data, data_len);

                slice_list.push_back(slice);
                __DEBUG("the slice key_digest = %s, value = %s, seg_offset = %ld, head_offset = %d is valid, need to write", digest.GetDigest(), data, phy_offset, head_offset);
            }
        }

        head_offset = header.GetNextHeadOffset();
    }

}

bool Migrate::loadKvList(uint32_t seg_id, std::list<KVSlice*> &slice_list) {

    uint64_t seg_phy_off;

    Volume *ft_vol = ft_->GetVolume();
    ft_vol->CalcSegOffsetFromId(seg_id, seg_phy_off);

    memset(ftDataBuf_, 0 , ftSegSize_);
    if (!ft_vol->Read(ftDataBuf_, ftSegSize_, seg_phy_off)) {
        return false;
    }

    SegmentOnDisk seg_disk;
    memcpy(&seg_disk, ftDataBuf_, Volume::SizeOfSegOnDisk());

    uint32_t num_keys = seg_disk.number_keys;

    loadSegKV(slice_list, num_keys, seg_phy_off);
    return true;
}

void Migrate::cleanKvList(std::list<KVSlice*> &slice_list) {
    while (!slice_list.empty()) {
        KVSlice *slice = slice_list.front();
        slice_list.pop_front();
        const char* key = slice->GetKey();
        delete[] key;
        const char* data = slice->GetData();
        delete[] data;
        delete slice;
    }
}

}//namespace hlkvds
