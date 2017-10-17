#include "MetaStor.h"
#include "Db_Structure.h"
#include "BlockDevice.h"
#include "SuperBlockManager.h"
#include "IndexManager.h"
#include "DataStor.h"

namespace hlkvds {

MetaStor::MetaStor(const char* paths, Options &opt, BlockDevice *dev, SuperBlockManager *sbm, IndexManager *im, SimpleDS_Impl *ds) : paths_(paths), options_(opt), metaDev_(dev), sbMgr_(sbm), idxMgr_(im), dataStor_(ds) {}

MetaStor::~MetaStor() {}

bool MetaStor::CreateMetaData() {

    uint32_t num_entries = 0;
    uint32_t number_segments = 0;
    uint64_t db_sb_size = 0;
    uint64_t db_index_size = 0;
    uint64_t db_seg_table_size = 0;
    uint64_t db_meta_size = 0;
    uint64_t db_data_region_size = 0;
    uint64_t db_size = 0;
    uint64_t device_capacity = 0;
    uint64_t data_theory_size = 0;

    uint32_t hash_table_size = options_.hashtable_size;

    uint32_t segment_size = options_.segment_size;

    int r = 0;
    r = metaDev_->Open(paths_);
    if (r < 0) {
        return false;
    } __DEBUG("Open device success.");

    device_capacity = metaDev_->GetDeviceCapacity();

    //Init Superblock region
    db_sb_size = SuperBlockManager::GetSuperBlockSizeOnDevice();
    __DEBUG("super block size; %ld",db_sb_size);

    //device capacity should be larger than size of (sb+index)
    if (device_capacity < db_sb_size) {
        __ERROR("The capacity of device is too small, can't store sb region");
        return NULL;
    }

    if (!sbMgr_->InitSuperBlockForCreateDB(0)) {
        return false;
    }

    __DEBUG("Init super block region success.");

    //Init Index region
    if (hash_table_size == 0) {
        hash_table_size = (device_capacity / ALIGNED_SIZE) * 2;
    }

    hash_table_size = IndexManager::ComputeHashSizeForPower2(hash_table_size);

    db_index_size = IndexManager::ComputeIndexSizeOnDevice(hash_table_size);
    __DEBUG("index region size; %ld",db_index_size);

    //device capacity should be larger than size of (sb+index)
    if (device_capacity < (db_sb_size + db_index_size)) {
        __ERROR("The capacity of device is too small, can't store index region");
        return NULL;
    }

    if (!idxMgr_->InitIndexForCreateDB(db_sb_size, hash_table_size)) {
        return false;
    } __DEBUG("Init index region success.");

    //Init Segment region
    uint64_t seg_total_size = device_capacity - db_sb_size - db_index_size;

    if (segment_size <= 0 || segment_size > seg_total_size) {
        __ERROR("Improper segment size, %d",segment_size);
        return false;
    }

    number_segments = SegmentManager::ComputeSegNum(seg_total_size,
                                                    segment_size);

    uint64_t segtable_offset = db_sb_size + db_index_size;

    if (!dataStor_->segMgr_->InitSegmentForCreateDB(segtable_offset, segment_size,
                                             number_segments)) {
        __ERROR("Segment region init failed.");
        return false;
    }

    __DEBUG("Init segment region success.");
    db_seg_table_size = SegmentManager::ComputeSegTableSizeOnDisk(number_segments);

    db_meta_size = db_sb_size + db_index_size + db_seg_table_size;
    db_data_region_size = dataStor_->segMgr_->GetDataRegionSize();
    db_size = db_meta_size + db_data_region_size;

    r = metaDev_->SetNewDBZero(db_meta_size);
    if (r < 0) {
        return false;
    }

    DBSuperBlock sb(MAGIC_NUMBER, hash_table_size, num_entries, segment_size,
                    number_segments, 0, db_sb_size, db_index_size,
                    db_seg_table_size, db_data_region_size, device_capacity,
                    data_theory_size);
    sbMgr_->SetSuperBlock(sb);

    return true;
}

bool MetaStor::LoadMetaData() {
    if (metaDev_->Open(paths_) < 0) {
        __ERROR("Could not open device\n");
        return false;
    }

    uint64_t offset = 0;
    if (!sbMgr_->LoadSuperBlockFromDevice(offset)) {
        return false;
    }

    uint32_t hashtable_size = sbMgr_->GetHTSize();
    uint64_t db_sb_size = SuperBlockManager::GetSuperBlockSizeOnDevice();
    offset += db_sb_size;
    if (!idxMgr_->LoadIndexFromDevice(offset, hashtable_size)) {
        return false;
    }

    uint64_t db_index_size = IndexManager::ComputeIndexSizeOnDevice(hashtable_size);
    offset += db_index_size;
    uint32_t segment_size = sbMgr_->GetSegmentSize();
    uint32_t number_segments = sbMgr_->GetSegmentNum();
    uint32_t current_seg = sbMgr_->GetCurSegmentId();
    if (!dataStor_->segMgr_->LoadSegmentTableFromDevice(offset, segment_size,
                                             number_segments, current_seg)) {
        return false;
    }

    return true;
}

bool MetaStor::PersistMetaData() {
    if (!idxMgr_->WriteIndexToDevice()) {
        return false;
    }

    if (!dataStor_->segMgr_->WriteSegmentTableToDevice()) {
        return false;
    }

    if (!sbMgr_->WriteSuperBlockToDevice()) {
        return false;
    }

    return true;
}

bool MetaStor::PersistIndexToDevice() {
    return true;
}

bool MetaStor::PersistSSTsToDevice() {
    return true;
}

bool MetaStor::FastRecovery() {
    return true;
}


} // namespace hlkvds
