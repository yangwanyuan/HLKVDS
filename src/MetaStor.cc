#include <math.h>

#include "MetaStor.h"
#include "Db_Structure.h"
#include "BlockDevice.h"
#include "SuperBlockManager.h"
#include "IndexManager.h"
#include "DataStor.h"

using namespace std;

namespace hlkvds {

MetaStor::MetaStor(const char* paths, vector<BlockDevice*> &dev_vec,
                    SuperBlockManager *sbm, IndexManager *im, Options &opt)
    : paths_(paths), bdVec_(dev_vec), metaDev_(NULL),
    sbMgr_(sbm), idxMgr_(im), options_(opt), sbOff_(-1),
    idxOff_(-1), sstOff_(-1) {
}

MetaStor::~MetaStor() {}

void MetaStor::InitDataStor(SimpleDS_Impl* ds) {
    dataStor_ = ds;
}

void MetaStor::checkMetaDevice() {
    metaDev_ = bdVec_[0];
}

bool MetaStor::CreateMetaData() {

    checkMetaDevice();

    uint32_t num_entries = 0;
    uint32_t number_segments = 0;
    uint64_t db_sb_size = 0;
    uint64_t db_index_size = 0;
    uint64_t db_seg_table_size = 0;
    uint64_t db_meta_size = 0;
    uint64_t db_data_region_size = 0;
    uint64_t device_capacity = 0;
    uint64_t data_theory_size = 0;

    uint32_t hashtable_size = options_.hashtable_size;
    uint32_t segment_size = options_.segment_size;

    //Init Block Device
    device_capacity = metaDev_->GetDeviceCapacity();

    //Init Superblock region
    db_sb_size = SuperBlockManager::GetSuperBlockSizeOnDevice();

    //device capacity should be larger than size of (sb+index)
    if (device_capacity < db_sb_size) {
        __ERROR("The capacity of device is too small, can't store sb region");
        return false;
    }

    sbOff_ = 0;
    if (!createSuperBlock()) {
        return false;
    }

    __DEBUG("Init super block region success.");

    //Init Index region
    if (hashtable_size == 0) {
        hashtable_size = (device_capacity / ALIGNED_SIZE) * 2;
    }
    hashtable_size = IndexManager::ComputeHashSizeForPower2(hashtable_size);

    db_index_size = IndexManager::ComputeIndexSizeOnDevice(hashtable_size);
    __DEBUG("index region size; %ld",db_index_size);

    //device capacity should be larger than size of (sb+index)
    if (device_capacity < (db_sb_size + db_index_size)) {
        __ERROR("The capacity of device is too small, can't store index region");
        return false;
    }

    idxOff_ = db_sb_size;
    if (!createIndex(hashtable_size, db_index_size)) {
        return false;
    }
    
    __DEBUG("Init index region success.");

    //Init Segment region
    uint64_t sb_index_total_size = db_sb_size + db_index_size;
    uint64_t seg_total_size = device_capacity - sb_index_total_size;

    if (segment_size <= 0 || segment_size > seg_total_size) {
        __ERROR("Improper segment size, %d",segment_size);
        return false;
    }

    number_segments = dataStor_->ComputeTotalSegNum(segment_size, sb_index_total_size);

    uint64_t segtable_offset = db_sb_size + db_index_size;

    sstOff_ = segtable_offset;
    if (!createDataStor(segment_size, number_segments)) {
        __ERROR("Segment region init failed.");
        return false;
    }

    __DEBUG("Init segment region success.");

    //Set zero to device.
    db_seg_table_size = dataStor_->ComputeTotalSSTsSizeOnDisk(number_segments);
    db_meta_size = db_sb_size + db_index_size + db_seg_table_size;

    int r = metaDev_->SetNewDBZero(db_meta_size);
    if (r < 0) {
        return false;
    }


    //Set Superblock
    db_data_region_size = dataStor_->GetDataRegionSize();
    DBSuperBlock sb(MAGIC_NUMBER, hashtable_size, num_entries, segment_size,
                    number_segments, 0, db_sb_size, db_index_size,
                    db_seg_table_size, db_data_region_size, device_capacity,
                    data_theory_size);
    sbMgr_->SetSuperBlock(sb);

    return true;
}

bool MetaStor::LoadMetaData() {

    checkMetaDevice();

    //Load SuperBlock
    sbOff_ = 0;
    if (!LoadSuperBlock()) {
        __ERROR("Load SuperBlock failed.");
        return false;
    }

    //Load Index
    uint32_t hashtable_size = sbMgr_->GetHTSize();
    uint64_t db_sb_size = sbMgr_->GetSbSize();
    uint64_t db_index_size = sbMgr_->GetIndexSize();

    idxOff_ = sbOff_ + db_sb_size;
    if(!LoadIndex(hashtable_size, db_index_size)) {
        __ERROR("Load Index failed.");
        return false;
    }

    //Load SST
    sstOff_ = idxOff_ + db_index_size;
    uint32_t segment_size = sbMgr_->GetSegmentSize();
    uint32_t number_segments = sbMgr_->GetSegmentNum();
    if (!LoadSSTs(segment_size,number_segments)) {
        __ERROR("Load SSTs failed.");
        return false;
    }

    return true;
}

bool MetaStor::PersistMetaData() {
    if (!PersistIndexToDevice()) {
        __WARN("Persist Index failed.");
        return false;
    }

    if (!PersistSSTsToDevice()) {
        __WARN("Persist SSTs failed.");
        return false;
    }

    if (!PersistSuperBlockToDevice()) {
        __WARN("Persist SB failed.");
        return false;
    }

    return true;
}

bool MetaStor::createSuperBlock() {
    uint64_t length = SuperBlockManager::GetSuperBlockSizeOnDevice();
    char *buff = new char[length];
    memset(buff, 0, length);

    if (!sbMgr_->Set(buff, length))
    {
        __ERROR("Can't Set SuperBlock, Load Failed!");
        delete[] buff;
        return false;
    }
    delete[] buff;
    return true;
}

bool MetaStor::LoadSuperBlock(){
    uint64_t length = SuperBlockManager::GetSuperBlockSizeOnDevice();
    char *buff = new char[length];
    memset(buff, 0, length);

    if ((uint64_t) metaDev_->pRead(buff, length, sbOff_) != length){
        __ERROR("Could not read superblock from device at %ld\n", sbOff_);
        delete[] buff;
        return false;
    }

    if (!sbMgr_->Set(buff, length))
    {
        __ERROR("Can't Set SuperBlock, Load Failed!");
        delete[] buff;
        return false;
    }
    delete[] buff;
    return true;
}

bool MetaStor::PersistSuperBlockToDevice(){
    int ret = false;
    char *align_buf;
    uint64_t length = SuperBlockManager::GetSuperBlockSizeOnDevice();
    ret = posix_memalign((void **)&align_buf, 4096, length);
    if (ret < 0) {
        __ERROR("Can't allocate memory for SuperBlock, Persist Failed!");
        return false;
    }

    memset(align_buf, 0, length);

    if (!sbMgr_->Get(align_buf, length)) {
        __ERROR("Can't Get SuperBlock, Persist Failed!");
        free(align_buf);
        return false;
    }

    if ((uint64_t) metaDev_->pWrite(align_buf, length, sbOff_) != length) {
        __ERROR("Could not write superblock at position %ld\n", sbOff_);
        free(align_buf);
        return false;
    }

    free(align_buf);
    return true;
}

bool MetaStor::createIndex(uint32_t ht_size, uint64_t index_size) {
    idxMgr_->InitMeta(ht_size, index_size, 0, 0);

    uint64_t length = index_size;
    char *buff = new char[length];
    memset(buff, 0, length);

    if(!idxMgr_->Set(buff, length)){
        __ERROR("Can't Set Index Region, Load Failed!");
        delete[] buff;
        return false;
    }

    delete[] buff;
    return true;
}

bool MetaStor::LoadIndex(uint32_t ht_size, uint64_t index_size) {
    idxMgr_->InitMeta(ht_size, index_size, sbMgr_->GetDataTheorySize(), sbMgr_->GetElementNum());

    uint64_t length = index_size;
    char *buff = new char[length];
    memset(buff, 0, length);

    if ((uint64_t) metaDev_->pRead(buff, length, idxOff_) != length) {
        __ERROR("Could not read Index region from device at %ld\n", idxOff_);
        delete[] buff;
        return false;
    }

    if(!idxMgr_->Set(buff, length)){
        __ERROR("Can't Set Index Region, Load Failed!");
        delete[] buff;
        return false;
    }

    delete[] buff;
    return true;
}

bool MetaStor::PersistIndexToDevice() {
    int ret = false;
    char *align_buf;
    uint64_t length = sbMgr_->GetIndexSize();
    ret = posix_memalign((void **)&align_buf, 4096, length);
    memset(align_buf, 0, length);
    if (ret < 0) {
        __ERROR("Can't allocate memory for Index, Persist Failed!");
        return false;
    }
    memset(align_buf, 0, length);

    if (!idxMgr_->Get(align_buf, length)) {
        __ERROR("Can't Get Index, Persist Failed!");
        free(align_buf);
        return false;
    }

    if ((uint64_t) metaDev_->pWrite(align_buf, length, idxOff_) != length) {
        __ERROR("Could not write Index at position %ld\n", idxOff_);
        free(align_buf);
        return false;
    }

    free(align_buf);

    idxMgr_->UpdateMetaToSB();
    return true;
}

bool MetaStor::createDataStor(uint32_t segment_size, uint32_t number_segments) {
    uint32_t align_bit = log2(ALIGNED_SIZE);
    if (segment_size != (segment_size >> align_bit) << align_bit) {
        __ERROR("Segment Size is not page aligned!");
        return false;
    }
    dataStor_->InitMeta(sstOff_, segment_size, number_segments, 0);

    uint64_t length = dataStor_->ComputeTotalSSTsSizeOnDisk(number_segments);
    char *buff = new char[length];
    memset(buff, 0 , length);

    if (!dataStor_->SetAllSSTs(buff, length)) {
        __ERROR("Can't Set SST Region, Load Failed!");
        delete[] buff;
        return false;
    }

    delete[] buff;
    return true;
}

bool MetaStor::LoadSSTs(uint32_t segment_size, uint32_t number_segments) {
    dataStor_->InitMeta(sstOff_, segment_size, number_segments, sbMgr_->GetCurSegmentId());

    uint64_t length = dataStor_->ComputeTotalSSTsSizeOnDisk(number_segments);
    char *buff = new char[length];
    memset(buff, 0 , length);
    if ((uint64_t) metaDev_->pRead(buff, length, sstOff_) != length) {
        __ERROR("Could not read SST region from device at %ld\n", idxOff_);
        delete[] buff;
        return false;
    }

    if (!dataStor_->SetAllSSTs(buff, length)) {
        __ERROR("Can't Set SST Region, Load Failed!");
        delete[] buff;
        return false;
    }

    delete[] buff;
    return true;
}

bool MetaStor::PersistSSTsToDevice() {
    int ret = false;
    char *align_buf;
    uint64_t length = sbMgr_->GetSegTableSize();
    ret = posix_memalign((void **)&align_buf, 4096, length);
    memset(align_buf, 0, length);
    if (ret < 0) {
        __ERROR("Can't allocate memory for SSTs, Persist Failed!");
        return false;
    }
    memset(align_buf, 0, length);

    if (!dataStor_->GetAllSSTs(align_buf, length)) {
        __ERROR("Can't Get SSTs, Persist Failed!");
        free(align_buf);
        return false;
    }

    if ((uint64_t) metaDev_->pWrite(align_buf, length, sstOff_) != length) {
        __ERROR("Could not write SSTs at position %ld\n", sstOff_);
        free(align_buf);
        return false;
    }

    free(align_buf);

    dataStor_->UpdateMetaToSB();
    return true;
}

bool MetaStor::FastRecovery() {
    return true;
}


} // namespace hlkvds
