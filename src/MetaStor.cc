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

    uint32_t index_ht_size          = 0;
    uint64_t index_region_offset    = 0;
    uint64_t index_region_length    = 0;
    uint32_t sst_total_num          = 0;
    uint64_t sst_region_offset      = 0;
    uint64_t sst_region_length      = 0;
    uint32_t data_store_type        = 0;
    uint64_t reserved_region_offset = 0;
    uint64_t reserved_region_length = 0;
    uint32_t entry_count            = 0;
    uint64_t entry_theory_data_size = 0;
    bool grace_close_flag           = false;
    //will remove
    uint32_t segment_size           = 0;
    uint32_t segment_num            = 0;
    uint32_t cur_seg_id             = 0;

    index_ht_size = options_.hashtable_size;
    segment_size = options_.segment_size;

    //Init Block Device
    uint64_t meta_device_capacity = metaDev_->GetDeviceCapacity();

    //Init Superblock region
    uint64_t sb_region_length = SuperBlockManager::SuperBlockSizeOnDevice();

    if (meta_device_capacity < sb_region_length) {
        __ERROR("The capacity of device is too small, can't store sb region");
        return false;
    }

    sbOff_ = 0;
    if (!createSuperBlock()) {
        return false;
    }

    __DEBUG("Init super block region success.");

    //Init Index region
    if (index_ht_size == 0) {
        index_ht_size = (meta_device_capacity / ALIGNED_SIZE) * 2;
    }
    index_ht_size = IndexManager::ComputeHashSizeForPower2(index_ht_size);

    index_region_length = IndexManager::ComputeIndexSizeOnDevice(index_ht_size);
    __DEBUG("index region size; %ld", index_region_length);

    if (meta_device_capacity < (sb_region_length + index_region_length)) {
        __ERROR("The capacity of device is too small, can't store index region");
        return false;
    }

    index_region_offset = sb_region_length;
    idxOff_ = sb_region_length;
    if (!createIndex(index_ht_size, index_region_length)) {
        return false;
    }
    
    __DEBUG("Init index region success.");

    //Init SST region
    sst_region_offset = sb_region_length + index_region_length;
    uint64_t meta_device_remain_capacity = meta_device_capacity - sst_region_offset;

    if (segment_size <= 0 || segment_size > meta_device_remain_capacity) {
        __ERROR("Improper segment size, %d",segment_size);
        return false;
    }

    sstOff_ = sst_region_offset;
    if (!createDataStor(segment_size)) {
        __ERROR("Segment region init failed.");
        return false;
    }

    sst_total_num = dataStor_->GetTotalSegNum();

    __DEBUG("Init segment region success.");

    //Set zero to device.
    sst_region_length = dataStor_->GetSSTsLengthOnDisk();
    uint64_t db_meta_region_length = sb_region_length + index_region_length + sst_region_length;

    int r = metaDev_->SetNewDBZero(db_meta_region_length);
    if (r < 0) {
        return false;
    }

    //Init reserve region
    data_store_type = 0;
    reserved_region_offset = SuperBlockManager::ReservedRegionOffset();
    reserved_region_length = SuperBlockManager::ReservedRegionLength();

    grace_close_flag = 0;

    segment_num = sst_total_num;

    //Set SuperBlock
    DBSuperBlock sb(MAGIC_NUMBER, index_ht_size, index_region_offset, index_region_length,
                    sst_total_num, sst_region_offset, sst_region_length, data_store_type,
                    reserved_region_offset, reserved_region_length, entry_count,
                    entry_theory_data_size, grace_close_flag,
                    segment_size, segment_num, cur_seg_id);
    sbMgr_->SetSuperBlock(sb);

    //Set SuperBlock reserved region
    char * reserved_content = new char[reserved_region_length];
    dataStor_->GetSBReservedContent(reserved_content, reserved_region_length);
    sbMgr_->SetReservedContent(reserved_content, reserved_region_length);
    delete[] reserved_content;

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
    uint32_t index_ht_size = sbMgr_->GetHTSize();
    uint64_t sb_region_length = SuperBlockManager::SuperBlockSizeOnDevice();
    uint64_t index_region_length = sbMgr_->GetIndexRegionLength();

    idxOff_ = sbOff_ + sb_region_length;
    if(!LoadIndex(index_ht_size, index_region_length)) {
        __ERROR("Load Index failed.");
        return false;
    }

    //Load SST
    sstOff_ = idxOff_ + index_region_length;
    if (!LoadSSTs()) {
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
    uint64_t length = SuperBlockManager::SuperBlockSizeOnDevice();
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
    uint64_t length = SuperBlockManager::SuperBlockSizeOnDevice();
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
    uint64_t length = SuperBlockManager::SuperBlockSizeOnDevice();
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
    idxMgr_->InitMeta(ht_size, index_size, sbMgr_->GetDataTheorySize(), sbMgr_->GetEntryCount());

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
    uint64_t length = sbMgr_->GetIndexRegionLength();
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

bool MetaStor::createDataStor(uint32_t segment_size) {
    uint32_t align_bit = log2(ALIGNED_SIZE);
    if (segment_size != (segment_size >> align_bit) << align_bit) {
        __ERROR("Segment Size is not page aligned!");
        return false;
    }
    dataStor_->CreateAllVolumes(sstOff_, segment_size);

    uint64_t length = dataStor_->GetSSTsLengthOnDisk();

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

bool MetaStor::LoadSSTs() {
    uint64_t reserved_region_length = SuperBlockManager::ReservedRegionLength();

    char *reserved_content = new char[reserved_region_length];
    sbMgr_->GetReservedContent(reserved_content, reserved_region_length);
    dataStor_->SetSBReservedContent(reserved_content, reserved_region_length);
    delete[] reserved_content;

    dataStor_->OpenAllVolumes();

    uint64_t length = dataStor_->GetSSTsLengthOnDisk();
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
    uint64_t length = sbMgr_->GetSSTRegionLength();
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

    uint64_t reserved_region_length = SuperBlockManager::ReservedRegionLength();

    char *reserved_content = new char[reserved_region_length];
    dataStor_->GetSBReservedContent(reserved_content, reserved_region_length);
    sbMgr_->SetReservedContent(reserved_content, reserved_region_length);
    delete[] reserved_content;

    return true;
}

bool MetaStor::FastRecovery() {
    return true;
}


} // namespace hlkvds
