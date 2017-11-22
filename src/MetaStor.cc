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

void MetaStor::verifyMetaDevice() {
    metaDev_ = bdVec_[0];
}

bool MetaStor::TryLoadSB(int &datastor_type) {
    verifyMetaDevice();

    //Load SuperBlock
    sbOff_ = 0;
    if (!loadSuperBlock()) {
        __ERROR("Load SuperBlock failed.");
        return false;
    }

    datastor_type = sbMgr_->GetDataStoreType();
    return true;
}

bool MetaStor::CreateMetaData() {

    verifyMetaDevice();

    uint32_t index_ht_size          = 0;
    uint64_t index_region_offset    = 0;
    uint64_t index_region_length    = 0;
    uint32_t sst_total_num          = 0;
    uint64_t sst_region_offset      = 0;
    uint64_t sst_region_length      = 0;
    uint32_t data_store_type        = 0;
    uint32_t entry_count            = 0;
    uint64_t entry_theory_data_size = 0;
    bool grace_close_flag           = false;

    index_ht_size = options_.hashtable_size;
    uint32_t segment_size = options_.segment_size;

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
    index_ht_size = IndexManager::CalcHashSizeForPower2(index_ht_size);

    index_region_length = IndexManager::CalcIndexSizeOnDevice(index_ht_size);
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

    __DEBUG("Init segment region success.");

    //Set zero to device.
    sst_total_num = dataStor_->GetTotalSegNum();
    sst_region_length = dataStor_->GetSSTsLengthOnDisk();
    uint64_t db_meta_region_length = sb_region_length + index_region_length + sst_region_length;

    if ( !zeroDeviceMetaRegion(db_meta_region_length) ) {
        return false;
    }

    //Init reserve region
    data_store_type = 0;
    grace_close_flag = 0;

    //Set SuperBlock
    DBSuperBlock sb(MAGIC_NUMBER, index_ht_size, index_region_offset, index_region_length,
                    sst_total_num, sst_region_offset, sst_region_length, data_store_type,
                    entry_count, entry_theory_data_size, grace_close_flag);
    sbMgr_->SetSuperBlock(sb);

    //Set SuperBlock reserved region
    uint64_t reserved_region_length = SuperBlockManager::ReservedRegionLength();
    char * reserved_content = new char[reserved_region_length];
    if ( !dataStor_->GetSBReservedContent(reserved_content, reserved_region_length) ) {
        delete[] reserved_content;
        return false;
    }
    if ( !sbMgr_->SetReservedContent(reserved_content, reserved_region_length) ) {
        delete[] reserved_content;
        return false;
    }

    delete[] reserved_content;
    return true;
}

bool MetaStor::zeroDeviceMetaRegion(uint64_t meta_size) {
    if (meta_size % 4096 != 0) {
        return false;
    }

    char *align_buf;
    int block_size = metaDev_->GetBlockSize();
    block_size = (block_size > 4096? block_size : 4096);

    int ret = posix_memalign((void **)&align_buf, 4096, (size_t)block_size);
    if (ret < 0) {
        __ERROR("Can't allocate memory for SSTs, Persist Failed!");
        return false;
    }
    memset(align_buf, 0, block_size);

    uint64_t need_wte_bytes = meta_size;
    uint64_t wte_offset = 0;
    while (need_wte_bytes > 0) {
        uint64_t bytes_to_write = min(need_wte_bytes, (uint64_t)block_size);
        if ( !metaDev_->pWrite(align_buf, bytes_to_write, wte_offset) ) {
            __ERROR("error in fill_file_with_zeros write: %s\n", strerror(errno));
            return false;
        }
        need_wte_bytes -= bytes_to_write;
        wte_offset += bytes_to_write;
    }
    return true;
}

bool MetaStor::LoadMetaData() {

    //Load Index
    uint32_t index_ht_size = sbMgr_->GetHTSize();
    uint64_t sb_region_length = SuperBlockManager::SuperBlockSizeOnDevice();
    uint64_t index_region_length = sbMgr_->GetIndexRegionLength();

    idxOff_ = sbOff_ + sb_region_length;
    if(!loadIndex(index_ht_size, index_region_length)) {
        __ERROR("Load Index failed.");
        return false;
    }

    //Load DataStor
    sstOff_ = idxOff_ + index_region_length;
    if (!loadDataStor()) {
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

    if (!persistSuperBlockToDevice()) {
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

bool MetaStor::loadSuperBlock() {
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

bool MetaStor::persistSuperBlockToDevice() {
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

bool MetaStor::loadIndex(uint32_t ht_size, uint64_t index_size) {
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

bool MetaStor::loadDataStor() {
    uint64_t reserved_region_length = SuperBlockManager::ReservedRegionLength();

    char *reserved_content = new char[reserved_region_length];
    if ( !sbMgr_->GetReservedContent(reserved_content, reserved_region_length) ) {
        delete[] reserved_content;
        return false;
    }
    if ( !dataStor_->SetSBReservedContent(reserved_content, reserved_region_length) ) {
        delete[] reserved_content;
        return false;
    }
    delete[] reserved_content;

    if (!dataStor_->OpenAllVolumes()) {
        __ERROR("Could not Open All Volumes, Maybe device topology is inconsistent");
        return false;
    }

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

    uint64_t reserved_region_length = SuperBlockManager::ReservedRegionLength();

    char *reserved_content = new char[reserved_region_length];
    if ( !dataStor_->GetSBReservedContent(reserved_content, reserved_region_length) ) {
        delete[] reserved_content;
        return false;
    }

    if ( !sbMgr_->SetReservedContent(reserved_content, reserved_region_length) ) {
        delete[] reserved_content;
        return false;
    }

    delete[] reserved_content;
    return true;
}

bool MetaStor::FastRecovery() {
    return true;
}


} // namespace hlkvds
