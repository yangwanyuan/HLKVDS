#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <unistd.h>

#include "SuperBlockManager.h"
#include "Db_Structure.h"

namespace hlkvds {

bool SuperBlockManager::Get(char* buff, uint64_t length) {
    if (length != SuperBlockManager::SuperBlockSizeOnDevice()) {
        return false;
    }
    memcpy((void *)buff, (const void*)&sb_, SuperBlockManager::SizeOfDBSuperBlock());

    uint64_t reserved_region_length = SuperBlockManager::ReservedRegionLength();

    char *cont_ptr = buff + SuperBlockManager::SizeOfDBSuperBlock();
    memcpy((void*)cont_ptr, (const void*)resCont_, reserved_region_length);
    return true;
}

bool SuperBlockManager::Set(char* buff, uint64_t length) {
    if (length != SuperBlockManager::SuperBlockSizeOnDevice()) {
        return false;
    }
    memcpy((void*)&sb_, (const void*)buff, SuperBlockManager::SizeOfDBSuperBlock());

    uint64_t reserved_region_length = SuperBlockManager::ReservedRegionLength();
    char * cont_ptr = buff + SuperBlockManager::SizeOfDBSuperBlock();
    memcpy((void*)resCont_, (const void*)cont_ptr, reserved_region_length);

    return true;
}

void SuperBlockManager::SetSuperBlock(DBSuperBlock& sb) {
    std::lock_guard<std::mutex> l(mtx_);
    sb_.magic_number            = sb.magic_number;
    sb_.index_ht_size           = sb.index_ht_size;
    sb_.index_region_offset     = sb.index_region_offset;
    sb_.index_region_length     = sb.index_region_length;
    sb_.sst_total_num           = sb.sst_total_num;
    sb_.sst_region_offset       = sb.sst_region_offset;
    sb_.sst_region_length       = sb.sst_region_length;
    sb_.data_store_type         = sb.data_store_type;
    sb_.entry_count             = sb.entry_count;
    sb_.entry_theory_data_size  = sb.entry_theory_data_size;
    sb_.grace_close_flag        = sb.grace_close_flag;
}

SuperBlockManager::SuperBlockManager(Options &opt) :
    resCont_(NULL), options_(opt) {
        uint64_t reserved_region_length = SuperBlockManager::ReservedRegionLength();
        resCont_ = new char[reserved_region_length];
        memset(resCont_, 0, reserved_region_length);
}

SuperBlockManager::~SuperBlockManager() {
    delete[] resCont_;
}

void SuperBlockManager::SetEntryCount(uint32_t num) {
    std::lock_guard<std::mutex> l(mtx_);
    sb_.entry_count = num;
}

bool SuperBlockManager::SetReservedContent(char* content, uint64_t length) {
    std::lock_guard<std::mutex> l(mtx_);
    if (length != SuperBlockManager::ReservedRegionLength()) {
        return false;
    }
    memcpy((void*)resCont_, (const void*)content, length);
    return true;
}

bool SuperBlockManager::GetReservedContent(char* content, uint64_t length) {
    std::lock_guard<std::mutex> l(mtx_);
    if (length != SuperBlockManager::ReservedRegionLength()) {
        return false;
    }
    memcpy((void*)content,(const void*)resCont_, length);
    return true;
}

void SuperBlockManager::SetDataTheorySize(uint64_t size) {
    std::lock_guard<std::mutex> l(mtx_);
    sb_.entry_theory_data_size = size;
}

} // namespace hlkvds
