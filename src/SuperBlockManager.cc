#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <unistd.h>

#include "SuperBlockManager.h"
#include "Db_Structure.h"

namespace hlkvds {

bool SuperBlockManager::Get(char* buff, uint64_t length) {
    if (length != SuperBlockManager::GetSuperBlockSizeOnDevice()) {
        return false;
    }
    memcpy((void *)buff, (const void*)&sb_, SuperBlockManager::SizeOfDBSuperBlock());
    return true;
}

bool SuperBlockManager::Set(char* buff, uint64_t length) {
    if (length != SuperBlockManager::GetSuperBlockSizeOnDevice()) {
        return false;
    }
    memcpy((void*)&sb_, (const void*)buff, SuperBlockManager::SizeOfDBSuperBlock());
    return true;
}

void SuperBlockManager::SetSuperBlock(DBSuperBlock& sb) {
    std::lock_guard < std::mutex > l(mtx_);
    sb_.hashtable_size = sb.hashtable_size;
    sb_.number_elements = sb.number_elements;
    sb_.segment_size = sb.segment_size;
    sb_.number_segments = sb.number_segments;
    sb_.current_segment = sb.current_segment;
    sb_.db_sb_size = sb.db_sb_size;
    sb_.db_index_size = sb.db_index_size;
    sb_.db_seg_table_size = sb.db_seg_table_size;
    sb_.db_data_region_size = sb.db_data_region_size;
    sb_.device_capacity = sb.device_capacity;
    sb_.data_theory_size = sb.data_theory_size;
}

uint64_t SuperBlockManager::GetSuperBlockSizeOnDevice() {
    uint64_t sb_size_pages = SuperBlockManager::SizeOfDBSuperBlock()
            / getpagesize();
    return (sb_size_pages + 1) * getpagesize();
}

SuperBlockManager::SuperBlockManager(Options &opt) :
    options_(opt) {
}

SuperBlockManager::~SuperBlockManager() {
}

void SuperBlockManager::SetElementNum(uint32_t num) {
    std::lock_guard < std::mutex > l(mtx_);
    sb_.number_elements = num;
}

void SuperBlockManager::SetCurSegId(uint32_t id) {
    std::lock_guard < std::mutex > l(mtx_);
    sb_.current_segment = id;
}

void SuperBlockManager::SetDataTheorySize(uint64_t size) {
    std::lock_guard < std::mutex > l(mtx_);
    sb_.data_theory_size = size;
}

} // namespace hlkvds
