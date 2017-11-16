#ifndef _HLKVDS_SUPERBLOCK_H_
#define _HLKVDS_SUPERBLOCK_H_

#include <mutex>

#include "hlkvds/Options.h"
#include "Utils.h"

#define SB_SIZE_ONDISK 4096;

namespace hlkvds {

class SuperBlockManager;

struct DBSuperBlock {
      uint32_t magic_number;

      uint32_t index_ht_size;
      uint64_t index_region_offset;
      uint64_t index_region_length;

      uint32_t sst_total_num;
      uint64_t sst_region_offset;
      uint64_t sst_region_length;

      uint32_t data_store_type;
      uint64_t reserved_region_offset;
      uint64_t reserved_region_length;

      uint32_t entry_count;
      uint64_t entry_theory_data_size;

      bool grace_close_flag;

      //will remove
      uint32_t segment_size;
      uint32_t segment_num;
      uint32_t cur_seg_id;

    /*
     *struct SimpleDS_SB_RESERVED {
     *public :
     *  uint32_t segment_size;
     *  uint32_t volume_num;
     *}
     *struct SimpleDS_SB_Volume {
     *  char[20] device_path;
     *  uint32_t segment_num;
     *  uint32_t cur_seg_id;
     *}
     * */

    DBSuperBlock(uint32_t magic, uint32_t ht_size, uint64_t idx_offset,
                uint64_t idx_len, uint32_t sst_total, uint64_t sst_offset,
                uint64_t sst_len, uint32_t ds_type, uint64_t reserved_offset,
                uint64_t reserved_len, uint32_t num_eles, uint64_t data_size,
                bool grace_close, uint32_t seg_size, uint32_t seg_num, uint32_t cur_id) :
        magic_number(magic), index_ht_size(ht_size),
        index_region_offset(idx_offset), index_region_length(idx_len),
        sst_total_num(sst_total), sst_region_offset(sst_offset),
        sst_region_length(sst_len), data_store_type(ds_type),
        reserved_region_offset(reserved_offset),
        reserved_region_length(reserved_len),
        entry_count(num_eles), entry_theory_data_size(data_size),
        grace_close_flag(grace_close),
        segment_size(seg_size), segment_num(seg_num), cur_seg_id(cur_id){
    }


    DBSuperBlock() :
        magic_number(0), index_ht_size(0), index_region_offset(0),
        index_region_length(0), sst_total_num(0), sst_region_offset(0),
        sst_region_length(0), data_store_type(0), reserved_region_offset(0),
        reserved_region_length(0), entry_count(0), entry_theory_data_size(0),
        grace_close_flag(0),
        segment_size(0), segment_num(0), cur_seg_id(0){
    }

    ~DBSuperBlock() {
    }
}__attribute__((__packed__));

class SuperBlockManager {
public:
    static inline size_t SizeOfDBSuperBlock() {
        return sizeof(DBSuperBlock);
    }

    static uint64_t GetSuperBlockSizeOnDevice() {
        return SB_SIZE_ONDISK;
    }

    bool Get(char* buff, uint64_t length);
    bool Set(char* buff, uint64_t length);

    void SetSuperBlock(DBSuperBlock& sb);
    const DBSuperBlock& GetSuperBlock() const {
        return sb_;
    }

    bool IsElementFull() const {
        return sb_.entry_count == sb_.index_ht_size;
    }
    uint32_t GetMagic() const {
        return sb_.magic_number;
    }
    uint32_t GetHTSize() const {
        return sb_.index_ht_size;
    }
    uint64_t GetIndexRegionOffset() const {
        return sb_.index_region_offset;
    }
    uint64_t GetIndexRegionLength() const {
        return sb_.index_region_length;
    }
    uint32_t GetSSTTotalNum() const {
        return sb_.sst_total_num;
    }
    uint64_t GetSSTRegionOffset() const {
        return sb_.sst_region_offset;
    }
    uint64_t GetSSTRegionLength() const {
        return sb_.sst_region_length;
    }
    uint32_t GetDataStoreType() const {
        return sb_.data_store_type;
    }
    uint64_t GetReservedRegionOffset() const {
        return sb_.reserved_region_offset;
    }
    uint64_t GetReservedRegionLength() const {
        return sb_.reserved_region_length;
    }
    uint32_t GetEntryCount() const {
        return sb_.entry_count;
    }
    uint64_t GetDataTheorySize() const {
        return sb_.entry_theory_data_size;
    }
    uint64_t GetGraceCloseFlag() const {
        return sb_.grace_close_flag;
    }

    //will remove
    uint32_t GetSegmentSize() const {
        return sb_.segment_size;
    }
    uint32_t GetSegmentNum() const {
        return sb_.segment_num;
    }
    uint32_t GetCurrentSegId() const {
        return sb_.cur_seg_id;
    }

    bool GetReservedContent(char* content, uint64_t length);
    void SetEntryCount(uint32_t num);
    bool SetReservedContent(char* content, uint64_t length);
    void SetDataTheorySize(uint64_t size);

    //will remove
    void SetCurSegId(uint32_t id);

    SuperBlockManager(Options &opt);
    ~SuperBlockManager();

private:
    DBSuperBlock sb_;
    char *resCont_;

    Options &options_;

    std::mutex mtx_;

};
}// namespace hlkvds

#endif //#ifndef _HLKVDS_SUPERBLOCK_H_
