#ifndef _KV_DB_SUPERBLOCK_H_
#define _KV_DB_SUPERBLOCK_H_

#include <mutex>

#include "Db_Structure.h"
#include "BlockDevice.h"
#include "Utils.h"

using namespace std;

namespace kvdb{
    class SuperBlockManager;

    class DBSuperBlock {
    public:
    //private:
        friend class SuperBlockManager;
        uint32_t magic_number;
        uint32_t hashtable_size;
        uint32_t number_elements;
        uint32_t deleted_elements;
        uint32_t segment_size;
        uint32_t number_segments;
        uint32_t current_segment;
        uint64_t db_sb_size;
        uint64_t db_index_size;
        uint64_t db_seg_table_size;
        uint64_t db_data_size;
        uint64_t device_capacity;
        
    public:
        DBSuperBlock(uint32_t magic, uint32_t ht_size, uint32_t num_eles, 
                uint32_t num_del, uint32_t seg_size, uint32_t num_seg, 
                uint32_t cur_seg, uint64_t sb_size, uint64_t index_size, 
                uint64_t seg_table_size, uint64_t data_size, uint64_t dev_size) :
            magic_number(magic), hashtable_size(ht_size), 
            number_elements(num_eles),deleted_elements(num_del), 
            segment_size(seg_size), number_segments(num_seg), 
            current_segment(cur_seg), db_sb_size(sb_size), 
            db_index_size(index_size), db_seg_table_size(seg_table_size), db_data_size(data_size),
            device_capacity(dev_size){}

        DBSuperBlock() :  
            magic_number(0), hashtable_size(0), number_elements(0),
            deleted_elements(0), segment_size(0), number_segments(0), 
            current_segment(0), db_sb_size(0), db_index_size(0), 
            db_seg_table_size(0), db_data_size(0), device_capacity(0){}

        uint32_t GetMagic() const { return magic_number; }
        uint32_t GetHTSize() const { return hashtable_size; }
        uint32_t GetElementNum() const { return number_elements; }
        uint32_t GetDeletedNum() const { return deleted_elements; }
        uint32_t GetSegmentSize() const { return segment_size; }
        uint32_t GetSegmentNum() const { return number_segments; }
        uint32_t GetCurSegmentId() const { return current_segment; }
        uint64_t GetSbSize() const { return db_sb_size; }
        uint64_t GetIndexSize() const { return db_index_size; }
        uint64_t GetSegTableSize() const {return db_seg_table_size;}
        uint64_t GetDataRegionSize() const { return db_data_size; }
        uint64_t GetDeviceCapacity() const { return device_capacity; }

        ~DBSuperBlock(){}
    } __attribute__((__packed__));

    class SuperBlockManager{
    public:
        static inline size_t SizeOfDBSuperBlock(){ return sizeof(DBSuperBlock); }
        static uint64_t GetSuperBlockSizeOnDevice(); 

        bool InitSuperBlockForCreateDB();

        bool LoadSuperBlockFromDevice(uint64_t offset);
        bool WriteSuperBlockToDevice(uint64_t offset);

        void SetSuperBlock(DBSuperBlock& sb);
        const DBSuperBlock& GetSuperBlock() const { return *sb_; }

        bool IsElementFull() const { return sb_->number_elements == sb_->hashtable_size; }
        uint32_t GetMagic() const { return sb_->magic_number; }
        uint32_t GetHTSize() const { return sb_->hashtable_size; }
        uint32_t GetElementNum() const { return sb_->number_elements; }
        uint32_t GetDeletedNum() const { return sb_->deleted_elements; }
        uint32_t GetSegmentSize() const { return sb_->segment_size; }
        uint32_t GetSegmentNum() const { return sb_->number_segments; }
        uint32_t GetCurSegmentId() const { return sb_->current_segment; }
        uint64_t GetSbSize() const { return sb_->db_sb_size; }
        uint64_t GetIndexSize() const { return sb_->db_index_size; }
        uint64_t GetSegTableSize() const { return sb_->db_seg_table_size; }
        uint64_t GetDataRegionSize() const { return sb_->db_data_size; }
        uint64_t GetDeviceCapacity() const { return sb_->device_capacity; }

        void AddElement();
        void DeleteElement();
        void AddDeleted();
        void DeleteDeleted();
        void SetCurSegId(uint32_t id);

        SuperBlockManager(BlockDevice* bdev);
        ~SuperBlockManager();

    private:
        BlockDevice* bdev_;
        DBSuperBlock* sb_;

        mutable std::mutex mtx_;

    };
}// namespace kvdb

#endif //#ifndef _KV_DB_SUPERBLOCK_H_
