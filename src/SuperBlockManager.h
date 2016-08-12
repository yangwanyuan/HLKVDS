#ifndef _KV_DB_SUPERBLOCK_H_
#define _KV_DB_SUPERBLOCK_H_

#include "Db_Structure.h"
#include "BlockDevice.h"

using namespace std;

namespace kvdb{

    class DBSuperBlock {
    public:
        uint32_t magic_number;
        uint32_t hashtable_size;
        uint32_t number_elements;
        uint32_t deleted_elements;
        uint32_t segment_size;
        uint32_t number_segments;
        uint32_t current_segment;
        uint64_t db_sb_size;
        uint64_t db_index_size;
        uint64_t db_data_size;
        uint64_t device_size;
        
    public:
        DBSuperBlock(uint32_t magic, uint32_t ht_size, uint32_t num_eles, 
                uint32_t num_del, uint32_t seg_size, uint32_t num_seg, 
                uint32_t cur_seg, uint64_t sb_size, uint64_t index_size, 
                uint64_t data_size, uint64_t dev_size) :
            magic_number(magic), hashtable_size(ht_size), 
            number_elements(num_eles),deleted_elements(num_del), 
            segment_size(seg_size), number_segments(num_seg), 
            current_segment(cur_seg), db_sb_size(sb_size), 
            db_index_size(index_size), db_data_size(data_size), 
            device_size(dev_size){}

        DBSuperBlock() :  
            magic_number(0), hashtable_size(0), number_elements(0),
            deleted_elements(0), segment_size(0), number_segments(0), 
            current_segment(0), db_sb_size(0), db_index_size(0), 
            db_data_size(0), device_size(0){}

        ~DBSuperBlock(){}
    } __attribute__((__packed__));

    class SuperBlockManager{
    public:
        static uint64_t GetSuperBlockSizeOnDevice(); 

        bool InitSuperBlockForCreateDB();

        bool LoadSuperBlockFromDevice(uint64_t offset);
        bool WriteSuperBlockToDevice(uint64_t offset);

        void SetSuperBlock(DBSuperBlock& sb);
        DBSuperBlock& GetSuperBlock() const {return *m_superblock;}

        bool IsElementFull() { return m_superblock->number_elements == m_superblock->hashtable_size;}

        SuperBlockManager(BlockDevice* bdev);
        ~SuperBlockManager();

    private:
        BlockDevice* m_bdev;
        DBSuperBlock* m_superblock;
    
    };
}// namespace kvdb

#endif //#ifndef _KV_DB_SUPERBLOCK_H_
