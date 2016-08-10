#ifndef _KV_DB_SUPERBLOCK_H_
#define _KV_DB_SUPERBLOCK_H_

#include "Db_Structure.h"
#include "BlockDevice.h"

using namespace std;

namespace kvdb{

    class DBSuperBlock {
    public:
        uint64_t magic_number;
        uint64_t hashtable_size;
        uint64_t number_elements;
        uint64_t deleted_elements;
        uint64_t segment_size;
        uint64_t number_segments;
        uint64_t current_segment;
        ssize_t db_sb_size;
        ssize_t db_index_size;
        ssize_t db_data_size;
        ssize_t device_size;
        
    public:
        DBSuperBlock(uint64_t magic, uint64_t ht_size, uint64_t num_eles, 
                uint64_t num_del, uint64_t seg_size, uint64_t num_seg, 
                uint64_t cur_seg, ssize_t sb_size, ssize_t index_size, 
                ssize_t data_size, ssize_t dev_size) :
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
        DBSuperBlock& GetSuperBlock();

        bool IsElementFull() { return m_superblock->number_elements == m_superblock->hashtable_size;}

        SuperBlockManager(BlockDevice* bdev);
        ~SuperBlockManager();
    private:
        BlockDevice* m_bdev;
        DBSuperBlock* m_superblock;
    
    };
}// namespace kvdb

#endif //#ifndef _KV_DB_SUPERBLOCK_H_
