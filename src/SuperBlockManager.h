#ifndef _KV_DB_SUPERBLOCK_H_
#define _KV_DB_SUPERBLOCK_H_

#include "Db_Structure.h"
#include "BlockDevice.h"

using namespace std;

namespace kvdb{

    struct DBSuperBlock {
        uint64_t magic_number;
        uint64_t hashtable_size;
        uint64_t number_elements;
        uint64_t deleted_elements;
        double max_deleted_ratio;
        double max_load_factor;
        off_t data_insertion_point;  // offset to where the next record should go
        off_t data_start_point;  // offset showing where first record is
        uint64_t segment_size;
        uint64_t number_segments;
    } __attribute__((__packed__));

    class SuperBlockManager{
    public:
        static uint64_t GetSuperBlockSizeOnDevice(); 

        bool InitSuperBlockForCreateDB();

        bool LoadSuperBlockFromDevice(uint64_t offset);
        bool WriteSuperBlockToDevice(uint64_t offset);

        void SetSuperBlock(DBSuperBlock sb);
        DBSuperBlock GetSuperBlock();

        bool IsElementFull() { return m_superblock->number_elements == m_superblock->hashtable_size;}

        SuperBlockManager(BlockDevice* bdev);
        ~SuperBlockManager();
    private:
        BlockDevice* m_bdev;
        DBSuperBlock* m_superblock;
    
    };
}// namespace kvdb

#endif //#ifndef _KV_DB_SUPERBLOCK_H_
