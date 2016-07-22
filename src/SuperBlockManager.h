#ifndef _KV_DB_SUPERBLOCK_H_
#define _KV_DB_SUPERBLOCK_H_

#include "Db_Structure.h"
#include "BlockDevice.h"

using namespace std;

namespace kvdb{
    class SuperBlockManager{
    public:
        static uint64_t GetSuperBlockSizeOnDevice(); 

        bool InitSuperBlockForCreateDB();

        bool LoadSuperBlockFromDevice(uint64_t offset);
        bool WriteSuperBlockToDevice(uint64_t offset);

        void SetSuperBlock(struct DBSuperBlock sb);
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
