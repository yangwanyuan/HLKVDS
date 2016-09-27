#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <unistd.h>

#include "SuperBlockManager.h"

namespace kvdb{
    bool SuperBlockManager::InitSuperBlockForCreateDB()
    {
        sb_ = new DBSuperBlock;

        memset(sb_, 0, SuperBlockManager::SizeOfDBSuperBlock());
        return true;
    }

    bool SuperBlockManager::LoadSuperBlockFromDevice(uint64_t offset)
    {   
        sb_ = new DBSuperBlock;
        
        uint64_t length = SuperBlockManager::SizeOfDBSuperBlock();
        if ((uint64_t)bdev_->pRead(sb_, length, offset) != length)
        {
            __ERROR("Could not read superblock from device\n"); 
            return false;
        }

        return true;
    }

    bool SuperBlockManager::WriteSuperBlockToDevice(uint64_t offset)
    {
        uint64_t length = SuperBlockManager::SizeOfDBSuperBlock();
        if ((uint64_t)bdev_->pWrite(sb_, length, offset) != length)
        {
            __ERROR("Could not write superblock at position %ld\n", offset);
            return false;
        }
        return true;
    }
    
    void SuperBlockManager::SetSuperBlock(DBSuperBlock& sb)
    {
        //mtx_.Lock();
        sb_->hashtable_size        = sb.hashtable_size;
        sb_->number_elements       = sb.number_elements;
        sb_->deleted_elements      = sb.deleted_elements;
        sb_->segment_size          = sb.segment_size;
        sb_->number_segments       = sb.number_segments;
        sb_->current_segment       = sb.current_segment;
        sb_->db_sb_size            = sb.db_sb_size;
        sb_->db_index_size         = sb.db_index_size;
        sb_->db_data_size          = sb.db_data_size;
        sb_->device_size           = sb.device_size;
        //mtx_.Unlock();
    }


    uint64_t SuperBlockManager::GetSuperBlockSizeOnDevice()
    {
        uint64_t sb_size_pages = SuperBlockManager::SizeOfDBSuperBlock() / getpagesize();
        return (sb_size_pages + 1) * getpagesize();
    }


    SuperBlockManager::SuperBlockManager(BlockDevice* bdev):
        bdev_(bdev), sb_(NULL), mtx_(Mutex()){}

    SuperBlockManager::~SuperBlockManager()
    {
        delete sb_;
    }

    void SuperBlockManager::AddElement()
    {
        mtx_.Lock();
        sb_->number_elements++;
        mtx_.Unlock();
    }

    void SuperBlockManager::DeleteElement()
    {
        mtx_.Lock();
        sb_->number_elements--;
        mtx_.Unlock();
    }

    void SuperBlockManager::AddDeleted()
    {
        mtx_.Lock();
        sb_->deleted_elements++;
        mtx_.Unlock();
    }

    void SuperBlockManager::DeleteDeleted()
    {
        mtx_.Lock();
        sb_->deleted_elements--;
        mtx_.Unlock();
    }

    void SuperBlockManager::SetCurSegId(uint32_t id)
    {
        mtx_.Lock();
        sb_->current_segment = id;
        mtx_.Unlock();
    }

} // namespace kvdb
