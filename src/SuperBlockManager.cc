#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <unistd.h>

#include "SuperBlockManager.h"

namespace kvdb{
    bool SuperBlockManager::InitSuperBlockForCreateDB()
    {
        m_superblock = (struct DBSuperBlock*)malloc(sizeof(struct DBSuperBlock));
        if(m_superblock == NULL){
            perror("could not malloc superblock\n");
            return false;
        }
        memset(m_superblock, 0, sizeof(struct DBSuperBlock));
        return true;
    }

    bool SuperBlockManager::LoadSuperBlockFromDevice(uint64_t offset)
    {   
        m_superblock = (struct DBSuperBlock*)malloc(sizeof(DBSuperBlock));
        if(m_superblock == NULL){
            perror("could not malloc superblock file\n");
            return false;
        }
        
        uint64_t length = sizeof(struct DBSuperBlock);
        if((uint64_t)m_bdev->pRead(m_superblock, length, offset) != length){
            perror("Could not read superblock from device\n"); 
            return false;
        }
        return true;
    }

    bool SuperBlockManager::WriteSuperBlockToDevice(uint64_t offset)
    {
        uint64_t length = sizeof(struct DBSuperBlock);
        if((uint64_t)m_bdev->pWrite(m_superblock, length, offset) != length){
            perror("Could not write malloc'd superblock at position 0\n");
            return false;
        }
        return true;
    }
    
    void SuperBlockManager::SetSuperBlock(struct DBSuperBlock sb)
    {
        m_superblock->hashtable_size        = sb.hashtable_size;
        m_superblock->number_elements       = sb.number_elements;
        m_superblock->deleted_elements      = sb.deleted_elements;
        m_superblock->max_deleted_ratio     = sb.max_deleted_ratio;
        m_superblock->max_load_factor       = sb.max_load_factor;
        m_superblock->data_insertion_point  = sb.data_insertion_point;
        m_superblock->data_start_point      = sb.data_start_point;
    }

    DBSuperBlock SuperBlockManager::GetSuperBlock()
    {
        return *m_superblock;
    }

    uint64_t SuperBlockManager::GetSuperBlockSizeOnDevice()
    {
        uint64_t sb_size_pages = sizeof(struct DBSuperBlock) / getpagesize();
        return (sb_size_pages + 1) * getpagesize();
    }

    SuperBlockManager::SuperBlockManager(BlockDevice* bdev):
        m_bdev(bdev), m_superblock(NULL)
    {
        return;
    }

    SuperBlockManager::~SuperBlockManager()
    {
        if (m_superblock)
            free(m_superblock);
    }

} // namespace kvdb
