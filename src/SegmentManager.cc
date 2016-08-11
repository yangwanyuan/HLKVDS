#include "SegmentManager.h"

namespace kvdb{

    SegmentOnDisk::SegmentOnDisk():
        checksum(0), number_keys(0)
    {
        time_stamp = Timing::GetNow();
    }

    SegmentOnDisk::SegmentOnDisk(uint32_t num):
        checksum(0), number_keys(num)
    {
        time_stamp = Timing::GetNow();
    }

    SegmentOnDisk::~SegmentOnDisk()
    {
        return;
    }
    
    void SegmentOnDisk::Update()
    {
        time_stamp = Timing::GetNow();
    }

    SegmentStat::~SegmentStat()
    {
        return;
    }

    bool SegmentManager::InitSegmentForCreateDB(uint64_t device_capacity, uint64_t meta_size, uint32_t segment_size)
    {
        if ((segment_size != ((segment_size>>12)<<12)) || (device_capacity < meta_size))
        {
            return false;
        }

        m_begin_offset = meta_size;
        m_seg_size = segment_size;
        m_num_seg = (device_capacity - meta_size) / segment_size;
        m_current_seg = 0;

        
        //init segment table
        SegmentStat seg_stat;
        for (uint32_t seg_index = 0; seg_index < m_num_seg; seg_index++)
        {
           m_seg_table.push_back(seg_stat);
        }

        //write all segments to device
        SegmentOnDisk seg_ondisk;
        uint64_t offset = m_begin_offset;
        for (uint32_t seg_index = 0; seg_index < m_num_seg; seg_index++)
        {
            //write seg to device
            if ( m_bdev->pWrite(&seg_ondisk, sizeof(SegmentOnDisk), offset) != sizeof(SegmentOnDisk))
            {
                perror("can not write segment to device!");
                return false;
            }
            offset += m_seg_size;
        }
        
        //SegmentOnDisk seg_ondisk(now_time);
        return true;
    }

    bool SegmentManager::LoadSegmentTableFromDevice(uint64_t meta_size, uint32_t segment_size, uint32_t num_seg, uint32_t current_seg)
    {
        m_begin_offset = meta_size;
        m_seg_size = segment_size;
        m_num_seg = num_seg;
        m_current_seg = current_seg;

        uint64_t offset = m_begin_offset;
        for (uint32_t seg_index = 0; seg_index < m_num_seg; seg_index++)
        {
            SegmentOnDisk seg_ondisk;
            //write seg to device
            if ( m_bdev->pRead(&seg_ondisk, sizeof(SegmentOnDisk), offset) != sizeof(SegmentOnDisk))
            {
                perror("can not write segment to device!");
                return false;
            }
            offset += m_seg_size;
            SegmentStat seg_stat;
            if (seg_ondisk.number_keys != 0)
            {
                seg_stat.state = in_use;
            }
            else
            {
                seg_stat.state = un_use;
            }
            m_seg_table.push_back(seg_stat);
        }
        
        return true;
    }

    bool SegmentManager::GetEmptySegId(uint32_t& seg_id)
    {
        //if (m_is_full)
        //{
        //    return false;
        //}

        //seg_id = m_current_seg; 

        //for first use !!
        if (m_seg_table[m_current_seg].state == un_use)
        {
            seg_id = m_current_seg;
            return true;
        }

        uint32_t seg_index = m_current_seg + 1; 
        
        while(seg_index != m_current_seg)
        {
            if (m_seg_table[seg_index].state == un_use)
            {
                seg_id = seg_index;
                return true;
            }
            seg_index++;
            if ( seg_index == m_num_seg)
            {
                seg_index = 0;
            }
        }
        return false;
    }

    bool SegmentManager::ComputeSegOffsetFromId(uint32_t seg_id, uint64_t& offset)
    {
        if (seg_id >= m_num_seg)
        {
            return false;
        }
        offset = m_begin_offset + seg_id * m_seg_size;   
        return true;
    }

    bool SegmentManager::ComputeSegIdFromOffset(uint64_t offset, uint32_t& seg_id)
    {
        uint64_t end_offset = m_begin_offset + ((uint64_t)m_seg_size) * ((uint64_t)(m_num_seg - 1));
        if (offset < m_begin_offset || offset > end_offset)
        {
            return false;
        }
        seg_id = (offset - m_begin_offset )/ m_seg_size;
        return true;
    }

    bool SegmentManager::ComputeSegOffsetFromOffset(uint64_t offset, uint64_t& seg_offset)
    {
        uint32_t seg_id = 0;
        if (!ComputeSegIdFromOffset(offset, seg_id))
        {
            return false;
        }
        if (!ComputeSegOffsetFromId(seg_id, seg_offset))
        {
            return false;
        }
        return true;
    }

    bool SegmentManager::ComputeDataOffsetPhyFromEntry(HashEntry* entry, uint64_t& data_offset)
    {
        uint64_t seg_offset = 0;
        uint64_t header_offset = entry->GetHeaderOffsetPhy();
        if (!ComputeSegOffsetFromOffset(header_offset, seg_offset))
        {
            return false;
        }
        data_offset = seg_offset + entry->GetDataOffsetInSeg();
        return true;
    }

    void SegmentManager::Update(uint32_t seg_id)
    {
        m_current_seg = seg_id;
        m_seg_table[m_current_seg].state = in_use;

        //m_seg_table[seg_id].state = in_use;

        //uint64_t seg_index = seg_id + 1; 
        //
        //while(seg_index != seg_id)
        //{
        //    if (m_seg_table[seg_index].state == un_use)
        //    {
        //        m_current_seg = seg_index;
        //        return;
        //    }
        //    seg_index++;
        //    if ( seg_index == m_num_seg)
        //    {
        //        seg_index = 0;
        //    }
        //}
        //
        //m_is_full = true;
        return;
    }

    SegmentManager::SegmentManager(BlockDevice* bdev) : 
        m_begin_offset(0), m_seg_size(0), m_num_seg(0), m_current_seg(0), m_is_full(false), m_bdev(bdev)
    {
        return;
    }

    SegmentManager::~SegmentManager()
    {
        m_seg_table.clear();
    }
} //end namespace kvdb
