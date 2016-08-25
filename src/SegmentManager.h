#ifndef _KV_DB_SEGMENT_MANAGER_H_
#define _KV_DB_SEGMENT_MANAGER_H_

#include <stdio.h>
#include <vector>

#include "Db_Structure.h"
#include "BlockDevice.h"
#include "IndexManager.h"
#include "Utils.h"

using namespace std;

namespace kvdb{

    class DataHeader;
    class HashEntry;
    class IndexManager;

    enum SegUseStat{un_use, in_use};

    class SegmentOnDisk{
    public:
        uint64_t time_stamp;
        uint32_t checksum;
        uint32_t number_keys;
    public:
        SegmentOnDisk();
        SegmentOnDisk(uint32_t num);
        ~SegmentOnDisk();
        void Update();
    }__attribute__((__packed__));


    class SegmentStat{
    public:
        SegUseStat state;
        int inuse_size;

    public:
        SegmentStat() : state(un_use), inuse_size(0){}
        ~SegmentStat();
    };

    class SegmentManager{
    public:
        bool InitSegmentForCreateDB(uint64_t device_capacity, uint64_t meta_size, uint32_t segment_size);
        bool LoadSegmentTableFromDevice(uint64_t meta_size, uint32_t segment_size, uint32_t num_seg, uint32_t current_seg);

        uint32_t GetNowSegId(){ return m_current_seg; }
        uint32_t GetNumberOfSeg(){ return m_num_seg; }
        uint64_t GetDataRegionSize(){ return m_seg_size * m_num_seg; }
        uint32_t GetSegmentSize(){ return m_seg_size; }
        uint32_t GetSegmentHeadSize() { return sizeof(SegmentOnDisk); }
        

        bool GetEmptySegId(uint32_t& seg_id);
        bool ComputeSegOffsetFromId(uint32_t seg_id, uint64_t& offset);
        bool ComputeSegIdFromOffset(uint64_t offset, uint32_t& seg_id);
        bool ComputeSegOffsetFromOffset(uint64_t offset, uint64_t& seg_offset);

        bool ComputeDataOffsetPhyFromEntry(HashEntry* entry, uint64_t& data_offset);

        void Update(uint32_t seg_id);

        SegmentManager(BlockDevice* bdev);
        ~SegmentManager();

    private:
        vector<SegmentStat> m_seg_table;
        uint64_t m_begin_offset;
        uint32_t m_seg_size;
        uint32_t m_num_seg;
        uint32_t m_current_seg;
        bool m_is_full;
        

        BlockDevice* m_bdev;


    };

} //end namespace kvdb
#endif // #ifndef _KV_DB_SEGMENT_MANAGER_H_
