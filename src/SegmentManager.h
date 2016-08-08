#ifndef _KV_DB_SEGMENT_MANAGER_H_
#define _KV_DB_SEGMENT_MANAGER_H_

#include <vector>

#include "Db_Structure.h"
#include "BlockDevice.h"
#include "Utils.h"

using namespace std;

namespace kvdb{
    class SegmentOnDisk{
    public:
        uint64_t time_stamp;
        uint32_t checksum;
        uint32_t number_keys;
    public:
        SegmentOnDisk(uint64_t tstamp): time_stamp(tstamp), checksum(0), number_keys(0){}
        ~SegmentOnDisk();
    }__attribute__((__packed__));


    class SegmentStat{
    public:
        bool state;
        int inuse_size;

    public:
        SegmentStat() : state(false), inuse_size(0){}
        ~SegmentStat();
    };

    class SegmentManager{
    public:
        bool InitSegmentForCreateDB(uint64_t device_capacity, uint64_t meta_size, uint64_t segment_size);
        bool LoadSegmentTableFromDevice();

        int FindNextSegId();
        ssize_t ComputeSegOffsetFromId(uint64_t seg_id);
        uint64_t ComputeSegIdFromOffset(ssize_t offset);

        SegmentManager(BlockDevice* bdev);
        ~SegmentManager();

    private:
        vector<SegmentStat> *seg_table;
        int num_seg;
        int current_seg;

        BlockDevice* m_bdev;
        //Timing* m_timestamp;


    };
} //end namespace kvdb
#endif // #ifndef _KV_DB_SEGMENT_MANAGER_H_
