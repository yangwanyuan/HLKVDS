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

    enum struct SegUseStat{FREE, USED};

    class SegmentOnDisk{
    public:
        uint64_t time_stamp;
        uint32_t checksum;
        uint32_t number_keys;
    public:
        SegmentOnDisk();
        ~SegmentOnDisk();
        SegmentOnDisk(const SegmentOnDisk& toBeCopied);
        SegmentOnDisk& operator=(const SegmentOnDisk& toBeCopied);

        SegmentOnDisk(uint32_t num);
        void Update();
        void SetKeyNum(uint32_t num) { number_keys = num; }
    }__attribute__((__packed__));


    class SegmentStat{
    public:
        SegUseStat state;
        int inuse_size;

    public:
        SegmentStat() : state(SegUseStat::FREE), inuse_size(0){}
        ~SegmentStat() {}
    };

    class SegmentManager{
    public:
        static inline size_t SizeOfSegOnDisk(){ return sizeof(SegmentOnDisk); }

        bool InitSegmentForCreateDB(uint64_t device_capacity, uint64_t meta_size, uint32_t segment_size);
        bool LoadSegmentTableFromDevice(uint64_t meta_size, uint32_t segment_size, uint32_t num_seg, uint32_t current_seg);

        uint32_t GetNowSegId(){ return curSegId_; }
        uint32_t GetNumberOfSeg(){ return segNum_; }
        uint64_t GetDataRegionSize(){ return (uint64_t)segNum_ << segSizeBit_; }
        uint32_t GetSegmentSize(){ return segSize_; }

        bool GetEmptySegId(uint32_t& seg_id);

        inline bool ComputeSegOffsetFromId(uint32_t seg_id, uint64_t& offset)
        {
            if (seg_id >= segNum_)
            {
                return false;
            }
            offset = startOffset_ + ((uint64_t)seg_id << segSizeBit_);
            return true;
        }

        inline bool ComputeSegIdFromOffset(uint64_t offset, uint32_t& seg_id)
        {
            if (offset < startOffset_ || offset > endOffset_)
            {
                return false;
            }
            seg_id = (offset - startOffset_ ) >> segSizeBit_;
            return true;
        }

        bool ComputeSegOffsetFromOffset(uint64_t offset, uint64_t& seg_offset);

        bool ComputeDataOffsetPhyFromEntry(HashEntry* entry, uint64_t& data_offset);

        void SetSegUsed(uint32_t seg_id);
        void SetSegFree(uint32_t seg_id);
        char* GetZeros() const { return zeros_; }

        SegmentManager(BlockDevice* bdev);
        ~SegmentManager();

        void Lock() { mtx_.Lock(); }
        void Unlock() { mtx_.Unlock(); }

    private:
        vector<SegmentStat> segTable_;
        uint64_t startOffset_;
        uint64_t endOffset_;
        uint32_t segSize_;
        uint32_t segSizeBit_;
        uint32_t segNum_;
        uint32_t curSegId_;
        bool isFull_;
        
        BlockDevice* bdev_;
        Mutex mtx_;

        char* zeros_;
    };

} //end namespace kvdb
#endif // #ifndef _KV_DB_SEGMENT_MANAGER_H_
