#ifndef _KV_DB_GCMANAGER_H_
#define _KV_DB_GCMANAGER_H_

#include <sys/types.h>
#include <mutex>

#include "Db_Structure.h"
#include "BlockDevice.h"
#include "IndexManager.h"
#include "SegmentManager.h"

namespace kvdb{

    class GCSegment{
    public:
        GCSegment();
        ~GCSegment();
        GCSegment(const GCSegment& toBeCopied);
        GCSegment& operator=(const GCSegment& toBeCopied);

        GCSegment(SegmentManager* sm, IndexManager* im, BlockDevice* bdev);

        void MergeSeg(vector<uint32_t> &cands);
        bool WriteSegToDevice(uint32_t seg_id);
        void UpdateToIndex();
        void FreeSegs();

        //uint32_t GetFreeSize() const { return freeSize_; }
        uint32_t GetFreeSize() const { return tailPos_ - headPos_; }

    private:
        void copyHelper(const GCSegment& toBeCopied);
        bool mergeSeg(uint32_t seg_id);
        bool readSegFromDevice(uint64_t seg_offset);
        void loadSegKV(list<KVSlice*> &slice_list, uint32_t num_keys, uint64_t phy_offset);
        bool tryPutBatch(list<KVSlice*> &slice_list);
        void putBatch(list<KVSlice*> &slice_list);

        bool isCanFit(KVSlice *slice, uint32_t head_pos, uint32_t tail_pos);
        void deleteKVList(list<KVSlice*> &slice_list);
        void fillSlice();
        void copyToData();
        bool _writeDataToDevice();

    private:
        uint32_t segId_;
        SegmentManager* segMgr_;
        IndexManager* idxMgr_;
        BlockDevice* bdev_;
        uint32_t segSize_;
        KVTime persistTime_;

        uint32_t headPos_;
        uint32_t tailPos_;
        //uint32_t freeSize_;

        int32_t keyNum_;
        int32_t keyAlignedNum_;

        SegmentOnDisk *segOndisk_;

        //seg list will do gc.
        std::vector<uint32_t> segVec_;

        std::list<KVSlice*> sliceList_;
        char *dataBuf_;
        //char *writeBuf_;
        //char *tempBuf_;
    };

    class GcManager {
        public:
            GcManager();
            ~GcManager();
            GcManager(const GcManager& toBeCopied);
            GcManager& operator=(const GcManager& toBeCopied);

            GcManager(BlockDevice* bdev, IndexManager* im, SegmentManager* sm);

            bool ForeGC();
            void BackGC(float utils);
            void FullGC();

        private:
            void doGC(std::vector<uint32_t> &candsOD);

        private:
           SegmentManager* segMgr_;
           IndexManager* idxMgr_;
           BlockDevice* bdev_;

           std::mutex gcMtx_;
    };



}//namespace kvdb

#endif //#ifndef _KV_DB_GCMANAGER_H_
