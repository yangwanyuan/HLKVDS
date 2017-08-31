#ifndef _HLKVDS_GCMANAGER_H_
#define _HLKVDS_GCMANAGER_H_

#include <sys/types.h>
#include <mutex>

#include "Db_Structure.h"
#include "BlockDevice.h"
#include "hlkvds/Options.h"
#include "IndexManager.h"
#include "SegmentManager.h"
#include "Segment.h"

namespace hlkvds {

class GcManager {
public:
    ~GcManager();
    GcManager(BlockDevice* bdev, IndexManager* im, SegmentManager* sm,
              Options &opt);

    bool ForeGC();
    void BackGC();
    void FullGC();

private:
    uint32_t doMerge(std::multimap<uint32_t, uint32_t> &cands_map);

    bool readSegment(uint64_t seg_offset);
    void loadSegKV(list<KVSlice*> &slice_list, uint32_t num_keys,
                   uint64_t phy_offset);

    bool loadKvList(uint32_t seg_id, std::list<KVSlice*> &slice_list);
    void cleanKvList(std::list<KVSlice*> &slice_list);

private:
    SegmentManager* segMgr_;
    IndexManager* idxMgr_;
    BlockDevice* bdev_;
    Options &options_;

    std::mutex gcMtx_;

    char *dataBuf_;
};

}//namespace hlkvds

#endif //#ifndef _HLKVDS_GCMANAGER_H_
