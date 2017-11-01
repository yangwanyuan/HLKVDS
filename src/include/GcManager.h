#ifndef _HLKVDS_GCMANAGER_H_
#define _HLKVDS_GCMANAGER_H_

#include <sys/types.h>
#include <mutex>
#include <list>
#include <map>

#include "hlkvds/Options.h"

namespace hlkvds {

class BlockDevice;
class IndexManager;
class Volumes;
class KVSlice;

class GcManager {
public:
    ~GcManager();
    GcManager(BlockDevice* bdev, IndexManager* im, Volumes* vol, Options &opt);

    bool ForeGC();
    void BackGC();
    void FullGC();

private:
    uint32_t doMerge(std::multimap<uint32_t, uint32_t> &cands_map);

    bool readSegment(uint64_t seg_offset);
    void loadSegKV(std::list<KVSlice*> &slice_list, uint32_t num_keys,
                   uint64_t phy_offset);

    bool loadKvList(uint32_t seg_id, std::list<KVSlice*> &slice_list);
    void cleanKvList(std::list<KVSlice*> &slice_list);

private:
    Volumes* vol_;
    BlockDevice* bdev_;
    IndexManager* idxMgr_;
    Options &options_;

    std::mutex gcMtx_;

    char *dataBuf_;
};

}//namespace hlkvds

#endif //#ifndef _HLKVDS_GCMANAGER_H_
