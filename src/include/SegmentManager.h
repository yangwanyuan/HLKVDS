#ifndef _HLKVDS_SEGMENT_MANAGER_H_
#define _HLKVDS_SEGMENT_MANAGER_H_

#include <stdio.h>
#include <vector>
#include <mutex>
#include <map>

#include "hlkvds/Options.h"
#include "Utils.h"

namespace hlkvds {

class DataHeader;
class HashEntry;
class IndexManager;

enum struct SegUseStat {
    FREE,
    USED,
    RESERVED
};

class SegmentStat {
public:
    SegUseStat state;
    uint32_t free_size;
    uint32_t death_size;

public:
    SegmentStat() :
        state(SegUseStat::FREE), free_size(0), death_size(0) {
    }
    ~SegmentStat() {
    }
    SegmentStat(const SegmentStat& toBeCopied) :
        state(toBeCopied.state), free_size(toBeCopied.free_size),
                death_size(toBeCopied.death_size) {
    }
    SegmentStat& operator=(const SegmentStat& toBeCopied) {
        state = toBeCopied.state;
        free_size = toBeCopied.free_size;
        death_size = toBeCopied.death_size;
        return *this;
    }
};

class SegmentManager {
public:
    static uint64_t SizeOfSegmentStat() {
        return sizeof(SegmentStat);
    }

    uint32_t GetNowSegId() {
        return curSegId_;
    }

    bool Get(char *buf, uint64_t length);
    bool Set(char *buf, uint64_t length);

    bool Alloc(uint32_t& seg_id);
    bool AllocForGC(uint32_t& seg_id);
    void FreeForFailed(uint32_t seg_id);
    void FreeForGC(uint32_t seg_id);
    void Use(uint32_t seg_id, uint32_t free_size);
    void AddDeathSize(uint32_t seg_id, uint32_t death_size);

    void SortSegsByUtils(std::multimap<uint32_t, uint32_t> &cand_map,
                         double utils);

    uint32_t GetTotalFreeSegs();
    uint32_t GetTotalUsedSegs();

    SegmentManager(Options &opt, uint32_t segment_size, uint32_t segment_num, uint32_t cur_seg_id, uint32_t seg_size_bit);
    ~SegmentManager();

private:
    std::vector<SegmentStat> segTable_;
    uint32_t segSize_;
    uint32_t segSizeBit_;
    uint32_t segNum_;
    uint32_t curSegId_;
    uint32_t usedCounter_;
    uint32_t freedCounter_;
    uint32_t reservedCounter_;

    Options &options_;
    mutable std::mutex mtx_;

};

} //end namespace hlkvds
#endif // #ifndef _HLKVDS_SEGMENT_MANAGER_H_
