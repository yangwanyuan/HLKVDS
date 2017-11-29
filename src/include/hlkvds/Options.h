#ifndef _HLKVDS_OPTIONS_H_
#define _HLKVDS_OPTIONS_H_

#include <stdint.h>

namespace hlkvds {
struct Options {
    //use in readCache
    bool disable_cache;
    int cache_size;
    int cache_policy;
    int slru_partition;

    //Open DB parameters
    int expired_time;
    int seg_write_thread;
    int shards_num;
    double seg_full_rate;
    double gc_upper_level;
    double gc_lower_level;

    //Create DB parameters
    int datastor_type;
    int segment_size;
    int hashtable_size;
    int secondary_seg_size;

    Options();
};
} // namespace hlkvds

#endif //#define _HLKVDS_OPTIONS_H_
