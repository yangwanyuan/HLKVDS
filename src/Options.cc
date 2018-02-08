#include "hlkvds/Options.h"
#include "Db_Structure.h"

namespace hlkvds {

Options::Options() :
	    disable_cache(DISABLE_CACHE),
        cache_size(CACHE_SIZE),
        cache_policy(CACHE_POLICY),
        slru_partition(SLRU_PARTITION),

        expired_time(EXPIRED_TIME),
        seg_write_thread(SEG_WRITE_THREAD),
        shards_num(1),
        seg_full_rate(SEG_FULL_RATE),
        gc_upper_level(GC_UPPER_LEVEL),
        gc_lower_level(GC_LOWER_LEVEL),
        aggregate_request(1),

        datastor_type(1),
        hashtable_size(0),
        segment_size(SEGMENT_SIZE),
        secondary_seg_size(SEGMENT_SIZE) {
}

} //namespace hlkvds
