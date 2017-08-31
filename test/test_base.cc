#include "test_base.h"

using namespace hlkvds;

KvdbDS* TestBase::Create_DB(int db_size) {
    int ht_size = db_size;
    int segment_size = SEGMENT_SIZE;

    opts.hashtable_size = ht_size;
    opts.segment_size = segment_size;
    opts.gc_upper_level = 0.7;

    return KvdbDS::Create_KvdbDS(FILENAME, opts);
}
