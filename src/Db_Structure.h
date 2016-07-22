/* -*- Mode: C++; c-basic-offset: 4; indent-tabs-mode: nil -*- */
#ifndef _KV_DB_DB_STRUCTURE_H_
#define _KV_DB_DB_STRUCTURE_H_

#include <stdint.h>

#define MAGIC_NUMBER 0xffff0001


#define DEBUG

#ifdef DEBUG
#include <unistd.h>
#define __DEBUG(x...) do {                                    \
        fprintf(stderr,"[%d] %s(%d) ",(int)getgid(), __FUNCTION__,__LINE__); \
        fprintf(stderr,##x);             \
        fprintf(stderr,"\n");              \
}while(0)
#else
    #define __DEBUG(x...)
#endif


#define OK 0
#define ERR (-1)

namespace kvdb {
    struct DBSuperBlock {
        uint64_t magic_number;
        uint64_t hashtable_size;
        uint64_t number_elements;
        uint64_t deleted_elements;
        double max_deleted_ratio;
        double max_load_factor;
        off_t data_insertion_point;  // offset to where the next record should go
        off_t data_start_point;  // offset showing where first record is
        uint64_t segment_size;
        uint64_t number_segments;
    } __attribute__((__packed__));


    /*
      Hash Entry Format
      D = Is slot deleted: 1 means deleted, 0 means not deleted.  Needed for lazy deletion
      V = Is slot empty: 0 means empty, 1 means taken
      K = Key fragment
      O = Offset bits
      ________________________________________________
      |DVKKKKKKKKKKKKKKOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOO|
      ¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯
    */
    struct HashEntryOnDisk {
        uint16_t present_key;
        uint32_t offset;
    } __attribute__((__packed__));


    struct DataHeader {
        uint32_t data_length;
        uint32_t key_length;
        bool deleteLog;
    } __attribute__((__packed__));

    struct HashEntry {
        uint16_t present_key;
        uint32_t offset;
        uint32_t pointer;
    } __attribute__((__packed__));

    struct SegmentHeader {
        time_t time_stamp;
        uint16_t check_sum;
        int nkeys;
    } __attribute__ ((__packed__));

}  // namespace kvdb

#endif  // #define _KV_DB_DB_STRUCTURE_H_
