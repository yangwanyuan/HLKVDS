/* -*- Mode: C++; c-basic-offset: 4; indent-tabs-mode: nil -*- */
#ifndef _KV_DB_DB_STRUCTURE_H_
#define _KV_DB_DB_STRUCTURE_H_

#include <stdint.h>

#define MAGIC_NUMBER 0xffff0001

#define RMDsize 160
#define KEYDIGEST_INT_NUM RMDsize/(sizeof(uint32_t)*8) // RIPEMD-160/(sizeof(uint32_t)*8) 160/32


//#define DEBUG

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
    //struct DBSuperBlock {
    //    uint64_t magic_number;
    //    uint64_t hashtable_size;
    //    uint64_t number_elements;
    //    uint64_t deleted_elements;
    //    double max_deleted_ratio;
    //    double max_load_factor;
    //    off_t data_insertion_point;  // offset to where the next record should go
    //    off_t data_start_point;  // offset showing where first record is
    //    uint64_t segment_size;
    //    uint64_t number_segments;
    //} __attribute__((__packed__));


    //struct DataHeader {
    //    uint32_t key_digest[KEYDIGEST_SIZE];
    //    uint16_t data_size;
    //    uint32_t data_offset;
    //    uint32_t next_header_offset;
    //} __attribute__((__packed__));

    ////struct DataHeaderOffset{
    ////    uint32_t segment_id;
    ////    uint32_t header_offset;
    ////}__attribute__((__packed__));
    //struct DataHeaderOffset{
    //    uint32_t physical_offset;
    //}__attribute__((__packed__));

    //struct HashEntryOnDisk {
    //    DataHeader header;
    //    DataHeaderOffset header_offset;
    //} __attribute__((__packed__));

    //struct HashEntry {
    //    HashEntryOnDisk hash_entry;
    //    uint32_t pointer;
    //} __attribute__((__packed__));


}  // namespace kvdb

#endif  // #define _KV_DB_DB_STRUCTURE_H_
