/* -*- Mode: C++; c-basic-offset: 4; indent-tabs-mode: nil -*- */
#ifndef _KV_DB_DB_STRUCTURE_H_
#define _KV_DB_DB_STRUCTURE_H_

#include <stdint.h>

#define MAGIC_NUMBER 0xffff0001

#define RMDsize 160
#define KEYDIGEST_INT_NUM RMDsize/(sizeof(uint32_t)*8) // RIPEMD-160/(sizeof(uint32_t)*8) 160/32


#define DEBUG

#ifdef DEBUG
//#include <unistd.h>
//#include <sys/types.h>
#include <pthread.h>
#define __DEBUG(x...) do {                                    \
        fprintf(stderr,"[Pid:%ld] %s (%s:%d) : ",(uint64_t)pthread_self(), __FUNCTION__, __FILE__,__LINE__); \
        fprintf(stderr,##x);             \
        fprintf(stderr,"\n");              \
}while(0)
#else
    #define __DEBUG(x...)
#endif


#define OK 0
#define ERR (-1)

typedef uint16_t U16;
typedef uint32_t U32;
typedef uint64_t U64;
typedef int16_t S16;
typedef int32_t S32;
typedef int64_t S64;

namespace kvdb {

}  // namespace kvdb

#endif  // #define _KV_DB_DB_STRUCTURE_H_
