/* -*- Mode: C++; c-basic-offset: 4; indent-tabs-mode: nil -*- */
#ifndef _KV_DB_DB_STRUCTURE_H_
#define _KV_DB_DB_STRUCTURE_H_

#include <stdint.h>

#define MAGIC_NUMBER 0xffff0001

#define RMDsize 160
#define KEYDIGEST_INT_NUM RMDsize/(sizeof(uint32_t)*8) // RIPEMD-160/(sizeof(uint32_t)*8) 160/32

//#define EXPIRED_TIME 80 // unit microseconds
#define EXPIRED_TIME 10000 // unit microseconds
#define SIZE_4K 4096

//#define DEBUG
#define INFO
#define WARN
#define ERROR

#ifdef DEBUG
#include <pthread.h>
#define __DEBUG(x...) do {                                    \
        fprintf(stderr,"[DEBUG] [Pid:%ld] %s (%s:%d) : ",(uint64_t)pthread_self(), __FUNCTION__, __FILE__,__LINE__); \
        fprintf(stderr,##x);             \
        fprintf(stderr,"\n");              \
}while(0)
#else
    #define __DEBUG(x...)
#endif

#ifdef INFO
#include <pthread.h>
#define __INFO(x...) do {                                    \
        fprintf(stderr,"[INFO]  [Pid:%ld] %s (%s:%d) : ",(uint64_t)pthread_self(), __FUNCTION__, __FILE__,__LINE__); \
        fprintf(stderr,##x);             \
        fprintf(stderr,"\n");              \
}while(0)
#else
    #define __INFO(x...)
#endif

#ifdef WARN
#include <pthread.h>
#define __WARN(x...) do {                                    \
        fprintf(stderr,"[WARN]  [Pid:%ld] %s (%s:%d) : ",(uint64_t)pthread_self(), __FUNCTION__, __FILE__,__LINE__); \
        fprintf(stderr,##x);             \
        fprintf(stderr,"\n");              \
}while(0)
#else
    #define __WARN(x...)
#endif

#ifdef ERROR
#include <pthread.h>
#define __ERROR(x...) do {                                    \
        fprintf(stderr,"[ERROR] [Pid:%ld] %s (%s:%d) : ",(uint64_t)pthread_self(), __FUNCTION__, __FILE__,__LINE__); \
        fprintf(stderr,##x);             \
        fprintf(stderr,"\n");              \
}while(0)
#else
    #define __ERROR(x...)
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
