/* -*- Mode: C++; c-basic-offset: 4; indent-tabs-mode: nil -*- */
#ifndef _KV_DB_HASHFUN_H_
#define _KV_DB_HASHFUN_H_

#include <string>
#include <stdlib.h>
#include <stdint.h>

#define HASH_LITTLE_ENDIAN 1
#define PROBES_BEFORE_REHASH 8
#define HASH_COUNT 3

#define rot(x,k) (((x)<<(k)) | ((x)>>(32-(k))))
#define mix(a,b,c)                      \
    {                       \
    a -= c;  a ^= rot(c, 4);  c += b;   \
    b -= a;  b ^= rot(a, 6);  a += c;   \
    c -= b;  c ^= rot(b, 8);  b += a;   \
    a -= c;  a ^= rot(c,16);  c += b;   \
    b -= a;  b ^= rot(a,19);  a += c;   \
    c -= b;  c ^= rot(b, 4);  b += a;   \
    }

#define final(a,b,c)                \
    {                       \
    c ^= b; c -= rot(b,14);         \
    a ^= c; a -= rot(c,11);         \
    b ^= a; b -= rot(a,25);         \
    c ^= b; c -= rot(b,16);         \
    a ^= c; a -= rot(c,4);          \
    b ^= a; b -= rot(a,14);         \
    c ^= b; c -= rot(b,24);         \
    }
    // Assuming little endian


typedef uint32_t (*hashfn_t)(const void*, size_t);

namespace kvdb {

    class HashUtil {
    public:
    // Bob Jenkins Hash
    static uint32_t BobHash(const void *buf, size_t length, uint32_t seed = 0);
    static uint32_t BobHash(const std::string &s, uint32_t seed = 0);

    private:
    HashUtil();
    };

    class Hashes {
    public:
        static uint32_t h1(const void* buf, size_t len);
        static uint32_t h2(const void* buf, size_t len);
        static uint32_t h3(const void* buf, size_t len);
        static hashfn_t hashes[HASH_COUNT];
    private:
        Hashes();
    };

}  // namespace kvdb

#endif  // #ifndef _KV_DB_HASHFUN_H_
