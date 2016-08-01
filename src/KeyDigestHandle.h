#ifndef _KV_DB_KEYDIGESTHANDLE_H_
#define _KV_DB_KEYDIGESTHANDLE_H_

#include <string.h>
#include <stdint.h>
#include <string>
#include "rmd160.h"

#define RMDsize 160
#define DIGEST_LEN RMDsize/8

using namespace std;

namespace kvdb{
    struct Kvdb_Key{
        char* value;
        size_t len;
    };

    struct Kvdb_Digest{
        unsigned char value[DIGEST_LEN];
    };

    class KeyDigestHandle{
    public:
        static uint32_t Hash(Kvdb_Key *key);
        static uint32_t Hash(Kvdb_Digest *digest);
        static void ComputeDigest(Kvdb_Key *key, Kvdb_Digest &digest);
        static string Tostring(Kvdb_Digest *digest);

    private:
        KeyDigestHandle();

    };
}// namespace kvdb

#endif //#ifndef _KV_DB_KEYDIGESTHANDLE_H_
