#ifndef _KV_DB_KEYDIGESTHANDLE_H_
#define _KV_DB_KEYDIGESTHANDLE_H_

#include <string.h>
#include <stdint.h>
#include <string>
#include "rmd160.h"

#include "Db_Structure.h"

//#define RMDsize 160
//#define DIGEST_LEN RMDsize/8

using namespace std;

namespace kvdb{
    class Kvdb_Key{
    private:
        char* value;
        uint32_t len;

    public:
        uint32_t GetLen(){return len;}
        char* GetValue(){return value;}
        Kvdb_Key(): value(NULL), len(0){}
        Kvdb_Key(const char* key, uint32_t key_len);
        Kvdb_Key(Kvdb_Key& toBeCopied);
        ~Kvdb_Key();
    }__attribute__((__packed__));

    class Kvdb_Digest{
    public:
        uint32_t value[KEYDIGEST_INT_NUM];

    public:
        Kvdb_Digest();
        ~Kvdb_Digest();
        Kvdb_Digest(const Kvdb_Digest &toBeCopied);
        Kvdb_Digest& operator=(const Kvdb_Digest& toBeCopied);
        bool operator==(const Kvdb_Digest& toBeCompare);
    }__attribute__((__packed__));

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
