#include <stdio.h>
#include <stdlib.h>

#include "KeyDigestHandle.h"

namespace kvdb{

    void KeyDigestHandle::ComputeDigest(Kvdb_Key *key, Kvdb_Digest &digest)
    /*
     * returns RMD(message)
     * message should be a string terminated by '\0'
     */
    {
        dword         MDbuf[RMDsize/32];   /* contains (A, B, C, D(, E))   */
        //static byte   hashcode[RMDsize/8]; /* for final hash-value         */
        byte          hashcode[RMDsize/8]; /* for final hash-value         */
        dword         X[16];               /* current 16-word chunk        */
        unsigned int  i;                   /* counter                      */
        dword         length;              /* length in bytes of message   */
        dword         nbytes;              /* # of bytes not yet processed */
     
        /* initialize */
        MDinit(MDbuf);
        //length = (dword)strlen((char *)message);
        length = key->len;
     
        /* process message in 16-word chunks */
        for (nbytes=length; nbytes > 63; nbytes-=64) {
            for (i=0; i<16; i++) {
                X[i] = BYTES_TO_DWORD(key->value);
                key->value += 4;
            }
            compress(MDbuf, X);
        }                                    /* length mod 64 bytes left */
     
        /* finish: */
        MDfinish(MDbuf, (byte *)key->value, length, 0);
     
        for (i=0; i<RMDsize/8; i+=4) {
            hashcode[i]   =  MDbuf[i>>2];         /* implicit cast to byte  */
            hashcode[i+1] = (MDbuf[i>>2] >>  8);  /*  extracts the 8 least  */
            hashcode[i+2] = (MDbuf[i>>2] >> 16);  /*  significant bits.     */
            hashcode[i+3] = (MDbuf[i>>2] >> 24);
        }
     
        //return (unsigned char*)hashcode;
        memcpy(digest.value, hashcode, DIGEST_LEN);
    }
    
    uint32_t KeyDigestHandle::Hash(Kvdb_Key *key)
    {
        Kvdb_Digest *result = new Kvdb_Digest;
        ComputeDigest(key, *result);

        uint32_t hash_value = Hash(result);

        delete result;
        return hash_value;
    }

    uint32_t KeyDigestHandle::Hash(Kvdb_Digest *digest)
    {
        uint32_t hash_value;
        hash_value = (digest->value[16]) + (digest->value[17] << 8) + (digest->value[18] << 16) + (digest->value[19] << 24);
        return hash_value;
    }
    
    string KeyDigestHandle::Tostring(Kvdb_Digest *digest)
    {
        char *res = (char*)malloc(40);
        for (int i = 0; i < 20; i++)
        {
            sprintf(&res[2 * i], "%02x", digest->value[i]);
        }
        string result = string((const char*)res);
        free(res);
        return result;
    }
    
} // namespace kvdb
    
