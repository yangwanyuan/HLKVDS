#include <iostream>
#include <stdio.h>
#include <stdint.h>
#include <stdlib.h>
#include <string>

#include "KeyDigestHandle.h"

using namespace kvdb;

int main(){
    //char* key_raw = "";
    //char* key_raw = "a";
    //char* key_raw = "abc";
    char* key_raw = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";
    std::cout << "The \"" << key_raw << "\" hashcode:\t";

    int key_len = strlen(key_raw);

    kvdb::Kvdb_Key key;
    key.value = key_raw;
    key.len = key_len; 

    kvdb::Kvdb_Digest *result = new kvdb::Kvdb_Digest;

    KeyDigestHandle::ComputeDigest(&key, *result);

    string final_res = KeyDigestHandle::Tostring(result);
    std::cout << final_res << std::endl;

    printf("hash index from key is:\t");
    printf("%u", KeyDigestHandle::Hash(&key));
    printf("\n");

    printf("hash index from digest is:\t");
    printf("%u", KeyDigestHandle::Hash(result));
    printf("\n");
    
    delete result;
}
