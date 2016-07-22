#include "hashutil.h"
#include <string>
#include <iostream>

using namespace kvdb;

int main(){
    std::cout << "hello world" << std::endl;

    while(1)
    {
        std::string str;
        std::cout << "please input key:" << std::endl;
        std::cin >> str;
        int len = str.length();
        uint32_t rc = HashUtil::BobHash(str.c_str(), len, 0 );
        std::cout << "the hash value is " << rc << std::endl;

    }
    //const char* str = "hello";
    //uint32_t rc = HashUtil::BobHash(str, 5, 0 );
    //std::cout << rc << std::endl;
    return 0;

}
