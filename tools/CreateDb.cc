#include <iostream>
#include <string>
#include <sstream>
#include "hyperds/Kvdb.h"
#include "hyperds/Options.h"

using namespace kvdb;

void CreateExample(string filename) {

    Options opts;
    if (!kvdb::DB::CreateDB(filename, opts)) {
        std::cout << "CreateDB Failed!" << std::endl;
        return;
    }

}

int main(int argc, char** argv) {
    string filename = argv[1];

    CreateExample(filename);
    return 0;
}
