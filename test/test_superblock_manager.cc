#include <string>
#include <iostream>
#include "test_base.h"

class test_superblock_manager : public TestBase {
public:
    Options opts;

    virtual void SetUp() {
    }
};

TEST_F(test_superblock_manager, InitSuperBlockForCreateDB) {
}

int main(int argc, char **argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}

