#include <string>
#include <iostream>
#include "test_base.h"

class test_datastor : public TestBase {
public:
    Options opts;

    virtual void SetUp() {
    }
};

TEST_F(test_datastor, TestDataStor) {
}

int main(int argc, char **argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}

