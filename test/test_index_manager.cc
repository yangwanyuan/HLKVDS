#include <string>
#include <iostream>
#include "test_base.h"

class IndexManagerTest : public TestBase {
public:
    Options opts;

    virtual void SetUp() {
    }
};

TEST_F(IndexManagerTest, CalcIndexSizeOnDevice)
{

}

int main(int argc, char **argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}

