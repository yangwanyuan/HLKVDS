#include <string>
#include <iostream>

#include "Kvdb_Impl.h"
#include "gtest/gtest.h"
#include "hyperds/Options.h"

using namespace std;
using namespace kvdb;

#define FILENAME  "/dev/loop2"

class test_block_manager: public ::testing::Test{

};

SuperBlockManager* sbMgr_;
BlockDevice* bdev_;
Options opts;

void init()
{
	bdev_ = BlockDevice::CreateDevice();
    sbMgr_ = new SuperBlockManager(bdev_,opts);
    bdev_->Open(FILENAME);
}


TEST_F(test_block_manager, InitSuperBlockForCreateDB)
{
	init();
	uint64_t offset=0;
	EXPECT_TRUE(sbMgr_->InitSuperBlockForCreateDB(offset));
	EXPECT_TRUE(sbMgr_->WriteSuperBlockToDevice());
	EXPECT_TRUE(sbMgr_->LoadSuperBlockFromDevice(offset));

	 
}


int main(int argc, char **argv) {
	::testing::InitGoogleTest(&argc, argv);
	return RUN_ALL_TESTS();
}

