#include <string>
#include <iostream>
#include "test_base.h"

class IndexManagerTest : public TestBase{
public:
	IndexManager* idxMgr_;
	SuperBlockManager* sbMgr_;
	BlockDevice* bdev_;
	SegmentManager* segMgr_;

	Options opts;

	virtual void SetUp(){
		bdev_ = BlockDevice::CreateDevice();
		sbMgr_ = new SuperBlockManager(bdev_,opts);
		segMgr_ = new SegmentManager(bdev_, sbMgr_,opts);
		idxMgr_ = new IndexManager(bdev_, sbMgr_, segMgr_,opts);
		bdev_->Open(FILENAME);
	}
};

TEST_F(IndexManagerTest, InitIndexForCreateDB)
{
	uint64_t offset=0;
	uint32_t numObjects=10;
	uint32_t ht_size=20;

	EXPECT_TRUE(idxMgr_->InitIndexForCreateDB(offset,numObjects));
	EXPECT_TRUE(idxMgr_->WriteIndexToDevice());
	EXPECT_TRUE(idxMgr_->LoadIndexFromDevice(offset,ht_size));

	KVSlice* kvslice;
	 
	EXPECT_TRUE(idxMgr_->UpdateIndex(kvslice));
	 
}


TEST_F(IndexManagerTest, RemoveEntry)
{
	HashEntry entry;
	 
	idxMgr_->RemoveEntry(entry);//TODO Expection
	
	 
}
TEST_F(IndexManagerTest, GetHashEntry)
{
	KVSlice* kvslice;
	 
	EXPECT_TRUE(idxMgr_->GetHashEntry(kvslice));
	 
}

TEST_F(IndexManagerTest, ComputeIndexSizeOnDevice)
{
	uint32_t ht_size=10;
	EXPECT_EQ(10,IndexManager::ComputeIndexSizeOnDevice(ht_size));
	 
}

int main(int argc, char **argv) {
	::testing::InitGoogleTest(&argc, argv);
	return RUN_ALL_TESTS();
}

