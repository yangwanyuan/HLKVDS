/*
 * test_base.cc
 *
 *  Created on: Feb 20, 2017
 *      Author: yongfu
 */
#include "test_base.h"

using namespace std;
using namespace kvdb;

int TestBase::Create_DB(string filename, int db_size)
{
	int ht_size = db_size ;
	int segment_size = SEGMENT_SIZE;

	opts.hashtable_size = ht_size;
	opts.segment_size = segment_size;

	KvdbDS *db = KvdbDS::Create_KvdbDS(filename.c_str(), opts);

	delete db;
	db = NULL;
	return 0;
}

KvdbDS* TestBase::initDb(int db_size)
{
	EXPECT_TRUE(Create_DB(FILENAME, db_size) >= 0);

	return KvdbDS::Open_KvdbDS(FILENAME, opts);

}


