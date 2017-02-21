/*
 * test_base.h
 *
 *  Created on: Feb 20, 2017
 *      Author: yongfu
 */

#ifndef TEST_BASE_H_
#define TEST_BASE_H_


#endif /* TEST_BASE_H_ */

#include "Kvdb_Impl.h"
#include "hyperds/Options.h"
#include "gtest/gtest.h"

using namespace std;
using namespace kvdb;

#define FILENAME  "/dev/loop2"
#define DB_SIZE 512
#define RECORDS 500

class TestBase :public ::testing::Test{
public:
	KvdbDS* initDb(int db_size);
private:
	int Create_DB(string filename, int db_size);
protected:
	Options opts;

};
