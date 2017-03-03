
#pragma once
#ifndef TEST_BASE_H_
#define TEST_BASE_H_

#include "Kvdb_Impl.h"
#include "hyperds/Options.h"
#include "gtest/gtest.h"
#include <string>

using namespace std;
using namespace kvdb;

#define FILENAME  "/dev/loop2"

class TestBase :public ::testing::Test{
public:
	KvdbDS* Create_DB(int db_size);
protected:
	Options opts;

};

#endif  //TEST_BASE_H_
