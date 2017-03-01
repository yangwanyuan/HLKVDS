/*
 * test_base.h
 *
 *  Created on: Feb 20, 2017
 *      Author: yongfu
 */
#pragma once
#ifndef TEST_BASE_H_
#define TEST_BASE_H_


#endif /* TEST_BASE_H_ */

#include "Kvdb_Impl.h"
#include "hyperds/Options.h"
#include "gtest/gtest.h"
#include "status.h"
#include <string>

using namespace std;
using namespace kvdb;

#define FILENAME  "/dev/loop2"
#define DB_SIZE 512
#define RECORDS 500

/*
::testing::AssertionResult AssertStatus(const char* s_expr, const Status& s);

#define ASSERT_OK(s) ASSERT_PRED_FORMAT1(AssertStatus, s)
#define ASSERT_NOK(s) ASSERT_FALSE((s).ok())
#define EXPECT_OK(s) EXPECT_PRED_FORMAT1(AssertStatus, s)
#define EXPECT_NOK(s) EXPECT_FALSE((s).ok())
*/

class TestBase :public ::testing::Test{
public:
	KvdbDS* Create_DB(int db_size);
protected:
	Options opts;

};
