#pragma once
#ifndef TEST_BASE_H_
#define TEST_BASE_H_

#include "Kvdb_Impl.h"
#include "hlkvds/Options.h"
#include "hlkvds/Iterator.h"
#include "gtest/gtest.h"
#include <string>

#define FILENAME  "/dev/loop2"
using namespace hlkvds;

class TestBase : public ::testing::Test {
public:
    KVDS* Create_DB(int db_size);
protected:
    Options opts;

};

#endif  //TEST_BASE_H_
