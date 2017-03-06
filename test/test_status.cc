/*
 * test_status.cc
 *
 *  Created on: Mar 6, 2017
 *      Author: yongfu
 */

#include "test_base.h"

class test_status: public TestBase{

};

TEST_F(test_status,invalidargs)
{
	int db_size=100;
	KvdbDS *db= Create_DB(db_size);

	string test_key = "";
	int test_key_size = 8;
	string test_value = "test-value";
	int test_value_size = 10;

	Status s=db->Insert(NULL, test_key_size, test_value.c_str(), test_value_size);
	std::cout<<s.ToString()<<std::endl;

}


int main(int argc, char** argv){
	::testing::InitGoogleTest(&argc, argv);
	return RUN_ALL_TESTS();
}
