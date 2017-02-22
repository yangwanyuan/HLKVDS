#include <string>
#include <iostream>
#include "test_base.h"

class test_operations: public TestBase{

};


TEST_F(test_operations,insert)
{
	int db_size=100;
	KvdbDS *db= initDb(db_size);
 
    string test_key = "test-key";
    int test_key_size = 8;
    string test_value = "test-value";
    int test_value_size = 10;

    EXPECT_TRUE(db->Insert(test_key.c_str(), test_key_size, test_value.c_str(), test_value_size));
	string get_data;
    EXPECT_TRUE(db->Get(test_key.c_str(), test_key_size, get_data));

   	EXPECT_EQ(test_value,get_data);

	delete db;
}

TEST_F(test_operations,emtpyvalue)
{
	int db_size=100;
	KvdbDS *db= initDb(db_size);

    string test_key = "";
    int test_key_size = 8;
    string test_value = "";
    int test_value_size = 10;

    EXPECT_TRUE(db->Insert(test_key.c_str(), test_key_size, test_value.c_str(), test_value_size));
	string get_data;
    EXPECT_TRUE(db->Get(test_key.c_str(), test_key_size, get_data));

   	EXPECT_EQ(test_value_size,get_data.length());

	delete db;
}

TEST_F(test_operations,zerosize)
{
	int db_size=100;
	KvdbDS *db= initDb(db_size);

    string test_key = "";
    int test_key_size = 0;
    string test_value = "";
    int test_value_size = 0;

    EXPECT_TRUE(db->Insert(test_key.c_str(), test_key_size, test_value.c_str(), test_value_size));
	string get_data;
    EXPECT_TRUE(db->Get(test_key.c_str(), test_key_size, get_data));

   	EXPECT_EQ(test_value,get_data);

	delete db;
}


TEST_F(test_operations,bigsizevalue)
{
	int db_size=100;
	KvdbDS *db= initDb(db_size);

    string test_key = "test_key";
    int test_key_size = 8;

	string test_value = string(1024000, 'v');

    int test_value_size =1024000;
	
    EXPECT_TRUE(db->Insert(test_key.c_str(), test_key_size, test_value.c_str(), test_value_size));//TRUE or FALSE ?

	//db->readMetaDataFromDevice();//TODO databases status dump
	string get_data;
    EXPECT_TRUE(db->Get(test_key.c_str(), test_key_size, get_data));

    std::cout<<"length:"<<get_data.length()<<std::endl;
   	//EXPECT_EQ(test_value,get_data);

	delete db;
}

TEST_F(test_operations,wrongvaluesize)
{
	int db_size=100;
	KvdbDS *db= initDb(db_size);

    string test_key = "test_key";
    int test_key_size = 8;

	string test_value = string(1024000, 'v');

    int test_value_size =1024;
	

    EXPECT_TRUE(db->Insert(test_key.c_str(), test_key_size, test_value.c_str(), test_value_size));

	string get_data;
    EXPECT_TRUE(db->Get(test_key.c_str(), test_key_size, get_data));

   	EXPECT_EQ(test_value_size,get_data.length());

	delete db;
}

TEST_F(test_operations,insertmorethandbsize)
{
	/*int db_size=100;//128 actually the db size should be equal to 128
	KvdbDS *db= initDb(db_size);

    string test_key = "key_";
    int test_key_size = 8;

	string test_value = "test_value";
    int test_value_size =1024;
	
	for(int i=0;i<127;i++)
	{	
		stringstream ss;
    	string str;
    	ss<<i;
    	ss>>str;
		test_key=test_key+str;
		EXPECT_TRUE(db->Insert(test_key.c_str(), test_key_size, test_value.c_str(), test_value_size));
	}
		
    EXPECT_FALSE(db->Insert(test_key.c_str(), test_key_size, test_value.c_str(), test_value_size));//TODO

	delete db;*/
}



TEST_F(test_operations,updatevalue)
{
	int db_size=100;
	KvdbDS *db= initDb(db_size);

    string test_key = "test_key";
    int test_key_size = 8;

	string test_value = "test_value";
    int test_value_size =10;
	

    EXPECT_TRUE(db->Insert(test_key.c_str(), test_key_size, test_value.c_str(), test_value_size));

	string get_data;
    EXPECT_TRUE(db->Get(test_key.c_str(), test_key_size, get_data));

   	EXPECT_EQ(test_value,get_data);


	test_value = "test-value-new";
    test_value_size = 14;

	get_data="";
    EXPECT_TRUE(db->Insert(test_key.c_str(), test_key_size, test_value.c_str(), test_value_size));
	EXPECT_TRUE(db->Get(test_key.c_str(), test_key_size, get_data));
	EXPECT_EQ(test_value,get_data);
    

	delete db;
}

TEST_F(test_operations,deletekey)
{
	int db_size=100;
	KvdbDS *db= initDb(db_size);

    string test_key = "test_key";
    int test_key_size = 8;
	string test_value = "test_value";
    int test_value_size =10;
	

    EXPECT_TRUE(db->Insert(test_key.c_str(), test_key_size, test_value.c_str(), test_value_size));

	string get_data;
    EXPECT_TRUE(db->Get(test_key.c_str(), test_key_size, get_data));
   	EXPECT_EQ(test_value,get_data);

	EXPECT_TRUE(db->Delete(test_key.c_str(), test_key_size));
	get_data="";
	EXPECT_TRUE(db->Get(test_key.c_str(), test_key_size, get_data));
	EXPECT_EQ("",get_data);
	//std::cout<<"deleted key value:"<<get_data<<std::endl;

	delete db;
}


TEST_F(test_operations,updateafterdelete)
{
	int db_size=100;
	KvdbDS *db= initDb(db_size);

    string test_key = "test_key";
    int test_key_size = 8;
	string test_value = "test_value";
    int test_value_size =10;
	

    EXPECT_TRUE(db->Insert(test_key.c_str(), test_key_size, test_value.c_str(), test_value_size));

	string get_data;
    EXPECT_TRUE(db->Get(test_key.c_str(), test_key_size, get_data));
   	EXPECT_EQ(test_value,get_data);

	EXPECT_TRUE(db->Delete(test_key.c_str(), test_key_size));
	get_data="";
	EXPECT_TRUE(db->Get(test_key.c_str(), test_key_size, get_data));

	EXPECT_TRUE(db->Insert(test_key.c_str(), test_key_size, test_value.c_str(), test_value_size));
	get_data="";
    EXPECT_TRUE(db->Get(test_key.c_str(), test_key_size, get_data));
   	EXPECT_EQ(test_value,get_data);
	
	delete db;
}

TEST_F(test_operations,deleteagain)
{
	int db_size=100;
	KvdbDS *db= initDb(db_size);

    string test_key = "test_key";
    int test_key_size = 8;
	string test_value = "test_value";
    int test_value_size =10;
	

    EXPECT_TRUE(db->Insert(test_key.c_str(), test_key_size, test_value.c_str(), test_value_size));

	string get_data;
    EXPECT_TRUE(db->Get(test_key.c_str(), test_key_size, get_data));
   	EXPECT_EQ(test_value,get_data);

	EXPECT_TRUE(db->Delete(test_key.c_str(), test_key_size));
	get_data="";
	EXPECT_TRUE(db->Get(test_key.c_str(), test_key_size, get_data));
	EXPECT_EQ("",get_data);

	std::cout<<"delete again."<<std::endl;
	EXPECT_TRUE(db->Delete(test_key.c_str(), test_key_size));

	delete db;
}

TEST_F(test_operations,readAfterUpdateAndDelete)
{
	int db_size=100;
	KvdbDS *db= initDb(db_size);

	string test_key = "test_key";
	int test_key_size = 8;

	string test_value = "test_value";
	int test_value_size =10;


	EXPECT_TRUE(db->Insert(test_key.c_str(), test_key_size, test_value.c_str(), test_value_size));

	string get_data;
	EXPECT_TRUE(db->Get(test_key.c_str(), test_key_size, get_data));
	EXPECT_EQ(test_value,get_data);

	//update
	test_value = "test-value-new";
	test_value_size = 14;

	get_data="";
	EXPECT_TRUE(db->Insert(test_key.c_str(), test_key_size, test_value.c_str(), test_value_size));
	EXPECT_TRUE(db->Get(test_key.c_str(), test_key_size, get_data));
	EXPECT_EQ(test_value,get_data);

	//then delete
	EXPECT_TRUE(db->Delete(test_key.c_str(), test_key_size));

	get_data="";
	//read again, value should be empty , or the raw one ?
	EXPECT_TRUE(db->Get(test_key.c_str(), test_key_size, get_data));
	EXPECT_EQ("",get_data);
}


TEST_F(test_operations,concurrentinsertwithsamekey)
{

}

TEST_F(test_operations,singlesegment)
{
	int db_size=100;
	KvdbDS *db= initDb(db_size);

	//std::cout<<"total used segments:"<<db->segMgr_->GetTotalUsedSegs()<<std::endl;;//TODO segMgr_ is private

	string test_key = "test_key";
	int test_key_size = 8;

	string test_value = "test_value";
	int test_value_size =10;

	EXPECT_TRUE(db->Insert(test_key.c_str(), test_key_size, test_value.c_str(), test_value_size));
	//std::cout<<"total used segments:"<<db->segMgr_->GetTotalUsedSegs()<<std::endl;;

}

TEST_F(test_operations,acrosssegment)
{

}



int main(int argc, char** argv){
	::testing::InitGoogleTest(&argc, argv);
	return RUN_ALL_TESTS();

}
