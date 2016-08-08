#include <string>
#include <string.h>
#include <iostream>
#include <sstream>
#include <sys/time.h>
#include <time.h>
#include <stdio.h>
#include <stdlib.h>
#include <vector>
#include <stdlib.h>
#include "Kvdb_Impl.h"

#define KEY_LEN 10
#define DATA_HEADER_LEN 9
//#define TEST_BS 4096-KEY_LEN-DATA_HEADER_LEN
#define TEST_BS 4066
//#define TEST_THREAD_NUM 10

using namespace std;
using namespace kvdb;


double timeval_diff(const struct timeval * const start, const struct timeval * const end)
{
    /* Calculate the second difference*/
    double r = end->tv_sec - start->tv_sec;

    /* Calculate the microsecond difference */
    if (end->tv_usec > start->tv_usec)
        r += (end->tv_usec - start->tv_usec)/1000000.0;
    else if (end->tv_usec < start->tv_usec)
        r -= (start->tv_usec - end->tv_usec)/1000000.0;

    return r;
}

void usage()
{
    fprintf(stderr, "Usage: Benchmark -f dbfile -s db_size -n num_records\n");
}

int Create_DB(string filename, int db_size)
{
    //int ht_size = db_size * 2;
    int ht_size = db_size ;
    //double delete_ratio = 0.9;
    //double load_ratio = 0.8;
    int segment_size = 256*1024;

    struct timeval tv_start, tv_end;
    gettimeofday(&tv_start, NULL);

    //KvdbDS *db = KvdbDS::Create_KvdbDS(filename.c_str(), ht_size, delete_ratio, load_ratio, segment_size);
    KvdbDS *db = KvdbDS::Create_KvdbDS(filename.c_str(), ht_size, segment_size);

    if (db->WriteMetaDataToDevice() < 0){
        cout << "Create DB failed !" << endl;
        return -1;
    }
    gettimeofday(&tv_end, NULL);
    double diff_time = timeval_diff(&tv_start, &tv_end);
    cout << "Create DB use time: " << diff_time << "s" << endl;
    delete db;
    db = NULL;
    return 0;
}

void Create_Keys(int record_num, vector<string> &key_list)
{
    for(int index = 0; index < record_num; index++)
    {
        char c_key[KEY_LEN+1] = "kkkkkkkkkk";
        //string key_last = string('k', 10);
        //char c_key[10] = {'k','k','k','k','k','k','k','k','k','k'};
        stringstream key_ss;
        key_ss << index;
        string key(key_ss.str());
        memcpy(c_key, key.c_str(), key.length());
        string key_last = c_key;
        key_list.push_back(key_last);
        //cout << "key_len:" << key_last.length() << endl;
    }
}

void Bench_Insert(KvdbDS *db, int record_num, vector<string> &key_list)
{

    cout << "Insert Bench Start, record_num = " << record_num << ", Please wait ..." << endl;

    string value = string(TEST_BS, 'v');
    int value_size = value.length();
    int key_len = KEY_LEN;
    
    struct timeval tv_start, tv_end;
    gettimeofday(&tv_start, NULL);

    for(vector<string>::iterator iter = key_list.begin(); iter != key_list.end(); iter++)
    {
        string key = *iter;
        if (!db->Insert(key.c_str(), key_len, value.c_str(), value_size))
            cout << "Insert key=" << key << "to DB failed!" << endl;
    }

    gettimeofday(&tv_end, NULL);
    double insert_time = timeval_diff(&tv_start, &tv_end);
    cout << "Insert Bench Finish. use time: " << insert_time << "s" <<endl;
    cout << "Insert Bench Result: " << record_num / insert_time << " per second." <<endl;

    return;
}

void Bench_Get_Seq(KvdbDS *db, int record_num, vector<string> &key_list)
{
    cout << "Get Sequential Bench Start, record_num = " << record_num << ", Please wait ..." << endl;

    string value = string(TEST_BS, 'v');
    int key_len = KEY_LEN;

    struct timeval tv_start, tv_end;
    gettimeofday(&tv_start, NULL);

    for (vector<string>::iterator iter = key_list.begin(); iter != key_list.end(); iter++)
    {
        string key = *iter;
        string get_data;
        if(!db->Get(key.c_str(), key_len, get_data))
            cout << "Get key=" << key << " from DB failed" << endl;
        if (strcmp(get_data.c_str(), value.c_str()) != 0)
        {
            cout << "Get key=" << key <<"Inconsistent! "<< endl;
            cout << "Value size = " << get_data.length() << endl;
            cout << "value = " << get_data << endl;
        }
    }
    //for (int index = 0; index < record_num; index++){
    //    stringstream key_ss;
    //    key_ss << index;
    //    std::string key(key_ss.str());
    //    key_len = key.length();
    //    std::string get_data;
    //    if(!db->Get(key.c_str(), key_len, get_data))
    //        std::cout << "Get key=" << key << " from DB failed" << std::endl;
    //    if (strcmp(get_data.c_str(), value.c_str()) != 0)
    //        std::cout << "Get key=" << key << "failed , the value = " << get_data << std::endl;
    //}

    gettimeofday(&tv_end, NULL);
    double get_time = timeval_diff(&tv_start, &tv_end);

    cout << "Get Sequential Bench Finish. use time: " << get_time << "s" << endl;
    cout << "Get Sequential Bench Result: " << record_num / get_time << " per second." <<endl;

    return;
}

int Parse_Option(int argc, char** argv,string &filename, int &db_size, int &record_num){

    //if (argc != 7 || strcmp(argv[1],'-f') == 0 || strcmp(argv[3],'-s') == 0 || strcmp(argv[5],'-n') == 0)  
    if (argc != 7)  
        return -1;

    string str_f = "-f";
    string str_s = "-s";
    string str_n = "-n";
    if (strcmp(argv[1], str_f.c_str()) !=0 || strcmp(argv[3], str_s.c_str()) != 0 || strcmp(argv[5], str_n.c_str()) != 0)
        return -1;

    filename = argv[2];
    db_size = atoi(argv[4]);
    record_num = atoi(argv[6]);
    //std::cout << "filename: " << filename << " db_size: " << db_size << " record_num: " << record_num <<std::endl;
    
    if (db_size < 0 ||  record_num < 0 )
        return -1;
    return 0;
}


void Bench(string file_path, int db_size, int record_num)
{

    vector<string> key_list;
    Create_Keys(record_num, key_list);
    if (Create_DB(file_path, db_size) < 0)
        return;

    KvdbDS *db = KvdbDS::Open_KvdbDS(file_path.c_str());

    Bench_Insert(db, record_num, key_list);
    Bench_Get_Seq(db, record_num, key_list);

    delete db;
}

int main(int argc, char** argv){
    string file_path;
    int db_size; 
    int record_num;

    if(Parse_Option(argc, argv, file_path, db_size, record_num) < 0){
        usage();
        return -1;
    }

    Bench(file_path, db_size, record_num);

    return 0;
}
