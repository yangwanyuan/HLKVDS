#ifndef KV_DB_KVDB_H_
#define KV_DB_KVDB_H_

#include <iostream>
#include <string>

#include "Kvdb_Impl.h"

using namespace std;
using namespace kvdb;

namespace kvdb{

class DB{
    public:
        //static bool CreateDB(string filename, uint64_t hash_table_size, double max_deleted_ratio, double max_load_factor, uint64_t segment_size);
        static bool CreateDB(string filename, uint32_t hash_table_size, uint32_t segment_size);
        static bool OpenDB(string filename, DB** db);
        //static DB* GetDB();

        virtual ~DB();

        bool Insert(const char* key, uint32_t key_len,const char* data, uint16_t length);
        bool Delete(const char* key, uint32_t key_len);
        bool Get(const char* key, uint32_t key_len, string &data);

    private:
        DB();
        DB(const DB &);
        DB& operator = (const DB &);
        bool writeMeta();

        string m_filename;
        KvdbDS *m_kvdb_db;
        static DB *m_db;

};

} // namespace kvdb

#endif //KV_DB_KVDB_H_
