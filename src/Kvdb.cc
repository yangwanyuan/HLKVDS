#include "Kvdb.h"
#include "Kvdb_Impl.h"

namespace kvdb{

    DB* DB::instance_ = NULL;

    bool DB::CreateDB(string filename, Options opts)
    {
        KvdbDS* kvdb_db;
        kvdb_db = KvdbDS::Create_KvdbDS(filename.c_str(), opts);
        if (!kvdb_db)
        {
            std::cout << "CreateDB failed" << std::endl;
            return false;
        }

        delete kvdb_db;
        return true;
    }

    bool DB::OpenDB(string filename, DB** db, Options opts)
    {
        if (instance_ == NULL)
        {
            instance_ = new DB();
        }

        instance_->kvdb_ = KvdbDS::Open_KvdbDS(filename.c_str(), opts);
        if (!instance_->kvdb_)
        {
            std::cout << "OpenDB failed" <<std::endl;
            return false;
        }

        *db = instance_;
        //std::cout << "OpenDB success" <<std::endl;
        return true;
    }

    DB::DB(){}

    DB::~DB()
    {
        delete instance_->kvdb_;
    }

    bool DB::Insert(const char* key, uint32_t key_len,const char* data, uint16_t length){
        if (!kvdb_->Insert(key, key_len, data, length))
        {
            std::cout << "DB Insert failed" <<std::endl;
            return false;
        }
        return true;
    }

    bool DB::Delete(const char* key, uint32_t key_len){
        if (!kvdb_->Delete(key, key_len))
        {
            std::cout << "DB Delete failed" <<std::endl;
            return false;
        }
        return true;
    }

    bool DB::Get(const char* key, uint32_t key_len, string &data){
        if (!kvdb_->Get(key, key_len, data))
        {
            std::cout << "DB Get failed" <<std::endl;
            return false;
        }
        return true;
    }

    void DB::Do_GC()
    {
        kvdb_->Do_GC();
    }

}// end namespace kvdb
