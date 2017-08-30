#include "hyperds/Kvdb.h"
#include "Kvdb_Impl.h"

namespace kvdb {

DB* DB::instance_ = NULL;

bool DB::CreateDB(string filename, Options opts) {
    KvdbDS* kvdb_db;
    kvdb_db = KvdbDS::Create_KvdbDS(filename.c_str(), opts);
    if (!kvdb_db) {
        std::cout << "CreateDB failed" << std::endl;
        return false;
    }

    delete kvdb_db;
    return true;
}

bool DB::OpenDB(string filename, DB** db, Options opts) {
    if (instance_ == NULL) {
        instance_ = new DB();
    }

    instance_->kvdb_ = KvdbDS::Open_KvdbDS(filename.c_str(), opts);
    if (!instance_->kvdb_) {
        std::cout << "OpenDB failed" << std::endl;
        return false;
    }

    *db = instance_;
    //std::cout << "OpenDB success" <<std::endl;
    return true;
}

DB::DB() {
}

DB::~DB() {
    if (instance_) {
        delete instance_->kvdb_;
        instance_->kvdb_ = NULL;
    }
    instance_ = NULL;
}

Status DB::Insert(const char* key, uint32_t key_len, const char* data,
                uint16_t length) {
    Status s = kvdb_->Insert(key, key_len, data, length);
    if (!s.ok()) {
        std::cout << "DB Insert failed" << std::endl;
    }

    return s;
}

Status DB::Delete(const char* key, uint32_t key_len) {
    Status s = kvdb_->Delete(key, key_len);
    if (!s.ok()) {
        std::cout << "DB Delete failed" << std::endl;
    }
    return s;
}

Status DB::Get(const char* key, uint32_t key_len, string &data) {
    Status s = kvdb_->Get(key, key_len, data);
    if (!s.ok()) {
        std::cout << "DB Get failed" << std::endl;
    }
    return s;
}

void DB::Do_GC() {
    kvdb_->Do_GC();
}

Status DB::InsertBatch(WriteBatch *batch) {
    return kvdb_->InsertBatch(batch);
}

Iterator* DB::NewIterator() {
    return kvdb_->NewIterator();
}

void DB::printDbStates()
{
    return kvdb_->printDbStates();
}

}// end namespace kvdb
