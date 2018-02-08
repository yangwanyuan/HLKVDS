#include "hlkvds/Kvdb.h"
#include "Kvdb_Impl.h"

using namespace std;

namespace hlkvds {

DB* DB::db_ = NULL;
DB::DBReaper DB::reaper_ ;


bool DB::CreateDB(string filename, Options opts) {
    KVDS* new_kvds;
    new_kvds = KVDS::Create_KVDS(filename.c_str(), opts);
    if (!new_kvds) {
        std::cout << "CreateDB failed" << std::endl;
        return false;
    }

    delete new_kvds;
    return true;
}

bool DB::OpenDB(string filename, DB** db, Options opts) {
    std::unique_lock<std::mutex> l(db_->mtx_);
    if ( db_->kvds_ == NULL ) {
        db_->kvds_ = KVDS::Open_KVDS(filename.c_str(), opts);
        if (!db_->kvds_) {
            std::cout << "OpenDB failed" << std::endl;
            return false;
        }
    }

    *db = db_;
    return true;
}

DB::DB() : kvds_(NULL) {
}

DB::~DB() {
    if (kvds_) {
        delete kvds_;
        kvds_ = NULL;
    }
}

DB::DBReaper::DBReaper() {
    db_ = new DB();
}

DB::DBReaper::~DBReaper() {
    //delete db_;
    //db_ = NULL;
}

Status DB::Insert(const char* key, uint32_t key_len, const char* data,
                uint16_t length, bool immediately) {
    Status s = kvds_->Insert(key, key_len, data, length, immediately);
    if (!s.ok()) {
        std::cout << "DB Insert failed" << std::endl;
    }
    return s;
}

Status DB::Delete(const char* key, uint32_t key_len) {
    Status s = kvds_->Delete(key, key_len);
    if (!s.ok()) {
        std::cout << "DB Delete failed" << std::endl;
    }
    return s;
}

Status DB::Get(const char* key, uint32_t key_len, string &data) {
    Status s = kvds_->Get(key, key_len, data);
    if (!s.ok()) {
        std::cout << "DB Get failed" << std::endl;
    }
    return s;
}

void DB::Do_GC() {
    kvds_->Do_GC();
}

Status DB::InsertBatch(WriteBatch *batch) {
    return kvds_->InsertBatch(batch);
}

Iterator* DB::NewIterator() {
    return kvds_->NewIterator();
}

void DB::printDbStates()
{
    return kvds_->printDbStates();
}

}// end namespace hlkvds
