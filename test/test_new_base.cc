#include "test_new_base.h"
#include "Db_Structure.h"

using namespace hlkvds;
using namespace std;

TestNewBase::TestNewBase(std::string filename, int datastor_type) : path_(filename), opts_(Options()), db_(NULL) {
}

TestNewBase::~TestNewBase() {
    if (!db_) {
        delete db_;
    }
}

KVDS* TestNewBase::Create() {
    db_ = KVDS::Create_KVDS(path_.c_str(), opts_);
    EXPECT_TRUE(NULL != db_);
    return db_;
}

KVDS* TestNewBase::Open() {
    db_ = KVDS::Open_KVDS(path_.c_str(), opts_);
    EXPECT_TRUE(NULL != db_);
    return db_;
}

KVDS* TestNewBase::ReOpen() {
    delete db_;
    db_ = NULL;
    return Open();
}

Status TestNewBase::Insert(const char* key, uint32_t key_len, const char* data, uint16_t length) {
    return db_->Insert(key, key_len, data, length);
}

Status TestNewBase::Get(const char* key, uint32_t key_len, std::string &data) {
    return db_->Get(key, key_len, data);
}

Status TestNewBase::Delete(const char* key, uint32_t key_len) {
    return db_->Delete(key, key_len);
}

Status TestNewBase::InsertBatch(WriteBatch *batch) {
    return db_->InsertBatch(batch);
}

Iterator* TestNewBase::NewIterator() {
    return db_->NewIterator();
}
