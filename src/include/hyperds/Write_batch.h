#ifndef _KV_DB_WRITE_BATCH_H_
#define _KV_DB_WRITE_BATCH_H_

#include <list>
#include <string>
#include "Db_Structure.h"


namespace kvdb {

class KvdbDS;
class KVSlice;

class WriteBatch {
public:
    WriteBatch();
    ~WriteBatch();

    void put(const char *key, uint32_t key_len, const char* data,
            uint16_t length);
    void del(const char *key, uint32_t key_len);
    void clear();

private:
    std::list<KVSlice *> batch_;
    friend class KvdbDS;
};

} // namespace kvdb

#endif //#define _KV_DB_WRITE_BATCH_H_
