#ifndef _HLKVDS_KVDB_IMPL_H_
#define _HLKVDS_KVDB_IMPL_H_

#include <vector>
#include <string>

#include "hlkvds/Options.h"
#include "hlkvds/Status.h"
#include "hlkvds/Write_batch.h"
#include "hlkvds/Iterator.h"

#include "ReadCache.h"

namespace hlkvds {

const std::string FileDelim = ",";

class BlockDevice;
class SuperBlockManager;
class IndexManager;
class MetaStor;
class DataStor;

class KVDS {
public:
    static KVDS* Create_KVDS(const char* filename, Options opts);
    static KVDS* Open_KVDS(const char* filename, Options opts);

    Status Insert(const char* key, uint32_t key_len, const char* data,
                  uint16_t length, bool immediately = false);
    Status Get(const char* key, uint32_t key_len, std::string &data);
    Status Delete(const char* key, uint32_t key_len);

    Status InsertBatch(WriteBatch *batch);

    Iterator* NewIterator();

    void Do_GC();
    void ClearReadCache();
    void printDbStates();

    virtual ~KVDS();

private:
    KVDS(const char* filename, Options opts);
    Status openDB();
    Status closeDB();
    void startThds();
    void stopThds();

    bool openAllDevices(std::string paths);
    void closeAllDevices();

private:
    std::string paths_;

    SuperBlockManager* sbMgr_;
    IndexManager* idxMgr_;

    dslab::ReadCache* rdCache_;// readcache, rmd160, slru/lru

    MetaStor *metaStor_;
    DataStor *dataStor_;

    Options options_;

    std::vector<BlockDevice *> bdVec_;

    bool isOpen_;

};

} // namespace hlkvds

#endif  // #ifndef _HLKVDS_KVDB_IMPL_H_
