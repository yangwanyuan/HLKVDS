#ifndef _HLKVDS_METASTOR_H_
#define _HLKVDS_METASTOR_H_

#include <string>
#include <vector>

#include "hlkvds/Options.h"

namespace hlkvds {

class BlockDevice;
class SuperBlockManager;
class IndexManager;
class SimpleDS_Impl;

class MetaStor {
public:
    bool TryLoadSB(int &datastor_type);

    bool CreateMetaData();
    bool LoadMetaData();
    bool PersistMetaData();

    bool PersistIndexToDevice();
    bool PersistSSTsToDevice();
    bool FastRecovery();

    MetaStor(const char* paths, std::vector<BlockDevice*> &dev_vec, SuperBlockManager *sbm, IndexManager *im, Options &opt);
    ~MetaStor();

    void InitDataStor(SimpleDS_Impl* ds);

private:
    bool createSuperBlock();
    bool createIndex(uint32_t ht_size, uint64_t index_size);
    bool createDataStor(uint32_t segment_size);
    bool loadSuperBlock();
    bool loadIndex(uint32_t ht_size, uint64_t index_size);
    bool loadDataStor();

    bool persistSuperBlockToDevice();

    void verifyMetaDevice();

private:
    const char* paths_;
    std::vector<BlockDevice *> &bdVec_;
    BlockDevice *metaDev_;
    SuperBlockManager *sbMgr_;
    IndexManager* idxMgr_;
    SimpleDS_Impl* dataStor_;
    Options &options_;

    int64_t sbOff_;
    int64_t idxOff_;
    int64_t sstOff_;
};
    
}// namespace hlkvds

#endif //#ifndef _HLKVDS_METASTOR_H_
