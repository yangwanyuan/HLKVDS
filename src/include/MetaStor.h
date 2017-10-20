#ifndef _HLKVDS_METASTOR_H_
#define _HLKVDS_METASTOR_H_

#include <string>

#include "hlkvds/Options.h"

using namespace std;
namespace hlkvds {

class BlockDevice;
class SuperBlockManager;
class IndexManager;
class SimpleDS_Impl;

class MetaStor {
public:
    bool CreateMetaData();
    bool LoadMetaData();
    bool PersistMetaData();

    bool LoadSuperBlockFromDevice(int first_create = 0);
    bool LoadIndexFromDevice();
    bool LoadSSTsFromDevice();

    bool PersistSuperBlockToDevice();
    bool PersistIndexToDevice();
    bool PersistSSTsToDevice();
    bool FastRecovery();

    MetaStor(const char* paths, Options &opt, BlockDevice *dev, SuperBlockManager *sbm, IndexManager *im, SimpleDS_Impl *ds); 
    ~MetaStor();

private:
    const char* paths_;
    Options &options_;
    BlockDevice *metaDev_;
    SuperBlockManager *sbMgr_;
    IndexManager* idxMgr_;
    SimpleDS_Impl* dataStor_;

    int64_t sbOff_;
    int64_t idxOff_;
    int64_t sstOff_;
    //uint64_t endOffset_;
};
    
}// namespace hlkvds

#endif //#ifndef _HLKVDS_METASTOR_H_
