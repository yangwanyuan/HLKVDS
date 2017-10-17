#ifndef _HLKVDS_METASTOR_H_
#define _HLKVDS_METASTOR_H_

#include <string>

using namespace std;
namespace hlkvds {

class BlockDevice;
class Options;
class SuperBlockManager;
class IndexManager;
class SimpleDS_Impl;

class MetaStor {
public:
    bool CreateMetaData();
    bool LoadMetaData();
    bool PersistMetaData();
    bool PersistIndexToDevice();
    bool PersistSSTsToDevice();
    bool FastRecovery();

    MetaStor(const char* paths, Options &opt, BlockDevice *dev, SuperBlockManager *sbm, IndexManager *im, SimpleDS_Impl *ds); 
    ~MetaStor();

private:
    const char* paths_;
    Options &options_;
    BlockDevice *metaDev_;
    uint64_t startOffset_;
    uint64_t endOffset_;
    SuperBlockManager *sbMgr_;
    IndexManager* idxMgr_;
    SimpleDS_Impl* dataStor_;
};
    
}// namespace hlkvds

#endif //#ifndef _HLKVDS_METASTOR_H_
