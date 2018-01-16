#ifndef _HLKVDS_MIGRATE_H_
#define _HLKVDS_MIGRATE_H_

#include <sys/types.h>
#include <mutex>
#include <list>
#include <map>

#include "hlkvds/Options.h"

namespace hlkvds {

class IndexManager;
class Volume;
class FastTier;
class MediumTier;
class KVSlice;

class Migrate {
public:
    ~Migrate();
    Migrate(IndexManager* im, FastTier* ft, MediumTier* mt, Options &opt);

    void DoMigrate(uint32_t mt_vol_id);

private:
    void loadSegKV(std::list<KVSlice*> &slice_list, uint32_t num_keys,
                   uint64_t phy_offset);

    bool loadKvList(uint32_t seg_id, std::list<KVSlice*> &slice_list);
    void cleanKvList(std::list<KVSlice*> &slice_list);

private:
    FastTier *ft_;
    MediumTier* mt_;
    IndexManager* idxMgr_;
    Options &options_;

    char *ftDataBuf_;
    uint32_t ftSegSize_;
};

}//namespace hlkvds

#endif //#ifndef _HLKVDS_MIGRATE_H_
