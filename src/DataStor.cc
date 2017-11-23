#include "DataStor.h"
#include "MultiVolumeDS.h"

namespace hlkvds {

DataStor* DataStor::Create(Options &opts, std::vector<BlockDevice*> &dev_vec, SuperBlockManager* sb, IndexManager* idx, int datastor_type) {

    switch (datastor_type) {
        case 0:
            return new MultiVolumeDS(opts, dev_vec, sb, idx);
        case 1:
            __ERROR("Multi_Tier has not been implement yet!");
            return NULL;
        default:
            __ERROR("UnKnow DataStor Type!");
            return NULL;
    }
}

} // namespace hlkvds
