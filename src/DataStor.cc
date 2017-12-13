#include "DataStor.h"
#include "DS_MultiVolume_Impl.h"
#include "DS_MultiTier_Impl.h"

namespace hlkvds {

DataStor* DataStor::Create(Options &opts, std::vector<BlockDevice*> &dev_vec, SuperBlockManager* sb, IndexManager* idx, int datastor_type) {

    switch (datastor_type) {
        case 0:
            return new DS_MultiVolume_Impl(opts, dev_vec, sb, idx);
        case 1:
            return new DS_MultiTier_Impl(opts, dev_vec, sb, idx);
        default:
            __ERROR("UnKnow DataStor Type!");
            return NULL;
    }
}

} // namespace hlkvds
