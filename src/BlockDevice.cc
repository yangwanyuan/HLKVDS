#include "BlockDevice.h"
#include "KernelDevice.h"

namespace hlkvds {
BlockDevice* BlockDevice::CreateDevice() {
    return new KernelDevice();
}
}//namespace hlkvds
