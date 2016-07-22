#include "BlockDevice.h"
#include "KernelDevice.h"

namespace kvdb{
    BlockDevice* BlockDevice::CreateDevice()
    {
        return new KernelDevice();
    }
}//namespace kvdb
