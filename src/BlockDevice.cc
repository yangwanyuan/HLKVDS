#include "BlockDevice.h"
#include "KernelDevice.h"
#include "PMEMDevice.h"
#include "libpmem.h"

namespace hlkvds {
BlockDevice* BlockDevice::CreateDevice(std::string path) {
    std::string type = "kernel";

    if (type == "kernel") {
        int is_pmem = 0;
        void *addr = pmem_map_file(path.c_str(), 1024*1024, PMEM_FILE_EXCL, O_RDONLY, NULL, &is_pmem);
        if (addr != NULL) {
            if (is_pmem) {
                type = "pmem";
            }
            pmem_unmap(addr, 1024*1024);
        }
    }

    if (type == "pmem") {
        return new PMEMDevice();
    }
    else if (type == "kernel") {
        return new KernelDevice();
    }

    return NULL;
}
}//namespace hlkvds
