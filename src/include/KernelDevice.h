#ifndef _HLKVDS_KERNELDEVICE_H_
#define _HLKVDS_KERNELDEVICE_H_

#include <string>
#include <unistd.h>
#include <fcntl.h>

#include "BlockDevice.h"

#define KD_OK 0
#define KD_ERR (-1)

namespace hlkvds {

class KernelDevice : public BlockDevice {
public:
    KernelDevice();
    virtual ~KernelDevice();

    int ZeroDevice();
    int Open(std::string path, bool dsync);
    void Close();
    void ClearReadCache();

    uint64_t GetDeviceCapacity() {
        return capacity_;
    }
    int GetPageSize() {
        return getpagesize();
    }
    int GetBlockSize() {
        return blockSize_;
    }

    std::string GetDevicePath() { return path_; };

    ssize_t pWrite(const void* buf, size_t count, off_t offset);
    ssize_t pRead(void* buf, size_t count, off_t offset);
    ssize_t pWritev(const struct iovec *iov, int iovcnt, off_t offset);
    ssize_t pReadv(const struct iovec *iov, int iovcnt, off_t offset);

private:
    int directFd_;
    int bufFd_;
    uint64_t capacity_;
    int blockSize_;
    std::string path_;
    bool isOpen_;

    int set_device_zero();
    int fill_file_with_zeros();
    uint64_t get_block_device_capacity();
    int lock_device();

    ssize_t DirectWriteAligned(const void* buf, size_t count, off_t offset);

    bool IsSectorAligned(const size_t off) {
        return off % ( GetBlockSize() ) == 0;
    }

    bool IsPageAligned(const void* ptr) {
        return (uint64_t)ptr % ( GetPageSize() ) == 0;
    }

};

}//namespace hlkvds

#endif // #ifndef _HLKVDS_KERNELDEVICE_H_
