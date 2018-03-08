#ifndef _HLKVDS_BLOCKDEVICE_H_
#define _HLKVDS_BLOCKDEVICE_H_

#include <sys/types.h>
#include <stdint.h>
#include <sys/uio.h>
#include <string>

namespace hlkvds {
class BlockDevice {
public:
    static BlockDevice* CreateDevice(std::string path);

    BlockDevice() {
    }
    virtual ~BlockDevice() {
    }

    virtual int ZeroDevice() = 0;
    virtual int Open(std::string path, bool dsync = true) = 0;
    virtual void Close() = 0;

    virtual std::string GetDevicePath() = 0;
    virtual uint64_t GetDeviceCapacity() = 0;
    virtual int GetPageSize() = 0;
    virtual int GetBlockSize() = 0;

    virtual ssize_t pWrite(const void* buf, size_t count, off_t offset) = 0;
    virtual ssize_t pRead(void* buf, size_t count, off_t offset) = 0;
    virtual ssize_t pWritev(const struct iovec *iov, int iovcnt, off_t offset) = 0;
    virtual ssize_t pReadv(const struct iovec *iov, int iovcnt, off_t offset) = 0;

    virtual void ClearReadCache() = 0;
};
}//namespace hlkvds

#endif // #ifndef _HLKVDS_BLOCKDEVICE_H_
