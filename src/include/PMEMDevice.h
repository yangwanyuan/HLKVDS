#ifndef _HLKVDS__PMEMDEVICE_H_
#define _HLKVDS__PMEMDEVICE_H_

#include "BlockDevice.h"
#include <string>

#define PMD_OK 0;
#define PMD_ERR (-1);

namespace hlkvds {

class PMEMDevice : public BlockDevice {
  int fd;
  char *addr; //the address of mmap
  std::string path;

  uint64_t size;
  uint64_t block_size;

public:

  PMEMDevice();
  virtual ~PMEMDevice();

  int ZeroDevice();
  int Open(std::string path, bool dsync = 1);
  void Close();
  void ClearReadCache();

  uint64_t GetDeviceCapacity() {
    return size;
  }

  int GetPageSize() {
    return getpagesize();
  }

  int GetBlockSize() {
    return block_size;
  }

  std::string GetDevicePath() { return path; };

  ssize_t pWrite(const void* buf, size_t count, off_t offset);
  ssize_t pRead(void* buf, size_t count, off_t offset);
  ssize_t pWritev(const struct iovec *iov, int iovcnt, off_t offset);
  ssize_t pReadv(const struct iovec *iov, int iovcnt, off_t offset);


private:
  bool is_valid_io(uint64_t off, uint64_t len) const {
    return (len > 0 &&
            off < size &&
            off + len <= size);
  }
  int _lock();
};

} // namespace hlkvds
#endif
