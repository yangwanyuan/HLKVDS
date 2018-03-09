//#include <unistd.h>
//#include <stdlib.h>
//#include <sys/types.h>
//#include <sys/stat.h>
#include <stdio.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <errno.h>
#include <inttypes.h>
#include <sys/ioctl.h>
#include <linux/fs.h>
#include <string.h>
#include <stdlib.h>

#include "PMEMDevice.h"
#include "libpmem.h"
#include "Db_Structure.h"

using namespace std;
namespace hlkvds {

int PMEMDevice::_lock()
{
  struct flock l;
  memset(&l, 0, sizeof(l));
  l.l_type = F_WRLCK;
  l.l_whence = SEEK_SET;
  l.l_start = 0;
  l.l_len = 0;
  int r = ::fcntl(fd, F_SETLK, &l);
  if (r < 0)
    return PMD_ERR;
  return 0;
}

PMEMDevice::PMEMDevice() {
}

PMEMDevice::~PMEMDevice() {
}

int PMEMDevice::ZeroDevice() {
    return 0;
}

int PMEMDevice::Open(std::string p, bool dsync)
{
  path = p;
  int r = 0;
  __INFO("detect PEMDevice path = %s", path.c_str());

  fd = ::open(path.c_str(), O_RDWR);
  if (fd < 0) {
    r = PMD_ERR;
    __ERROR("Open %s failed !", path.c_str());
    return r;
  }

  r = _lock();
  if (r < 0) {
    __ERROR("Failed to lock %s", path.c_str());
    goto out_fail;
  }

  struct stat st;
  r = ::fstat(fd, &st);
  if (r < 0) {
    r = PMD_ERR;
    __ERROR("fstat got error ");
    goto out_fail;
  }

  size_t map_len;
  addr = (char *)pmem_map_file(path.c_str(), 0, PMEM_FILE_EXCL, O_RDWR, &map_len, NULL);
  if (addr == NULL) {
    __ERROR(" pmem_map_file failed ");
    goto out_fail;
  }
  size = map_len;

  block_size = st.st_blksize;

  __INFO("PEMDevice Open Success! path = %s", path.c_str());
  return 0;

 out_fail:
  ::close(fd);
  fd = -1;
  return r;
}

void PMEMDevice::Close()
{
  pmem_unmap(addr, size);
  ::close(fd);
  fd = -1;

  path.clear();
}

void PMEMDevice::ClearReadCache() {
}

ssize_t PMEMDevice::pWrite(const void* buf, size_t count, off_t offset) {
    if (!is_valid_io(offset, count)) {
        return PMD_ERR;
    }
    pmem_memcpy_persist(addr + offset, (const char *)buf, (uint32_t)count);
    return (ssize_t)count;
}

ssize_t PMEMDevice::pRead(void* buf, size_t count, off_t offset) {
    if (!is_valid_io(offset, count)) {
        return PMD_ERR;
    }
    memcpy(buf, addr + offset, count);
    return (ssize_t)count;
}

ssize_t PMEMDevice::pWritev(const struct iovec *iov, int iovcnt, off_t offset) {
    return PMD_ERR;
}

ssize_t PMEMDevice::pReadv(const struct iovec *iov, int iovcnt, off_t offset) {
    return PMD_ERR;
}

}

