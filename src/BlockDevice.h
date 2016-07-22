#ifndef KV_DB_BLOCKDEVICE_H_
#define KV_DB_BLOCKDEVICE_H_

#include <sys/types.h>
#include <stdint.h>
#include <sys/uio.h>
#include <string>
#include "Db_Structure.h"

using namespace std;

namespace kvdb{
    class BlockDevice{
    public:
        //static BlockDevice* CreateDevice(const std::string& path);
        static BlockDevice* CreateDevice();

        BlockDevice(){}
        virtual ~BlockDevice() {}

        virtual int CreateNewDB(string path, off_t size) = 0;
        virtual int Open(string path) = 0;
        virtual void Close() = 0;

        virtual ssize_t pWrite(const void* buf, size_t count, off_t offset, bool writebuf = true) = 0;
        virtual ssize_t pRead(void* buf, size_t count, off_t offset, bool readbuf = true) = 0; 
        //virtual ssize_t pWrite(const void* buf, size_t count, off_t offset, bool writebuf = false) = 0;
        //virtual ssize_t pRead(void* buf, size_t count, off_t offset, bool readbuf = false) = 0; 
        virtual ssize_t pWritev(const struct iovec *iov, int iovcnt, off_t offset) = 0;
        virtual ssize_t pReadv(const struct iovec *iov, int iovcnt, off_t offset) = 0;

    private:
        //std::string m_path;
    };
}//namespace kvdb

#endif // #ifndef _KV_DB_BLOCKDEVICE_H_
