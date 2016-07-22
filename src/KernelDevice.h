#ifndef KV_DB_KERNELDEVICE_H_
#define KV_DB_KERNELDEVICE_H_

#include <string>
#include <unistd.h>

#include "BlockDevice.h"

namespace kvdb{

    enum device_type{REG_FILE, BLOCK_FILE};
    
    class KernelDevice : public BlockDevice{
    public:
        //KernelDevice(const std::string& path);
        KernelDevice();
        virtual ~KernelDevice();

        int CreateNewDB(string path, off_t size);
        int Open(string path);
        void Close();

        ssize_t pWrite(const void* buf, size_t count, off_t offset, bool writebuf);
        ssize_t pRead(void* buf, size_t count, off_t offset, bool readbuf);
        ssize_t pWritev(const struct iovec *iov, int iovcnt, off_t offset);
        ssize_t pReadv(const struct iovec *iov, int iovcnt, off_t offset);

    private:
        int m_direct_fd;
        int m_buffer_fd;
        uint64_t m_capacity;
        int m_blocksize;
        std::string m_path;
        device_type m_dtype;
        
        int set_device_zero();
        int fill_file_with_zeros();
        uint64_t get_block_device_capacity();
        int disable_readahead();


        uint64_t get_capacity() { return m_capacity; }
        int get_pagesize() {return getpagesize();}
        int get_blocksize() {return m_blocksize;}

        bool IsSectorAligned(const size_t off){return off % (get_blocksize()) == 0;}
        bool IsPageAligned(const void* ptr){return (uint64_t)ptr % (get_pagesize()) == 0;}

        ssize_t BufferRead(void* buf, size_t count, off_t offset);
        ssize_t BufferWrite(const void* buf, size_t count, off_t offset);
        ssize_t DirectRead(void* buf, size_t count, off_t offset);
        ssize_t DirectWrite(const void* buf, size_t count, off_t offset);

        ssize_t DirectReadAligned(void* buf, size_t count, off_t offset);
        ssize_t DirectWriteAligned(const void* buf, size_t count, off_t offset);
        ssize_t DirectReadUnaligned(void* buf, size_t count, off_t offset);
        ssize_t DirectWriteUnaligned(const void* buf, size_t count, off_t offset);


        off_t LowerAligned(off_t offset, int pagesize);
        off_t UpperAligned(off_t offset, int pagesize);
    };

}//namespace kvdb

#endif // #ifndef KV_DB_KERNELDEVICE_H_
