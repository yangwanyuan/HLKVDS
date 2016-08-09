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

#include <vector>
#include "KernelDevice.h"
   
namespace kvdb{
    KernelDevice::KernelDevice():
        m_direct_fd(-1), m_buffer_fd(-1), m_capacity(0), m_blocksize(0), m_path("")
    {
        ;
    }

    KernelDevice::~KernelDevice()
    {
        if (m_direct_fd != -1 || m_buffer_fd != -1)
        {
            Close();
        }
    }

    int KernelDevice::CreateNewDB(string path, off_t size)
    {
        m_path = path;

        int r = 0;
        //check file exist, if not create a reg file.
        //if (access(m_path.c_str(), F_OK)){
        //    m_dtype = REG_FILE;

        //    r = open(m_path.c_str(), O_RDWR | O_CREAT | O_DIRECT, 0666);
        //    if (r < 0){
        //        fprintf(stderr, "Could not create file: %s\n", strerror(errno));
        //        goto create_fail;
        //    }
        //    m_direct_fd = r;

        //    r = open(m_path.c_str(), O_RDWR | O_CREAT, 0666);
        //    if (r < 0){
        //        fprintf(stderr, "Could not create file: %s\n", strerror(errno));
        //        goto create_fail;
        //    }
        //    m_buffer_fd = r;

        //}else{
        //

        r = open(m_path.c_str(), O_RDWR | O_DIRECT, 0666);
        if (r < 0)
        {
            fprintf(stderr, "Could not create file: %s\n", strerror(errno));
            goto create_fail;
        }
        m_direct_fd = r;

        //r = open(m_path.c_str(), O_RDWR, 0666);
        //r = open(m_path.c_str(), O_RDWR | O_SYNC, 0666);
        r = open(m_path.c_str(), O_RDWR | O_DSYNC, 0666);
        if (r < 0)
        {
            fprintf(stderr, "Could not create file: %s\n", strerror(errno));
            goto create_fail;
        }
        m_buffer_fd = r;

        //}
        struct stat statbuf;
        r = fstat(m_direct_fd, &statbuf);
        if (r < 0)
        {
            fprintf(stderr, "Could not stat file: %s\n", strerror(errno));
            goto create_fail;
        }
        
        if (S_ISBLK(statbuf.st_mode))
        {
            m_dtype = BLOCK_FILE;
            m_capacity = get_block_device_capacity();
            m_blocksize = statbuf.st_blksize;
        }
        else
        {
            m_dtype = REG_FILE;
            m_capacity = size;
        }

        r = posix_fadvise(m_buffer_fd, 0, 0, POSIX_FADV_RANDOM);
        if (r < 0)
        {
            perror("couldn't posix_fadvise random");
            goto create_fail;
        }

        r = set_metazone_zero(size);
        if (r<0)
        {
            perror("couldn't set metazone zero");
            goto create_fail;
        }

        r = set_device_zero();
        if (r < 0)
        {
            perror("couldn't set device zero");
            goto create_fail;
        }

        return OK;

create_fail:
        close(m_direct_fd);
        close(m_buffer_fd);
        m_direct_fd = -1;
        m_buffer_fd = -1;
        return ERR; 
    }

    int KernelDevice::set_device_zero()
    {
        int r = 0;
        if (m_dtype == REG_FILE)
        {
            r = ftruncate(m_buffer_fd, (off_t)m_capacity);
            if (r < 0)
            {
                return ERR;
            }
        }

        lseek(m_buffer_fd, 0, SEEK_SET);
        //r = fill_file_with_zeros();
        if (r < 0)
        {
            perror("Could not zero out DB file.\n");
            return ERR;
        }    

        return OK;
    }

    int KernelDevice::set_metazone_zero(uint64_t meta_size)
    {
        size_t nbytes = meta_size;

        static const size_t BLOCKSIZE = 8192;
        char zeros[BLOCKSIZE];
        memset(zeros, 0, BLOCKSIZE);
        while (nbytes > 0)
        {
            size_t bytes_to_write = min(nbytes, BLOCKSIZE);
            ssize_t ret = write(m_buffer_fd, zeros, bytes_to_write);
            if (ret < 0)
            {
                fprintf(stderr, "error in fill_file_with_zeros write: %s\n", strerror(errno));
                return ERR;
            }
            nbytes -= bytes_to_write;
        }
        return OK;
    }

    int KernelDevice::fill_file_with_zeros()
    {
        size_t nbytes = m_capacity;

        static const size_t BLOCKSIZE = 8192;
        char zeros[BLOCKSIZE];
        memset(zeros, 0, BLOCKSIZE);
        while (nbytes > 0)
        {
            size_t bytes_to_write = min(nbytes, BLOCKSIZE);
            ssize_t ret = write(m_buffer_fd, zeros, bytes_to_write);
            if (ret < 0)
            {
                fprintf(stderr, "error in fill_file_with_zeros write: %s\n", strerror(errno));
                return ERR;
            }
            nbytes -= bytes_to_write; 
        }
        return OK;
    }

    uint64_t KernelDevice::get_block_device_capacity()
    {   
        uint64_t blocksize ;
        if (ioctl(m_buffer_fd, BLKGETSIZE64, &blocksize) < 0)
        {
            fprintf(stderr, "Could not get block size: %s\n", strerror(errno));
            return ERR;
        }

        return blocksize;
    }

    int KernelDevice::Open(string path)
    {
        m_path = path;

        int r = 0;

        m_direct_fd =  open(m_path.c_str(), O_RDWR | O_DIRECT, 0666);
        if (m_direct_fd < 0)
        {
            fprintf(stderr, "Could not open file: %s\n", strerror(errno));
            goto open_fail;
        }
        //m_buffer_fd =  open(m_path.c_str(), O_RDWR, 0666);
        //m_buffer_fd =  open(m_path.c_str(), O_RDWR | O_SYNC, 0666);
        m_buffer_fd =  open(m_path.c_str(), O_RDWR | O_DSYNC, 0666);
        if (m_buffer_fd < 0)
        {
            fprintf(stderr, "Could not open file: %s\n", strerror(errno));
            goto open_fail;
        }

        r = posix_fadvise(m_buffer_fd, 0, 0, POSIX_FADV_RANDOM);
        if (r < 0)
        {
            fprintf(stderr, "Couldn't posix_fadvise random: %s\n", strerror(errno));
            goto open_fail;
        }

        struct stat statbuf;
        r = fstat(m_direct_fd, &statbuf);
        if (r < 0)
        {
            fprintf(stderr, "Couldn't read fstat: %s\n", strerror(errno));
            goto open_fail;
        }

        if (S_ISBLK(statbuf.st_mode))
        {
            m_dtype = BLOCK_FILE;
            m_capacity = get_block_device_capacity();
            m_blocksize = statbuf.st_blksize;
        }
        else
        {
            m_dtype = REG_FILE;
            m_capacity = statbuf.st_size; 
        }
        return OK;
open_fail:
        close(m_buffer_fd);
        close(m_direct_fd);
        m_direct_fd = -1;
        m_buffer_fd = -1;
        return ERR;

    }

    void KernelDevice::Close()
    {
        if (m_direct_fd != -1)
        {
            close(m_direct_fd);
        }
        if (m_buffer_fd != -1)
        {
            close(m_buffer_fd);
        }
    }


    off_t KernelDevice::LowerAligned(off_t offset, int pagesize)
    {
        if (offset % pagesize == 0)
        {
            return offset;
        }
        return offset - (offset % pagesize);
    }

    off_t KernelDevice::UpperAligned(off_t offset, int pagesize)
    {
        if (offset % pagesize == 0)
        {
            return offset;
        }
        return offset + (pagesize - offset % pagesize);
    }

    ssize_t KernelDevice::DirectWriteUnaligned(const void* buf, size_t count, off_t offset)
    {
        int ret;
        int pagesize = get_pagesize();
        int blocksize = get_blocksize();

        off_t offset_lower = LowerAligned(offset, pagesize);
        off_t upper = UpperAligned(offset + count, pagesize);

        int total_pages_count = (upper - offset_lower) / pagesize;

        //alloc buf_list
        std::vector<unsigned char*> buf_list;
        for (int index = 0; index < total_pages_count; index++)
        {
            unsigned char *page_buf;
            ret = posix_memalign((void **)&page_buf, blocksize, pagesize);
            if (ret)
            {
                fprintf(stderr, "posix_memalign failed: %s\n", strerror(errno));
                return ERR; 
            }
            buf_list.push_back(page_buf);
        }

        //read prefix data from device to buf_list
        int pre_data_length = offset - offset_lower;
        char* pre_data = (char*)malloc(pre_data_length);        
        char* pre_data_temp;
        ret = posix_memalign((void **)&pre_data_temp, blocksize, pagesize);
        if (ret)
        {
            fprintf(stderr, "posix_memalign failed: %s\n", strerror(errno));
            return ERR; 
        }
        //if(DirectReadUnaligned(pre_data, pre_data_length, offset_lower) != pre_data_length)
        if(DirectReadAligned(pre_data_temp, pagesize, offset_lower) != pagesize)
        {
            fprintf(stderr, "pread64 failed: %s\n", strerror(errno));
            return ERR; 
        }
        memcpy(pre_data, pre_data_temp, pre_data_length);
        free(pre_data_temp);
        
        //read suffix data from device to buf_list
        int suf_data_length = upper - (offset + count);
        char* suf_data = (char*)malloc(suf_data_length);
        char* suf_data_temp;
        ret = posix_memalign((void **)&suf_data_temp, blocksize, pagesize);
        if (ret)
        {
            fprintf(stderr, "posix_memalign failed: %s\n", strerror(errno));
            return ERR; 
        }
        //if(DirectReadUnaligned(suf_data, suf_data_length, offset + count) != suf_data_length)
        if (DirectReadAligned(suf_data_temp, pagesize, LowerAligned(offset + count, pagesize)) != pagesize)
        {
            fprintf(stderr, "pread64 failed: %s\n", strerror(errno));
            return ERR; 
        }
        memcpy(suf_data, suf_data_temp + pagesize - suf_data_length, suf_data_length);
        free(suf_data_temp);

        //copy data to buf_list
        const char* buf_ptr = (const char*)buf;
        std::vector<unsigned char*>::iterator iter;
        unsigned char* buf_item;

        if (total_pages_count == 1)
        {
            unsigned char* buf_item = *buf_list.begin();
            memcpy(buf_item, pre_data, pre_data_length);
            memcpy(buf_item + pre_data_length, buf_ptr, count);
            memcpy(buf_item + pre_data_length + count, suf_data, suf_data_length);
        }
        else
        {
            int index = 0;
            for (iter = buf_list.begin(); iter != buf_list.end(); iter++)
            {
                buf_item = *iter;
                if (index == 0)
                {
                    int copy_readbytes_infirstpage = pagesize - pre_data_length;
                    memcpy(buf_item, pre_data, pre_data_length); 
                    memcpy(buf_item + pre_data_length, buf_ptr, copy_readbytes_infirstpage);
                    buf_ptr += copy_readbytes_infirstpage;
                }
                else if (index == total_pages_count - 1)
                {
                    int copy_offset_inlastpage = pagesize - suf_data_length;
                    memcpy(buf_item, buf_ptr, copy_offset_inlastpage);
                    memcpy(buf_item + copy_offset_inlastpage, suf_data, suf_data_length);
                    buf_ptr += copy_offset_inlastpage;
                }
                else
                {
                    memcpy(buf_item, buf_ptr, pagesize);
                    buf_ptr += pagesize;
                }
                index++;
            }
        }
        
        free(pre_data);
        free(suf_data);

        //write buf_list to device
        for (iter = buf_list.begin(); iter != buf_list.end();) 
        {
            buf_item = *iter;
            if (DirectWriteAligned(buf_item, pagesize, offset_lower) != pagesize)
            {
                fprintf(stderr, "pwrite64 failed: %s\n", strerror(errno));
                return ERR; 
            }
            offset_lower += pagesize;
            iter = buf_list.erase(iter);
            free(buf_item);
        }

        return count;
    }


    //void KernelDevice::NewAlignedBufList()
    ssize_t KernelDevice::DirectReadUnaligned(void* buf, size_t count, off_t offset)
    {
        int ret;
        int pagesize = get_pagesize();
        int blocksize = get_blocksize();

        off_t offset_lower = LowerAligned(offset, pagesize);
        off_t upper = UpperAligned(offset + count, pagesize);
        
        int total_pages_count = (upper - offset_lower) / pagesize;

        //alloc buf_list and pread data to buf_list.
        std::vector<unsigned char*> buf_list;
        for (int index = 0; index < total_pages_count; index++)
        {   
            unsigned char *page_buf;
            ret = posix_memalign((void **)&page_buf, blocksize, pagesize);
            if (ret)
            {
                fprintf(stderr, "posix_memalign failed: %s\n", strerror(errno));
                return ERR; 
            }
            if (DirectReadAligned(page_buf, pagesize, offset_lower + index * pagesize ) != pagesize)
            {
                fprintf(stderr, "pread64 failed: %s\n", strerror(errno));
                return ERR; 
            }
            buf_list.push_back(page_buf);
        }   

        //handle data from buf list to return.
        int64_t copy_offset_infirstpage = offset - offset_lower;
        int64_t copy_bytes_infirstpage = pagesize - copy_offset_infirstpage;

        //int64_t copy_bytes_inlastpage = offset + count - (offset_lower + pagesize * (total_pages_count-1));
        int64_t copy_bytes_inlastpage = offset + count - (LowerAligned(offset + count, pagesize));

        unsigned char* buf_ptr = (unsigned char*)buf;
        if (total_pages_count == 1)
        {   
            unsigned char* buf_item = *buf_list.begin();
            buf_item = *buf_list.begin();
            memcpy(buf_ptr, buf_item + copy_offset_infirstpage, count);
            buf_list.erase(buf_list.begin());
            free(buf_item);
        }   
        else
        {   
            int index = 0 ; 
            std::vector<unsigned char*>::iterator iter;
            unsigned char* buf_item;

            for(iter = buf_list.begin(); iter != buf_list.end(); )
            {
                buf_item = *iter;
                if (index == 0)
                {
                    memcpy(buf_ptr, buf_item + copy_offset_infirstpage, copy_bytes_infirstpage);
                    buf_ptr += copy_bytes_infirstpage;
                }
                else if (index == total_pages_count - 1)
                {
                    memcpy(buf_ptr, buf_item, copy_bytes_inlastpage);
                    buf_ptr += copy_bytes_inlastpage;
                }
                else
                {
                    memcpy(buf_ptr, buf_item, pagesize);
                    buf_ptr += pagesize;
                }
                iter = buf_list.erase(iter);
                free(buf_item);
                index++;
            }
        }

        return count;
    }
    
    ssize_t KernelDevice::DirectWriteAligned(const void* buf, size_t count, off_t offset)
    {
        return pwrite64(m_direct_fd, buf, count, offset);
    }

    ssize_t KernelDevice::DirectReadAligned(void* buf, size_t count, off_t offset)
    {
        return pread64(m_direct_fd, buf, count, offset);
    }

    ssize_t KernelDevice::DirectWrite(const void* buf, size_t count, off_t offset)
    {
        if (IsPageAligned(buf) && IsSectorAligned(count) && IsSectorAligned(offset))
        {
            return DirectWriteAligned(buf, count, offset);
        }
        return DirectWriteUnaligned(buf, count, offset);
    }

    ssize_t KernelDevice::DirectRead(void* buf, size_t count, off_t offset)
    {
        if (IsPageAligned(buf) && IsSectorAligned(count) && IsSectorAligned(offset))
        {
            return DirectReadAligned(buf, count, offset);
        }
        return DirectReadUnaligned(buf, count, offset);
    }
    
    ssize_t KernelDevice::BufferWrite(const void* buf, size_t count, off_t offset)
    {
        return pwrite64(m_buffer_fd, buf, count, offset);
    }

    ssize_t KernelDevice::BufferRead(void* buf, size_t count, off_t offset)
    {
        return pread64(m_buffer_fd, buf, count, offset);
    }

    
    ssize_t KernelDevice::pWrite(const void* buf, size_t count, off_t offset, bool writebuf)
    {
        if(writebuf)
        {
            return BufferWrite(buf, count, offset);
        }
        
        return DirectWrite(buf, count, offset);
    }

    ssize_t KernelDevice::pRead(void* buf, size_t count, off_t offset, bool readbuf)
    {
        if(readbuf)
        {
            return BufferRead(buf, count, offset);
        }

        return DirectRead(buf, count, offset);
    }


    ssize_t KernelDevice::pWritev(const struct iovec *iov, int iovcnt, off_t offset)
    {
        return pwritev(m_buffer_fd, iov, iovcnt, offset);
    }

    ssize_t KernelDevice::pReadv(const struct iovec *iov, int iovcnt, off_t offset)
    {
        return preadv(m_buffer_fd, iov, iovcnt, offset);
    }
}//namespace kvdb
