#include <sys/uio.h>
#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <errno.h>
#include <inttypes.h>

#include "DataHandle.h"

namespace kvdb{

    bool DataHandle::ReadDataHeader(off_t offset, DataHeader &data_header, string &key)
    {
        ssize_t n_read = m_bdev->pRead(&data_header, sizeof(struct DataHeader), offset);
        if (n_read < (ssize_t)sizeof(struct DataHeader)) {
            fprintf(stderr, "Read %lu bytes from DataHeader, expected %lu\n", n_read, sizeof(struct DataHeader));
            return false;
        }
        uint32_t key_len = data_header.key_length;
        char *mdata = (char *)malloc(key_len);

        n_read = m_bdev->pRead(mdata, key_len, offset + sizeof(struct DataHeader));
        if (n_read < key_len) {
            free(mdata);
            fprintf(stderr, "Read %lu bytes from key, expected %u\n", n_read, key_len);
            return false;
        }
        key.assign(mdata, key_len);
        free(mdata);
        return true;
    }
    
    bool DataHandle::WriteDataHeader()
    {
        return true;
    }
    
    bool DataHandle::ReadData(const char* key, uint32_t key_len, off_t offset, string &data)
    {
        DataHeader data_header;
        string inputkey;
    
        if(!ReadDataHeader(offset, data_header, inputkey)){
            return false;
        }
    
        if(memcmp(inputkey.data(), key, key_len) != 0){ 
            return false;
        }
    
        size_t length = data_header.data_length;
        if(length == 0){ 
            return true;
        }
    
        char *mdata = (char*)malloc(length);
        if((uint64_t)m_bdev->pRead(mdata, length, offset+key_len+sizeof(struct DataHeader)) != length){
            perror("Could not read data at position");
            free(mdata);
            return false; 
        }
        data.assign(mdata, length);
        free(mdata);
        return true;
    }
    
    bool DataHandle::WriteData(const char* key, uint32_t key_len, const char* data, uint32_t length, off_t offset)
    {
        struct DataHeader data_header;
        data_header.data_length = length;
        data_header.key_length = key_len;
        data_header.deleteLog = false;

        //struct iovec iov[3];
        //iov[0].iov_base = &data_header;
        //iov[0].iov_len = sizeof(struct DataHeader);
        //iov[1].iov_base = const_cast<char *>(key);
        //iov[1].iov_len = key_len;
        //iov[2].iov_base = const_cast<char *>(data);
        //iov[2].iov_len = length;

        //if (m_bdev->pWritev(iov, 3, offset) != (ssize_t) (sizeof(struct DataHeader) + key_len + length)) {
        //    fprintf(stderr, "Could not write iovec structure: %s\n", strerror(errno));
        //    return false;
        //}

        //ssize_t dataheader_len = sizeof(struct DataHeader);
        //if(m_bdev->pWrite(&data_header, dataheader_len, offset) != dataheader_len){
        //    fprintf(stderr, "Could not write dataheader: %s\n", strerror(errno));
        //    return false;
        //}
        //offset += dataheader_len;

        //if(m_bdev->pWrite(key, key_len, offset) != key_len){
        //    fprintf(stderr, "Could not write key: %s\n", strerror(errno));
        //    return false;
        //}
        //offset += key_len;

        //if(m_bdev->pWrite(data, length, offset) != length){
        //    fprintf(stderr, "Could not write data: %s\n", strerror(errno));
        //    return false;
        //}

        ssize_t dataheader_len = sizeof(struct DataHeader);
        ssize_t data_all_len = dataheader_len + key_len + length;

        unsigned char* data_all = (unsigned char*)malloc(data_all_len);
        memcpy(data_all, (char *)&data_header, dataheader_len);
        memcpy(data_all + dataheader_len, key, key_len );
        memcpy(data_all + dataheader_len + key_len, data, length);
        if(m_bdev->pWrite(data_all, data_all_len, offset) != data_all_len){
            fprintf(stderr, "Could not write data: %s\n", strerror(errno));
            free(data_all);
            return false;
        }
        free(data_all);
        //__DEBUG("offset: %ld, data_len:%ld, header_len:%ld", offset, data_all_len, dataheader_len );
        


        return true;
    }
    
    bool DataHandle::DeleteData(const char* key, uint32_t key_len, off_t offset) 
    {
        /*********   DELETE LOG    **********/
        // Just needs the key and the fact that it's deleted to be appended.
        struct DataHeader delete_header;
        delete_header.data_length = 0;
        delete_header.key_length = key_len;
        delete_header.deleteLog = true;

        if ((uint64_t)m_bdev->pWrite(&delete_header, sizeof(struct DataHeader),
                               offset) != sizeof(struct DataHeader)) {
            fprintf(stderr, "Could not write delete header at position %"PRIu64": %s\n",
                    (uint64_t)offset, strerror(errno));
            return false;
        }

        if ((uint64_t)m_bdev->pWrite(key, key_len,
                               offset + sizeof(struct DataHeader)) != key_len) {
            fprintf(stderr, "Could not write delete header at position %"PRIu64": %s\n",
                    (uint64_t)offset + sizeof(struct DataHeader), strerror(errno));
            return false;
        }

        return true;
    }

    DataHandle::DataHandle(BlockDevice* bdev):
        m_bdev(bdev)
    {
        ;
    }

    DataHandle::~DataHandle()
    {
        ;
    }
}


