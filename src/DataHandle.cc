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
        return true;
    }
    
    bool DataHandle::WriteDataHeader()
    {
        return true;
    }
    
    bool DataHandle::ReadData(DataHeader* data_header, string &data)
    {
        uint32_t data_len = data_header->data_size;
        uint32_t data_offset = data_header->data_offset;
        if(data_len == 0){ 
            return true;
        }
    
        char *mdata = (char*)malloc(data_len);
        if((uint64_t)m_bdev->pRead(mdata, data_len, data_offset) != data_len){
            perror("Could not read data at position");
            free(mdata);
            return false; 
        }
        data.assign(mdata, data_len);
        free(mdata);
        return true;
    }
    
    bool DataHandle::WriteData(DataHeader* data_header, const char* data, uint32_t length, off_t offset)
    {
        /**************************************************************************************
        //Struct DataHeader *data_header = new DataHeader;
        ////data_header.key = digest->value;
        //Memcpy(data_header->key, (digest->value), 20);
        //Data_header->data_size = length;
        //Data_header->data_offset = offset + sizeof(struct DataHeader);
        //Data_header->next_header_offset = offset + sizeof(struct DataHeader) + length;

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
        ************************************************************************************/


        //ssize_t dataheader_len = sizeof(struct DataHeader);
        //if(m_bdev->pWrite(&data_header, dataheader_len, offset) != dataheader_len){
        //    fprintf(stderr, "Could not write dataheader: %s\n", strerror(errno));
        //    return false;
        //}
        //offset += dataheader_len;

        //if(m_bdev->pWrite(data, length, offset) != length){
        //    fprintf(stderr, "Could not write data: %s\n", strerror(errno));
        //    return false;
        //}

        ssize_t dataheader_len = sizeof(struct DataHeader);
        ssize_t data_all_len = dataheader_len + length;

        unsigned char* data_all = (unsigned char*)malloc(data_all_len);
        memcpy(data_all, (char *)&data_header, dataheader_len);
        memcpy(data_all + dataheader_len, data, length);
        if(m_bdev->pWrite(data_all, data_all_len, offset) != data_all_len){
            fprintf(stderr, "Could not write data: %s\n", strerror(errno));
            free(data_all);
            return false;
        }
        free(data_all);
        __DEBUG("offset: %ld, data_len:%ld, header_len:%ld", offset, data_all_len, dataheader_len );
        

        //delete data_header;

        return true;
    }
    
    bool DataHandle::DeleteData(const char* key, uint32_t key_len, off_t offset) 
    {

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


