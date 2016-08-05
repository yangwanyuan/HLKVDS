#include <sys/uio.h>
#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <errno.h>
#include <inttypes.h>

#include "DataHandle.h"

namespace kvdb{

    DataHeader::DataHeader() : data_size(0), data_offset(0), next_header_offset(0)
    {
        //memset(&key_digest, 0, sizeof(Kvdb_Digest));
        ;
    }

    DataHeader::DataHeader(Kvdb_Digest &digest, uint16_t size, uint32_t offset, uint32_t next_offset)
    {
        key_digest = digest;
        data_size = size;
        data_offset = offset;
        next_header_offset = next_offset;
    }

    DataHeader::DataHeader(const DataHeader& toBeCopied)
    {
        key_digest = toBeCopied.key_digest;
        data_size = toBeCopied.data_size;
        data_offset = toBeCopied.data_offset;
        next_header_offset = toBeCopied.next_header_offset;
    }

    DataHeader::~DataHeader()
    {
        return;
    }

    DataHeader& DataHeader::operator=(const DataHeader& toBeCopied)
    {
        key_digest = toBeCopied.key_digest;
        data_size = toBeCopied.data_size;
        data_offset = toBeCopied.data_offset;
        next_header_offset = toBeCopied.next_header_offset;
        return *this;
    }


    DataHeaderOffset::DataHeaderOffset(uint32_t offset)
    {
        physical_offset = offset;
    }

    DataHeaderOffset::DataHeaderOffset(const DataHeaderOffset& toBeCopied)
    {
        physical_offset = toBeCopied.physical_offset;
    }

    DataHeaderOffset::~DataHeaderOffset()
    {
        return;
    }

    DataHeaderOffset& DataHeaderOffset::operator=(const DataHeaderOffset& toBeCopied)
    {
        physical_offset = toBeCopied.physical_offset;
        return *this;
    }

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
    
        //char *mdata = (char*)malloc(data_len);
        char *mdata = new char[data_len];
        if((uint64_t)m_bdev->pRead(mdata, data_len, data_offset) != data_len){
            perror("Could not read data at position");
            free(mdata);
            return false; 
        }
        data.assign(mdata, data_len);
        delete[] mdata;
        return true;
    }
    
    bool DataHandle::WriteData(DataHeader* data_header, const char* data, uint32_t length, off_t offset)
    {
        /**************************************************************************************
        //Struct DataHeader *data_header = new DataHeader;
        ////data_header.key = digest->value;
        //memcpy(data_header->key, (digest->value), sizeof(Kvdb_Digest));
        //Data_header->data_size = length;
        //Data_header->data_offset = offset + sizeof(DataHeader);
        //Data_header->next_header_offset = offset + sizeof(DataHeader) + length;

        //struct iovec iov[3];
        //iov[0].iov_base = &data_header;
        //iov[0].iov_len = sizeof(DataHeader);
        //iov[1].iov_base = const_cast<char *>(key);
        //iov[1].iov_len = key_len;
        //iov[2].iov_base = const_cast<char *>(data);
        //iov[2].iov_len = length;

        //if (m_bdev->pWritev(iov, 3, offset) != (ssize_t) (sizeof(DataHeader) + key_len + length)) {
        //    fprintf(stderr, "Could not write iovec structure: %s\n", strerror(errno));
        //    return false;
        //}
        //delete data_header;
        ************************************************************************************/


        //ssize_t dataheader_len = sizeof(DataHeader);
        //if(m_bdev->pWrite(&data_header, dataheader_len, offset) != dataheader_len){
        //    fprintf(stderr, "Could not write dataheader: %s\n", strerror(errno));
        //    return false;
        //}
        //offset += dataheader_len;

        //if(m_bdev->pWrite(data, length, offset) != length){
        //    fprintf(stderr, "Could not write data: %s\n", strerror(errno));
        //    return false;
        //}

        ssize_t dataheader_len = sizeof(DataHeader);
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
        __DEBUG("offset: %ld, data_len:%ld, header_len:%ld", offset, (ssize_t)length, dataheader_len );
        

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


