#include <sys/uio.h>
#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <errno.h>
#include <inttypes.h>

#include "DataHandle.h"

namespace kvdb{

    bool DataHandle::ReadData(HashEntry* entry, string &data)
    {
        uint16_t data_len = entry->GetDataSize();
        if (data_len == 0)
        { 
            return true;
        }

        uint64_t data_offset = 0;
        if (!m_sm->ComputeDataOffsetPhyFromEntry(entry, data_offset))
        {
            return false;
        }

        char *mdata = new char[data_len];
        if (m_bdev->pRead(mdata, data_len, data_offset) != (ssize_t)data_len)
        {
            perror("Could not read data at position");
            delete[] mdata;
            return false; 
        }
        data.assign(mdata, data_len);
        delete[] mdata;

        __DEBUG("get data offset %ld", data_offset);

        return true;
    }
    
    bool DataHandle::WriteData(Kvdb_Digest& digest, const char* data, uint16_t length)
    {

        uint32_t seg_id = 0;
        if (!m_sm->GetEmptySegId(seg_id))
        {
            return false;
        }

        uint64_t seg_offset; 
        m_sm->ComputeSegOffsetFromId(seg_id, seg_offset);

        uint32_t head_offset = sizeof(SegmentOnDisk);
        uint32_t data_offset = head_offset + sizeof(DataHeader);
        uint32_t next_offset = data_offset + length;

        SegmentOnDisk seg_ondisk(1);
        DataHeader data_header(digest, length, data_offset, next_offset);

        ////---Write data to device 1:
        //
        //m_bdev->pWrite(&seg_ondisk, sizeof(SegmentOnDisk), seg_offset);
        //m_bdev->pWrite(&data_header, sizeof(DataHeader), seg_offset + head_offset);
        //m_bdev->pWrite(data, length, seg_offset + data_offset);
        //
        ////---Write data to device 1 end


        ////---Write data to device 2:
        //struct iovec iov[3];
        //iov[0].iov_base = &seg_ondisk;
        //iov[0].iov_len = sizeof(SegmentOnDisk);
        //iov[1].iov_base = &data_header;
        //iov[1].iov_len = sizeof(DataHeader);
        //iov[2].iov_base = const_cast<char *>(data);
        //iov[2].iov_len = length;

        //if (m_bdev->pWritev(iov, 3, seg_offset) != (int64_t) (sizeof(SegmentOnDisk) + sizeof(DataHeader) + length)) {
        //    fprintf(stderr, "Could not write iovec structure: %s\n", strerror(errno));
        //    return false;
        //}
        ////---Write data to device 2 end


        //---Write data to device 3:
        SegmentSlice slice(seg_id, m_sm);
        slice.Put(data_header, data, length);

        if (m_bdev->pWrite(slice.GetData(), slice.GetLength(), seg_offset) != slice.GetLength()) {
            fprintf(stderr, "Could  write data to device: %s\n", strerror(errno));
            return false;
        }
        //---Write data to device 3 end


        m_sm->Update(seg_id);
        m_im->UpdateIndexFromInsert(&data_header, &digest, head_offset, seg_offset);
        DBSuperBlock sb = m_sbm->GetSuperBlock();
        sb.current_segment = seg_id;
        m_sbm->SetSuperBlock(sb);


        __DEBUG("write seg_id:%d, seg_offset: %ld, head_offset: %d, data_offset:%d, header_len:%d, data_len:%d", 
                seg_id, seg_offset, head_offset, data_offset, (uint32_t)sizeof(DataHeader), (uint32_t)length );

        return true;

    }
    

    DataHandle::DataHandle(BlockDevice* bdev, SuperBlockManager* sbm, IndexManager* im, SegmentManager* sm):
        m_bdev(bdev), m_sbm(sbm), m_im(im), m_sm(sm)
    {
        ;
    }

    DataHandle::~DataHandle()
    {
        ;
    }

    SegmentSlice::~SegmentSlice()
    {
        if(m_data)
        {
            delete[] m_data;
        }
    }

    SegmentSlice::SegmentSlice(uint32_t seg_id, SegmentManager* sm)
        : m_id(seg_id), m_sm(sm)
    {
        m_seg_size = m_sm->GetSegmentSize();
        m_data = new char[m_seg_size];
        //memset(m_data, 0, m_seg_size);
        m_len = 0;
    }

    bool SegmentSlice::Put(DataHeader& header, const char* data, uint16_t length)
    {
        SegmentOnDisk seg_ondisk;
        m_len = sizeof(SegmentOnDisk) + sizeof(DataHeader) + length;
        memcpy(m_data, &seg_ondisk, sizeof(SegmentOnDisk));
        memcpy(&m_data[sizeof(SegmentOnDisk)], &header, sizeof(DataHeader));
        memcpy(&m_data[sizeof(SegmentOnDisk) + sizeof(DataHeader)], data, length);

        return true;
    }
    

}


