#ifndef _KV_DB_DATAHANDLE_H_
#define _KV_DB_DATAHANDLE_H_

#include <string>
#include <sys/types.h>
#include "Db_Structure.h"
#include "BlockDevice.h"
#include "KeyDigestHandle.h"

using namespace std;

namespace kvdb{

    class DataHeader {
    public:
        //uint32_t key_digest[KEYDIGEST_SIZE];
        Kvdb_Digest key_digest;
        uint16_t data_size;
        uint32_t data_offset;
        uint32_t next_header_offset;

    public:
        DataHeader();
        DataHeader(Kvdb_Digest &digest, uint16_t data_size, uint32_t data_offset, uint32_t next_header_offset);
        DataHeader(const DataHeader& toBeCopied);
        ~DataHeader();
        DataHeader& operator=(const DataHeader& toBeCopied);

    } __attribute__((__packed__));

    //struct DataHeaderOffset{
    //    uint32_t segment_id;
    //    uint32_t header_offset;
    //}__attribute__((__packed__));
    class DataHeaderOffset{
    public:
        uint32_t physical_offset;

    public:
        DataHeaderOffset(): physical_offset(0){}
        DataHeaderOffset(uint32_t offset);
        DataHeaderOffset(const DataHeaderOffset& toBeCopied);
        ~DataHeaderOffset();
        DataHeaderOffset& operator=(const DataHeaderOffset& toBeCopied);

    }__attribute__((__packed__));

    class DataHandle{
    public:
        bool ReadDataHeader(off_t offset, DataHeader &data_header, string &key);
        bool WriteDataHeader();
        bool ReadData(DataHeader* data_header, string &data);
        bool WriteData(DataHeader* data_header, const char* data, uint32_t length, off_t offset);
        bool DeleteData(const char* key, uint32_t key_len, off_t offset); 

        DataHandle(BlockDevice* bdev);
        ~DataHandle();
    private:
        BlockDevice* m_bdev;
    };
}


#endif //#ifndef _KV_DB_DATAHANDLE_H_
