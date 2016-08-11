#ifndef _KV_DB_DATAHANDLE_H_
#define _KV_DB_DATAHANDLE_H_

#include <string>
#include <sys/types.h>
#include "Db_Structure.h"
#include "BlockDevice.h"
#include "KeyDigestHandle.h"
#include "IndexManager.h"
#include "SuperBlockManager.h"
#include "SegmentManager.h"

using namespace std;

namespace kvdb{

    class DataHandle{
    public:
        bool ReadData(HashEntry* entry, string &data);
        bool WriteData(Kvdb_Digest digest, const char* data, uint16_t length);

        DataHandle(BlockDevice* bdev, SuperBlockManager* sbm, IndexManager* im, SegmentManager* sm);
        ~DataHandle();
    private:
        BlockDevice* m_bdev;
        SuperBlockManager* m_sbm;
        IndexManager* m_im;
        SegmentManager* m_sm;
    };

    class SegmentSlice{
    public:
        SegmentSlice(uint32_t seg_id, SegmentManager* sm) :  
            m_id(seg_id), m_sm(sm), m_data(NULL), m_len(0){}
        ~SegmentSlice();

        bool Put(DataHeader& header, const char* data, uint16_t length);
        void GetData(char* ptr, uint32_t len);
    private:
        uint32_t m_id;
        SegmentManager* m_sm;
        char* m_data;
        uint32_t m_len;
            
    };

} //end namespace kvdb


#endif //#ifndef _KV_DB_DATAHANDLE_H_
