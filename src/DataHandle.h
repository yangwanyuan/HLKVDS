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
        bool WriteData(Kvdb_Digest& digest, const char* data, uint16_t length);

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
        SegmentSlice(uint32_t seg_id, SegmentManager* sm);
        ~SegmentSlice();

        bool Put(DataHeader& header, const char* data, uint16_t length);
        const void* GetData() const {return m_data;}
        uint32_t GetLength() const {return m_len;}
    private:
        uint32_t m_id;
        SegmentManager* m_sm;
        uint32_t m_seg_size;
        char* m_data;
        uint32_t m_len;
            
    };

} //end namespace kvdb


#endif //#ifndef _KV_DB_DATAHANDLE_H_
