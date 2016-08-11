/* -*- Mode: C++; c-basic-offset: 4; indent-tabs-mode: nil -*- */
#ifndef _KV_DB_KVDB_IMPL_H_
#define _KV_DB_KVDB_IMPL_H_

#include "HashFun.h"
#include "Db_Structure.h"
#include "BlockDevice.h"
#include "SuperBlockManager.h"
#include "IndexManager.h"
#include "DataHandle.h"
#include "SegmentManager.h"

using namespace std;

namespace kvdb {

    class KvdbDS {
    public:
        static KvdbDS* Create_KvdbDS(const char* filename, uint32_t hash_table_size,
                                        uint32_t segment_size)
            __attribute__ ((warn_unused_result));

        static KvdbDS* Open_KvdbDS(const char* filename);

        bool Insert(const char* key, uint32_t key_len, const char* data, uint16_t length)
            __attribute__ ((warn_unused_result));
        bool Delete(const char* key, uint32_t key_len) __attribute__ ((warn_unused_result));
        bool Get(const char* key, uint32_t key_len, string &data) const
            __attribute__ ((warn_unused_result));

        // Used to write header/hashtable sequentially for split/merge/rewrite
        bool WriteMetaDataToDevice() __attribute__ ((warn_unused_result));
        // Used to read header/hashtable sequentially when header is malloc'd
        bool ReadMetaDataFromDevice() __attribute__ ((warn_unused_result));

        virtual ~KvdbDS();
    private:
        KvdbDS(const string& filename);
        bool ReopenKvdbDS();

        SuperBlockManager* m_sb_manager;
        IndexManager* m_index_manager;
        DataHandle* m_data_handle;
        BlockDevice* m_bdev;
        SegmentManager* m_segment_manager;
        string m_filename;
    };

}  // namespace kvdb

#endif  // #ifndef _KV_DB_KVDB_IMPL_H_
