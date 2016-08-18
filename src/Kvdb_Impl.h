/* -*- Mode: C++; c-basic-offset: 4; indent-tabs-mode: nil -*- */
#ifndef _KV_DB_KVDB_IMPL_H_
#define _KV_DB_KVDB_IMPL_H_

#include <list>

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
        static KvdbDS* Create_KvdbDS(const char* filename,
                                    uint32_t hash_table_size,
                                    uint32_t segment_size);
        static KvdbDS* Open_KvdbDS(const char* filename);

        bool Insert(const char* key, uint32_t key_len,
                    const char* data, uint16_t length);
        bool Get(const char* key, uint32_t key_len, string &data) const;
        bool Delete(const char* key, uint32_t key_len);

        bool WriteMetaDataToDevice();
        bool ReadMetaDataFromDevice();

        virtual ~KvdbDS();

    private:
        KvdbDS(const string& filename);
        bool reopen();
        void resetThreads();

        bool insertNewKey(Kvdb_Digest* digest, const char* data, uint16_t length);
        bool updateExistKey(Kvdb_Digest* digest, const char* data, uint16_t length);

    private:
        SuperBlockManager* m_sb_manager;
        IndexManager* m_index_manager;
        DataHandle* m_data_handle;
        BlockDevice* m_bdev;
        SegmentManager* m_segment_manager;
        string m_filename;

        Mutex *m_mutex;
        Cond *m_cond;

    private:
        friend class ReqsThread;
        class ReqsThread : public Thread
        {
        public:
            ReqsThread(): m_db(NULL){}
            ReqsThread(KvdbDS* db): m_db(db){}
            virtual ~ReqsThread(){}
            ReqsThread(ReqsThread& toCopied) = delete;
            ReqsThread& operator=(ReqsThread& toCopied) = delete;

            virtual void* Entry() { return m_db->ReqsThreadEntry(); }

        private:
            friend class KvdbDS;
            KvdbDS* m_db;
        };
        ReqsThread m_reqs_t;
        std::list<Request*> m_reqs;
        void* ReqsThreadEntry();

    };

}  // namespace kvdb

#endif  // #ifndef _KV_DB_KVDB_IMPL_H_
