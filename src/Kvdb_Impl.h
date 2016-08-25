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

        virtual ~KvdbDS();

    private:
        KvdbDS(const string& filename);
        bool openDB();
        bool closeDB();
        bool writeMetaDataToDevice();
        bool readMetaDataFromDevice();
        void startThreads();
        void stopThreads();

        bool insertNewKey(const KVSlice *slice);
        bool updateExistKey(const KVSlice *slice);


    private:
        SuperBlockManager* m_sb_manager;
        IndexManager* m_index_manager;
        DataHandle* m_data_handle;
        BlockDevice* m_bdev;
        SegmentManager* m_segment_manager;
        string m_filename;

        Mutex *m_reqsQueue_mutex;

    private:
        friend class ReqsThread;
        class ReqsThread : public Thread
        {
        public:
            ReqsThread(): m_db(NULL){}
            ReqsThread(KvdbDS* db): m_db(db){}
            virtual ~ReqsThread(){}
            ReqsThread(ReqsThread& toBeCopied) = delete;
            ReqsThread& operator=(ReqsThread& toBeCopied) = delete;

            virtual void* Entry() { m_db->ReqsThreadEntry(); return 0; }

        private:
            friend class KvdbDS;
            KvdbDS* m_db;
        };
        ReqsThread m_reqs_t;
        std::list<Request*> m_reqs_list;
        void ReqsThreadEntry();
        bool m_stop_reqs_t;

    };

}  // namespace kvdb

#endif  // #ifndef _KV_DB_KVDB_IMPL_H_
