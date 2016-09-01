/* -*- Mode: C++; c-basic-offset: 4; indent-tabs-mode: nil -*- */
#ifndef _KV_DB_KVDB_IMPL_H_
#define _KV_DB_KVDB_IMPL_H_

#include <list>
#include <atomic>

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
        bool Get(const char* key, uint32_t key_len, string &data);
        bool Delete(const char* key, uint32_t key_len);

        virtual ~KvdbDS();

    private:
        KvdbDS(const string& filename);
        bool openDB();
        bool closeDB();
        bool writeMetaDataToDevice();
        bool readMetaDataFromDevice();
        void startThds();
        void stopThds();

        enum OpType {INSERT, UPDATE, DELETE};
        bool insertKey(KVSlice& slice, OpType op_type);
        void updateMeta(Request *req, OpType op_type);

        bool readData(HashEntry* entry, string &data);
        bool enqueReqs(Request *req);
        bool findAvailableSeg(Request *req, SegmentSlice*& seg_ptr);


    private:
        SuperBlockManager* sbMgr_;
        IndexManager* idxMgr_;
        BlockDevice* bdev_;
        SegmentManager* segMgr_;
        string fileName_;

        Mutex segQueMtx_;

    private:
        friend class SegThd;
        class SegThd : public Thread
        {
        public:
            SegThd(): db_(NULL){}
            SegThd(KvdbDS* db): db_(db){}
            virtual ~SegThd(){}
            SegThd(SegThd& toBeCopied) = delete;
            SegThd& operator=(SegThd& toBeCopied) = delete;

            virtual void* Entry() { db_->SegThdEntry(); return 0; }

        private:
            friend class KvdbDS;
            KvdbDS* db_;
        };
        SegThd segThd_;
        std::list<SegmentSlice*> segQue_;
        std::atomic<bool> segThd_stop_;
        void SegThdEntry();

    private:
        static KvdbDS *instance_;

    };

}  // namespace kvdb

#endif  // #ifndef _KV_DB_KVDB_IMPL_H_
