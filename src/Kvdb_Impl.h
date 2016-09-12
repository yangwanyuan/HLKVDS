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

    // Seg Write to device thread
    private:
        friend class SegWriteThd;
        class SegWriteThd : public Thread
        {
        public:
            SegWriteThd(): db_(NULL){}
            SegWriteThd(KvdbDS* db): db_(db){}
            virtual ~SegWriteThd(){}
            SegWriteThd(SegWriteThd& toBeCopied) = delete;
            SegWriteThd& operator=(SegWriteThd& toBeCopied) = delete;

            virtual void* Entry() { db_->SegWriteThdEntry(); return 0; }

        private:
            friend class KvdbDS;
            KvdbDS* db_;
        };
        SegWriteThd segWriteT_;
        std::list<SegmentSlice*> segWriteQue_;
        std::atomic<bool> segWriteT_stop_;
        void SegWriteThdEntry();

    ////Seg producer thread
    //private:
    //    friend class SegProductThd;
    //    class SegProductThd : public Thread
    //    {
    //    public:
    //        SegProductThd(): db_(NULL){}
    //        SegProductThd(KvdbDS* db): db_(db){}
    //        virtual ~SegProductThd(){}
    //        SegProductThd(SegProductThd& toBeCopied) = delete;
    //        SegProductThd& operator=(SegProductThd& toBeCopied) = delete;

    //        virtual void* Entry() { db_->SegProductThdEntry(); return 0; }

    //    private:
    //        friend class KvdbDS;
    //        KvdbDS* db_;
    //    };
    //    SegProductThd segProductT_;
    //    std::list<SegmentSlice*> segProductQue_;
    //    std::atomic<bool> segProductT_stop_;
    //    void SegProductThdEntry();
    //
    //
    //private:
    //    friend class SegTimeoutThd;
    //    class SegTimeoutThd : public Thread
    //    {
    //    public:
    //        SegTimeoutThd(): db_(NULL){}
    //        SegTimeoutThd(KvdbDS* db): db_(db){}
    //        virtual ~SegTimeoutThd(){}
    //        SegTimeoutThd(SegTimeoutThd& toBeCopied) = delete;
    //        SegTimeoutThd& operator=(SegTimeoutThd& toBeCopied) = delete;

    //        virtual void* Entry() { db_->SegTimeoutThdEntry(); return 0; }

    //    private:
    //        friend class KvdbDS;
    //        KvdbDS* db_;
    //    };
    //    SegTimeoutThd segTimeoutT_;
    //    std::list<SegmentSlice*> segWorkQue_;
    //    std::atomic<bool> segTimeoutT_stop_;
    //    void SegTimeoutThdEntry();

    private:
        static KvdbDS *instance_;

    };

}  // namespace kvdb

#endif  // #ifndef _KV_DB_KVDB_IMPL_H_
