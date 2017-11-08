#ifndef _HLKVDS_INDEXMANAGER_H_
#define _HLKVDS_INDEXMANAGER_H_

#include <string>
#include <sys/time.h>
#include <mutex>
#include <list>

#include "hlkvds/Options.h"
#include "Utils.h"
#include "KeyDigestHandle.h"
#include "LinkedList.h"

#include "Segment.h"
#include "WorkQueue.h"

namespace hlkvds {

class SuperBlockManager;
class KVSlice;
class SimpleDS_Impl;

class DataHeader {
private:
    Kvdb_Digest key_digest;
    uint16_t key_size;
    uint16_t data_size;
    uint32_t data_offset;
    uint32_t next_header_offset;

public:
    DataHeader();
    DataHeader(const Kvdb_Digest &digest, uint16_t key_len, uint16_t data_len,
               uint32_t data_offset, uint32_t next_header_offset);

    ~DataHeader();

    uint16_t GetKeySize() const {
        return key_size;
    }
    uint16_t GetDataSize() const {
        return data_size;
    }
    uint32_t GetDataOffset() const {
        return data_offset;
    }
    uint32_t GetNextHeadOffset() const {
        return next_header_offset;
    }
    Kvdb_Digest GetDigest() const {
        return key_digest;
    }

    void SetDigest(const Kvdb_Digest& digest);
    void SetKeySize(uint16_t size) {
        key_size = size;
    }
    void SetDataSize(uint16_t size) {
        data_size = size;
    }
    void SetDataOffset(uint32_t offset) {
        data_offset = offset;
    }
    void SetNextHeadOffset(uint32_t offset) {
        next_header_offset = offset;
    }

}__attribute__((__packed__));

class DataHeaderAddress {
private:
    uint16_t location;
    uint64_t offset;

public:
    DataHeaderAddress() :
        location(0), offset(0) {
    }
    DataHeaderAddress(uint16_t lctn, uint64_t offset) :
        location(lctn), offset(offset) {
    }
    ~DataHeaderAddress();

    uint64_t GetHeaderOffset() const {
        return offset;
    }

    uint16_t GetLocation() const {
        return location;
    }

}__attribute__((__packed__));

class HashEntryOnDisk {
private:
    DataHeader header;
    DataHeaderAddress address;

public:
    HashEntryOnDisk();
    HashEntryOnDisk(DataHeader& dataheader, DataHeaderAddress& addrs);
    HashEntryOnDisk(const HashEntryOnDisk& toBeCopied);
    ~HashEntryOnDisk();
    HashEntryOnDisk& operator=(const HashEntryOnDisk& toBeCopied);

    uint16_t GetHeaderLocation() const {
        return address.GetLocation();
    }
    uint64_t GetHeaderOffset() const {
        return address.GetHeaderOffset();
    }
    uint16_t GetKeySize() const {
        return header.GetKeySize();
    }
    uint16_t GetDataSize() const {
        return header.GetDataSize();
    }
    uint32_t GetDataOffsetInSeg() const {
        return header.GetDataOffset();
    }
    uint32_t GetNextHeadOffsetInSeg() const {
        return header.GetNextHeadOffset();
    }
    Kvdb_Digest GetKeyDigest() const {
        return header.GetDigest();
    }
    DataHeader& GetDataHeader() {
        return header;
    }

    void SetKeyDigest(const Kvdb_Digest& digest);

}__attribute__((__packed__));

class HashEntry {
public:
    class LogicStamp {
    private:
        KVTime segTime_;
        int32_t keyNo_;
    public:
        LogicStamp() :
            segTime_(KVTime()), keyNo_(0) {
        }
        LogicStamp(KVTime seg_time, int32_t key_no) :
            segTime_(seg_time), keyNo_(key_no) {
        }
        LogicStamp(const LogicStamp& toBeCopied) {
            segTime_ = toBeCopied.segTime_;
            keyNo_ = toBeCopied.keyNo_;
        }
        ~LogicStamp() {
        }
        LogicStamp& operator=(const LogicStamp& toBeCopied) {
            segTime_ = toBeCopied.segTime_;
            keyNo_ = toBeCopied.keyNo_;
            return *this;
        }

        bool operator<(const LogicStamp& toBeCopied) {
            if ((segTime_ < toBeCopied.segTime_) || (segTime_
                    == toBeCopied.segTime_ && (keyNo_ < toBeCopied.keyNo_))) {
                return true;
            }
            return false;
        }

        bool operator>(const LogicStamp& toBeCopied) {
            if ((segTime_ > toBeCopied.segTime_) || (segTime_
                    == toBeCopied.segTime_ && (keyNo_ > toBeCopied.keyNo_))) {
                return true;
            }
            return false;
        }

        bool operator==(const LogicStamp& toBeCopied) {
            return ((segTime_ == toBeCopied.segTime_) && (keyNo_
                    == toBeCopied.keyNo_));
        }

        KVTime& GetSegTime() {
            return segTime_;
        }

        int32_t GetKeyNo() {
            return keyNo_;
        }

        void Set(KVTime seg_time, int32_t seg_key_no) {
            segTime_ = seg_time;
            keyNo_ = seg_key_no;
        }
        };

        HashEntry();
        HashEntry(HashEntryOnDisk& entry_ondisk, KVTime time_stamp, void* read_ptr);
        HashEntry(DataHeader& data_header, DataHeaderAddress& addrs, void* read_ptr);
        HashEntry(const HashEntry&);
        ~HashEntry();
        bool operator==(const HashEntry& toBeCompare) const;
        HashEntry& operator=(const HashEntry& toBeCopied);

        uint16_t GetHeaderLocation() const {
            return entryPtr_->GetHeaderLocation();
        }
        uint64_t GetHeaderOffset() const {
            return entryPtr_->GetHeaderOffset();
        }

        uint16_t GetKeySize() const {
            return entryPtr_->GetKeySize();
        }

        uint16_t GetDataSize() const {
            return entryPtr_->GetDataSize();
        }

        uint32_t GetDataOffsetInSeg() const {
            return entryPtr_->GetDataOffsetInSeg();
        }

        uint32_t GetNextHeadOffsetInSeg() const {
            return entryPtr_->GetNextHeadOffsetInSeg();
        }

        void* GetReadCachePtr() const {
            return cachePtr_;
        }

        Kvdb_Digest GetKeyDigest() const {
            return entryPtr_->GetKeyDigest();
        }

        HashEntryOnDisk& GetEntryOnDisk() {
            return *entryPtr_;
        }

        LogicStamp* GetLogicStamp() {
            return stampPtr_;
        }

        void SetKeyDigest(const Kvdb_Digest& digest);
        void SetLogicStamp(KVTime seg_time, int32_t seg_key_no);

    private:
        HashEntryOnDisk *entryPtr_;
        LogicStamp *stampPtr_;
        void* cachePtr_;

    };

    

    class IndexManager{
    public:
        static inline size_t SizeOfDataHeader() {
            return sizeof(DataHeader);
        }

        static inline size_t SizeOfHashEntryOnDisk() {
            return sizeof(HashEntryOnDisk);
        }

        static uint64_t ComputeIndexSizeOnDevice(uint32_t ht_size);
        static uint32_t ComputeHashSizeForPower2(uint32_t number);

        void InitMeta(uint32_t ht_size, uint64_t ondisk_size, uint64_t data_theory_size, uint32_t element_num);
        void UpdateMetaToSB();
        bool Get(char* buff, uint64_t length);
        bool Set(char* buff, uint64_t length);

        void InitDataStor(SimpleDS_Impl *ds) { dataStor_ = ds; }

        bool UpdateIndex(KVSlice* slice);
        void UpdateIndexes(std::list<KVSlice*> &slice_list);
        bool GetHashEntry(KVSlice *slice);
        void RemoveEntry(HashEntry entry);

        uint32_t GetHashTableSize() const {
            return htSize_;
        }

        uint64_t GetDataTheorySize() const ;
        uint32_t GetKeyCounter() const ;

        void StartThds();
        void StopThds();

        void AddToReaper(SegForReq*);
        int GetSegReaperQueSize() {
            return (!segRprWQ_)? 0: segRprWQ_->Size();
        }

        IndexManager(SuperBlockManager* sbm, Options &opt);
        ~IndexManager();

        bool IsSameInMem(HashEntry entry);

        LinkedList<HashEntry>* GetEntryListByNo(uint32_t no) {
            return hashtable_[no].entryList_;
        }
    public:
        struct HashtableSlot
        {
            LinkedList<HashEntry> *entryList_;
            std::mutex slotMtx_;
            HashtableSlot()
            {
                entryList_ = new LinkedList<HashEntry>;
            }
            ~HashtableSlot()
            {
                delete entryList_;
            }
        };

    private:

        void initHashTable();
        void destroyHashTable();

        HashtableSlot *hashtable_;
        uint32_t htSize_;
        uint64_t sizeOndisk_;
        uint32_t keyCounter_;
        uint64_t dataTheorySize_;
        SuperBlockManager* sbMgr_;
        SimpleDS_Impl* dataStor_;
        Options &options_;

        KVTime* lastTime_;
        mutable std::mutex mtx_;
        std::mutex batch_mtx_;

    // Seg Reaper thread
    private:
    class SegmentReaperWQ : public dslab::WorkQueue<SegForReq> {
        public:
            explicit SegmentReaperWQ(IndexManager *im, int thd_num=1) : dslab::WorkQueue<SegForReq>(thd_num), idxMgr_(im) {}

        protected:
            void _process(SegForReq* seg) override {
                idxMgr_->SegReaper(seg);
            }
        private:
            IndexManager *idxMgr_;
        };

    SegmentReaperWQ *segRprWQ_;
    void SegReaper(SegForReq* seg);

    };

}// namespace hlkvds
#endif //#ifndef _HLKVDS_INDEXMANAGER_H_
