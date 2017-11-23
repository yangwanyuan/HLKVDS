#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <unistd.h>

#include <iostream>

#include "IndexManager.h"
#include "Db_Structure.h"
#include "SuperBlockManager.h"
#include "DataStor.h"

using namespace std;

namespace hlkvds {

DataHeader::DataHeader() :
    key_digest(Kvdb_Digest()), key_size(0), data_size(0), data_offset(0),
            next_header_offset(0) {
}

DataHeader::DataHeader(const Kvdb_Digest &digest, uint16_t key_len, uint16_t data_len,
                       uint32_t offset, uint32_t next_offset) :
    key_digest(digest), key_size(key_len), data_size(data_len), data_offset(offset),
            next_header_offset(next_offset) {
}

DataHeader::~DataHeader() {
}

void DataHeader::SetDigest(const Kvdb_Digest& digest) {
    key_digest = digest;
}

DataHeaderAddress::~DataHeaderAddress() {
}

HashEntryOnDisk::HashEntryOnDisk() :
    header(DataHeader()), address(DataHeaderAddress()) {
}

HashEntryOnDisk::HashEntryOnDisk(DataHeader& dataheader,
                                 DataHeaderAddress& addrs) :
    header(dataheader), address(addrs) {
}

HashEntryOnDisk::HashEntryOnDisk(const HashEntryOnDisk& toBeCopied) {
    header = toBeCopied.header;
    address = toBeCopied.address;
}

HashEntryOnDisk::~HashEntryOnDisk() {
}

HashEntryOnDisk& HashEntryOnDisk::operator=(const HashEntryOnDisk& toBeCopied) {
    header = toBeCopied.header;
    address = toBeCopied.address;
    return *this;
}

void HashEntryOnDisk::SetKeyDigest(const Kvdb_Digest& digest) {
    header.SetDigest(digest);
}

HashEntry::HashEntry() :
    cachePtr_(NULL) {
    stampPtr_ = new LogicStamp;
    entryPtr_ = new HashEntryOnDisk;
}

HashEntry::HashEntry(HashEntryOnDisk& entry_ondisk, KVTime time_stamp,
                     void* read_ptr) :
    cachePtr_(read_ptr) {
    stampPtr_ = new LogicStamp(time_stamp, 0);
    entryPtr_ = new HashEntryOnDisk(entry_ondisk);
}

HashEntry::HashEntry(DataHeader& data_header, DataHeaderAddress &addrs,
                     void* read_ptr) :
    cachePtr_(read_ptr) {
    stampPtr_ = new LogicStamp;
    entryPtr_ = new HashEntryOnDisk(data_header, addrs);
}

HashEntry::HashEntry(const HashEntry& toBeCopied) {
    entryPtr_ = new HashEntryOnDisk(*toBeCopied.entryPtr_);
    stampPtr_ = new LogicStamp(*toBeCopied.stampPtr_);
    cachePtr_ = toBeCopied.cachePtr_;
}

HashEntry::~HashEntry() {
    cachePtr_ = NULL;
    if (entryPtr_) {
        delete entryPtr_;
    }
    if (stampPtr_) {
        delete stampPtr_;
    }
    return;
}

bool HashEntry::operator==(const HashEntry& toBeCompare) const {
    if (this->GetKeyDigest() == toBeCompare.GetKeyDigest()) {
        return true;
    }
    return false;
}

HashEntry& HashEntry::operator=(const HashEntry& toBeCopied) {
    *entryPtr_ = *toBeCopied.entryPtr_;
    *stampPtr_ = *toBeCopied.stampPtr_;
    cachePtr_ = toBeCopied.cachePtr_;
    return *this;
}

void HashEntry::SetKeyDigest(const Kvdb_Digest& digest) {
    entryPtr_->SetKeyDigest(digest);
}

void HashEntry::SetLogicStamp(KVTime seg_time, int32_t seg_key_no) {
    stampPtr_->Set(seg_time, seg_key_no);
}

void IndexManager::InitDataStor(DataStor *ds) {
    dataStor_ = ds;
}

void IndexManager::printDynamicInfo() {
    __INFO("\n DB Dynamic information: \n"
            "\t number of entries           : %d\n"
            "\t Entry Theory Data Size      : %ld Bytes\n"
            "\t Segment Reaper Queue Size   : %d",
            keyCounter_, dataTheorySize_,
            GetSegReaperQueSize());
}

void IndexManager::InitMeta(uint32_t ht_size, uint64_t ondisk_size, uint64_t data_theory_size, uint32_t element_num) {
    htSize_ = ht_size;
    sizeOndisk_ = ondisk_size;
    initHashTable();

    //Set dataTheorySize
    dataTheorySize_ = data_theory_size;
    keyCounter_ = element_num;

}
void IndexManager::UpdateMetaToSB() {
    //Update data theory size to superblock
    sbMgr_->SetDataTheorySize(dataTheorySize_);
    sbMgr_->SetEntryCount(keyCounter_);
}

bool IndexManager::Get(char* buff, uint64_t length) {
    if (length != sizeOndisk_) {
        return false;
    }
    char* buf_ptr = buff;

    //Copy TS
    int64_t time_len = KVTime::SizeOf();
    lastTime_->Update();
    time_t t = lastTime_->GetTime();
    memcpy((void *)buf_ptr, (const void*)&t, time_len);
    buf_ptr += time_len;
    __DEBUG("memcpy timestamp: %s at %p, at %ld", KVTime::ToChar(*lastTime_), (void *)buf_ptr, (int64_t)(buf_ptr-buff));

    //Copy Counter Table
    __DEBUG("memcpy Counter at %p, at %ld", (void *)buf_ptr, (int64_t)(buf_ptr-buff));
    for (uint32_t i = 0; i < htSize_; i++) {
        int counter = hashtable_[i].entryList_->get_size();
        memcpy((void *)buf_ptr, (const void*)&counter, sizeof(int));
        buf_ptr += sizeof(int);
    }

    //Copy Index
    __DEBUG("memcpy Index at %p, at %ld", (void *)buf_ptr, (int64_t)(buf_ptr-buff));
    uint64_t entry_len = IndexManager::SizeOfHashEntryOnDisk();
    for (uint32_t i = 0; i < htSize_; i++) {
        int slot_num = hashtable_[i].entryList_->get_size();
        if (slot_num > 0) {
            vector<HashEntry> tmp_vec = hashtable_[i].entryList_->get();
            for (vector<HashEntry>::iterator iter = tmp_vec.begin(); iter != tmp_vec.end(); iter++) {
                memcpy((void *)buf_ptr, (const void*)&(iter->GetEntryOnDisk()), entry_len);
                buf_ptr += entry_len;
            }
        }
    }

    __DEBUG("memcpy complete at %p, at %ld", (void *)buf_ptr, (int64_t)(buf_ptr-buff));

    return true;
}

bool IndexManager::Set(char* buff, uint64_t length) {
    if (length != sizeOndisk_) {
        return false;
    }
    char* buf_ptr = buff;

    //Set TS
    int64_t time_len = KVTime::SizeOf();
    time_t t;
    memcpy((void *)&t, (const void*)buf_ptr, time_len);
    lastTime_->SetTime(t);
    buf_ptr += time_len;

    __DEBUG("memcpy timestamp: %s at %p, at %ld", KVTime::ToChar(*lastTime_), (void *)buf_ptr, (int64_t)(buf_ptr-buff));

    //Set Index
    uint64_t counter_table_len = sizeof(int) * htSize_;
    char * counter_table_ptr = buf_ptr;
    char * ht_ptr = buf_ptr + counter_table_len;

    uint32_t total_entry = 0;
    uint64_t counter_size = sizeof(int);
    uint64_t entry_ondisk_size = IndexManager::SizeOfHashEntryOnDisk();

    for (uint32_t i = 0; i < htSize_; i++) {

        int slot_num = 0;
        memcpy((void*)&slot_num, (const void *)counter_table_ptr, sizeof(int));

        if (slot_num > 0) {
            for (int j = 0; j < slot_num; j++) {
                HashEntryOnDisk entry_ondisk;
                memcpy((void*)&entry_ondisk, (const void *)ht_ptr, entry_ondisk_size);
                HashEntry entry(entry_ondisk, *lastTime_, 0);
                hashtable_[i].entryList_->put(entry);
                total_entry++;
                ht_ptr += entry_ondisk_size;
            }
        }

        counter_table_ptr += counter_size;
    }

    if (total_entry != keyCounter_) {
        __ERROR("The Key Number is conflit between superblock and index!!!!!");
        return false;
    }

    return true;
}

bool IndexManager::UpdateIndex(KVSlice* slice) {
    const Kvdb_Digest *digest = &slice->GetDigest();

    HashEntry entry = slice->GetHashEntry();
    const char* data = slice->GetData();

    uint32_t hash_index = KeyDigestHandle::Hash(digest) % htSize_;

    std::unique_lock<std::mutex> meta_lck(mtx_, std::defer_lock);

    std::lock_guard<std::mutex> l(hashtable_[hash_index].slotMtx_);
    LinkedList<HashEntry> *entry_list = hashtable_[hash_index].entryList_;

    bool is_exist = entry_list->search(entry);
    if (!is_exist) {
        if (data) {
            //It's insert a new entry operation
            meta_lck.lock();
            if (keyCounter_ == htSize_) {
                __WARN("UpdateIndex Failed, because the hashtable is full!");
                return false;
            }
            meta_lck.unlock();

            entry_list->put(entry);

            meta_lck.lock();
            keyCounter_++;
            dataTheorySize_ += SizeOfDataHeader() + slice->GetDataLen();
            meta_lck.unlock();

            __DEBUG("UpdateIndex request, because this entry is not exist! Now dataTheorySize_ is %ld", dataTheorySize_);
        }
        else {
            //It's a invalid delete operation
            dataStor_->ModifyDeathEntry(entry);
            __DEBUG("Ignore the UpdateIndex request, because this is a delete operation but not exist in Memory");
        }
    }
    else {
        HashEntry *entry_inMem = entry_list->getRef(entry);
        HashEntry::LogicStamp *lts = entry.GetLogicStamp();
        HashEntry::LogicStamp *lts_inMem = entry_inMem->GetLogicStamp();

        if ( *lts < *lts_inMem) {
            dataStor_->ModifyDeathEntry(entry);
            __DEBUG("Ignore the UpdateIndex request, because request is expired!");
        }
        else {
            //this operation is need to do
            dataStor_->ModifyDeathEntry(*entry_inMem);

            uint16_t data_size = entry.GetDataSize() ;
            uint16_t data_inMem_size = entry_inMem->GetDataSize();

            meta_lck.lock();
            if (data_size == 0) {
                dataTheorySize_ -= (uint64_t)(SizeOfDataHeader() + data_inMem_size);
            }
            else {
                dataTheorySize_ += ( data_size > data_inMem_size? 
                        ((uint64_t)(data_size - data_inMem_size)) : 
                        -((uint64_t)(data_inMem_size - data_size)) );
            }
            meta_lck.unlock();

            entry_list->put(entry);

            __DEBUG("UpdateIndex request, because request is new than in memory!Now dataTheorySize_ is %ld", dataTheorySize_);
        }
        return true;
    }

    __DEBUG("Update Index: key:%s, data_len:%u, seg_id:%u, data_offset:%u",
            slice->GetKeyStr().c_str(), slice->GetDataLen(),
            slice->GetSegId(), slice->GetHashEntry().GetDataOffsetInSeg());

    return true;
}

void IndexManager::UpdateIndexes(list<KVSlice*> &slice_list) {
    std::lock_guard<std::mutex> l(batch_mtx_);
    for (list<KVSlice *>::iterator iter = slice_list.begin(); iter
            != slice_list.end(); iter++) {
        KVSlice *slice = *iter;
        UpdateIndex(slice);
    } __DEBUG("UpdateToIndex Success!");
}

void IndexManager::RemoveEntry(HashEntry entry) {
    Kvdb_Digest digest = entry.GetKeyDigest();

    uint32_t hash_index = KeyDigestHandle::Hash(&digest) % htSize_;

    std::unique_lock<std::mutex> meta_lck(mtx_, std::defer_lock);
    std::lock_guard<std::mutex> l(hashtable_[hash_index].slotMtx_);
    LinkedList<HashEntry> *entry_list = hashtable_[hash_index].entryList_;

    HashEntry *entry_inMem = entry_list->getRef(entry);
    if (!entry_inMem) {
        __DEBUG("Already remove the index entry");
        return;
    }
    HashEntry::LogicStamp *lts = entry.GetLogicStamp();
    HashEntry::LogicStamp *lts_inMem = entry_inMem->GetLogicStamp();
    KVTime &t = lts->GetSegTime();
    KVTime &t_inMem = lts_inMem->GetSegTime();
    if (t_inMem == t && entry_inMem->GetDataSize() == 0) {
        entry_list->remove(entry);
        dataStor_->ModifyDeathEntry(entry);

        meta_lck.lock();
        keyCounter_--;
        meta_lck.unlock();

        __DEBUG("Remove the index entry!");
    }
}

bool IndexManager::GetHashEntry(KVSlice *slice) {
    const Kvdb_Digest *digest = &slice->GetDigest();
    uint32_t hash_index = KeyDigestHandle::Hash(digest) % htSize_;
    HashEntry entry;
    entry.SetKeyDigest(*digest);

    std::lock_guard<std::mutex> l(hashtable_[hash_index].slotMtx_);
    LinkedList<HashEntry> *entry_list = hashtable_[hash_index].entryList_;

    if (entry_list->search(entry)) {
        vector<HashEntry> tmp_vec = entry_list->get();
        for (vector<HashEntry>::iterator iter = tmp_vec.begin(); iter!=tmp_vec.end(); iter++) {
            if (iter->GetKeyDigest() == *digest) {
                entry = *iter;
                slice->SetHashEntry(&entry);
                __DEBUG("IndexManger: entry : header_offset = %lu, data_offset = %u, next_header=%u",
                        entry.GetHeaderOffset(), entry.GetDataOffsetInSeg(),
                        entry.GetNextHeadOffsetInSeg());
                return true;
            }
        }
    }
    return false;
}

uint64_t IndexManager::GetDataTheorySize() const {
    std::lock_guard<std::mutex> l(mtx_);
    return dataTheorySize_;
}

uint32_t IndexManager::GetKeyCounter() const {
    std::lock_guard<std::mutex> l(mtx_);
    return keyCounter_;
}

bool IndexManager::IsSameInMem(HashEntry entry)
{
    Kvdb_Digest digest = entry.GetKeyDigest();
    uint32_t hash_index = KeyDigestHandle::Hash(&digest) % htSize_;

    std::lock_guard<std::mutex> l(hashtable_[hash_index].slotMtx_);
    LinkedList<HashEntry> *entry_list = hashtable_[hash_index].entryList_;

    bool is_exist = entry_list->search(entry);
    if (!is_exist) {
        __DEBUG("Not Same, because entry is not exist!");
        return false;
    } else {
        HashEntry *entry_inMem = entry_list->getRef(entry);
        __DEBUG("the entry header_offset = %ld, in memory entry header_offset=%ld", entry.GetHeaderOffset(), entry_inMem->GetHeaderOffset());
        if (entry_inMem->GetHeaderOffset() == entry.GetHeaderOffset()) {
            __DEBUG("Same, because entry is same with in memory!");
            return true;
        }
    }

    __DEBUG("Not Same, because entry is not same with in memory!");
    return false;
}

uint64_t IndexManager::CalcIndexSizeOnDevice(uint32_t ht_size) {
    uint64_t index_size = sizeof(time_t)
            + sizeof(int) * ht_size
            + IndexManager::SizeOfHashEntryOnDisk() * ht_size;
    uint64_t index_size_pages = index_size / getpagesize();
    return (index_size_pages + 1) * getpagesize();
}

IndexManager::IndexManager(SuperBlockManager* sbm, Options &opt) :
    hashtable_(NULL), htSize_(0), sizeOndisk_(0), keyCounter_(0), dataTheorySize_(0),
            sbMgr_(sbm), dataStor_(NULL),
            options_(opt), segRprWQ_(NULL) {
    lastTime_ = new KVTime();
    return;
}

IndexManager::~IndexManager() {
    if (lastTime_) {
        delete lastTime_;
    }
    if (hashtable_) {
        destroyHashTable();
    }
}

uint32_t IndexManager::CalcHashSizeForPower2(uint32_t number) {
    // Gets the next highest power of 2 larger than number
    number--;
    number = (number >> 1) | number;
    number = (number >> 2) | number;
    number = (number >> 4) | number;
    number = (number >> 8) | number;
    number = (number >> 16) | number;
    number++;
    return number;
}

void IndexManager::initHashTable() {
    hashtable_ = new HashtableSlot[htSize_];
    return;
}

void IndexManager::destroyHashTable() {
    delete[] hashtable_;
    hashtable_ = NULL;
    return;
}

void IndexManager::StartThds(){
    segRprWQ_ = new SegmentReaperWQ(this, 1);
    segRprWQ_->Start();
}

void IndexManager::StopThds(){
    if (segRprWQ_) {
        segRprWQ_->Stop();
        delete segRprWQ_;
    }
}

void IndexManager::SegReaper(SegForReq *seg) {
    seg->CleanDeletedEntry();
    __DEBUG("Segment reaper delete seg_id = %d", seg->GetSegId());
    delete seg;
}

void IndexManager::AddToReaper(SegForReq* seg) {
    segRprWQ_->Add_task(seg);
}

}

