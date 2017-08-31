#include "KvdbIter.h"
#include "IndexManager.h"
#include "SegmentManager.h"
#include "BlockDevice.h"
#include "Db_Structure.h"

#ifdef WITH_ITERATOR
namespace hlkvds {

KvdbIter::KvdbIter(IndexManager* im, SegmentManager* sm, BlockDevice* bdev) :
    idxMgr_(im), segMgr_(sm), bdev_(bdev), valid_(false), hashEntry_(NULL){
        //ht_ = idxMgr_->GetHashTable();
        htSize_ = idxMgr_->GetHashTableSize();
}

KvdbIter::~KvdbIter() {
    valid_ = false;
}

void KvdbIter::SeekToFirst() {
    LinkedList<HashEntry> *entry_list;
    hashEntry_ = NULL;
    htSize_ = idxMgr_->GetHashTableSize();
    for (int i = 0; i < htSize_; i++) {
        //entry_list = ht_[i].entryList_;
        entry_list = idxMgr_->GetEntryListByNo(i);
        int entry_list_size = entry_list->get_size();
        if (entry_list_size > 0) {
            hashTableCur_ = i;
            entryListCur_ = 0;
            hashEntry_ = entry_list->getByNo(entryListCur_);
            break;
        }
    }

    if (NULL != hashEntry_) {
        valid_ = true;
    } else {
        valid_ = false;
    }

    //status_ = Stats::OK()
}

void KvdbIter::SeekToLast() {
    LinkedList<HashEntry> *entry_list;
    hashEntry_ = NULL;
    htSize_ = idxMgr_->GetHashTableSize();
    //__INFO("htSize_=%d",htSize_);
    for (int i = htSize_ - 1; i >= 0; i--) {
        //entry_list = ht_[i].entryList_;
        entry_list = idxMgr_->GetEntryListByNo(i);
        int entry_list_size = entry_list->get_size();
        //__INFO("entry_list_size=%d",entry_list_size);
        if (entry_list_size > 0) {
            hashTableCur_ = i;
            entryListCur_ = entry_list_size - 1;
            hashEntry_ = entry_list->getByNo(entryListCur_);
            break;
        }
    }

    if (NULL != hashEntry_) {
        valid_ = true;
    } else {
        valid_ = false;
    }
    //__INFO("hastTableCur_ = %d, entryListCur_ = %d", hashTableCur_, entryListCur_);

}

void KvdbIter::Seek(const char* key) {
    //need hashEntry;
    int key_len = strlen(key);
    KVSlice slice(key, key_len, NULL, 0);
    DataHeader data_header;
    data_header.SetDigest(slice.GetDigest());
    HashEntry entry(data_header, 0, NULL);
    
    LinkedList<HashEntry> *entry_list;
    hashEntry_ = NULL;
    for (int i = 0; i < htSize_; i++) {
        //entry_list = ht_[i].entryList_;
        entry_list = idxMgr_->GetEntryListByNo(i);
        hashEntry_ = entry_list->getRef(entry);
        if (NULL != hashEntry_) {
            hashTableCur_ = i;
            entryListCur_ = entry_list->searchNo(entry);
            break;
        }
    }

    if (NULL != hashEntry_) {
        valid_ = true;
    } else {
        valid_ = false;
    }
}

void KvdbIter::Next() {
    LinkedList<HashEntry> *entry_list;
    //entry_list = ht_[hashTableCur_];
    entry_list = idxMgr_->GetEntryListByNo(hashTableCur_);
    hashEntry_ = NULL;
    int entry_list_size = entry_list->get_size();
    if ( entryListCur_ < entry_list_size - 1) {
        entryListCur_++;
        hashEntry_ = entry_list->getByNo(entryListCur_);
    } else {
        hashTableCur_++;
        while (hashTableCur_ < htSize_) {
            //entry_list = ht_[i].entryList_;
            entry_list = idxMgr_->GetEntryListByNo(hashTableCur_);
            entry_list_size = entry_list->get_size();
            if (entry_list_size  > 0) {
                entryListCur_ = 0;
                hashEntry_ = entry_list->getByNo(entryListCur_);
                break;
            }
            hashTableCur_++;
        }
    }

    if (NULL != hashEntry_) {
        valid_ = true;
    } else {
        valid_ = false;
    }
    //__INFO("hastTableCur_ = %d, entryListCur_ = %d", hashTableCur_, entryListCur_);
}

void KvdbIter::Prev() {
    LinkedList<HashEntry> *entry_list;
    //entry_list = ht_[hashTableCur_];
    entry_list = idxMgr_->GetEntryListByNo(hashTableCur_);
    hashEntry_ = NULL;
    int entry_list_size = entry_list->get_size();
    if (entryListCur_ > 0) {
        entryListCur_--;
        hashEntry_ = entry_list->getByNo(entryListCur_);
    } else {
        hashTableCur_--;
        while (hashTableCur_ >= 0) {
            //entry_list = ht_[i].entryList_;
            entry_list = idxMgr_->GetEntryListByNo(hashTableCur_);
            entry_list_size = entry_list->get_size();
            if ( entry_list_size > 0) {
                entryListCur_ = entry_list_size - 1;
                hashEntry_ = entry_list->getByNo(entryListCur_);
                break;
            }
            hashTableCur_--;
        }
    }

    if (NULL != hashEntry_) {
        valid_ = true;
    } else {
        valid_ = false;
    }
    //__INFO("hastTableCur_ = %d, entryListCur_ = %d", hashTableCur_, entryListCur_);
}

string KvdbIter::Key() {
    if (!valid_) {
        return "";
    }

    uint64_t key_offset = 0;
    if (!segMgr_->ComputeKeyOffsetPhyFromEntry(hashEntry_, key_offset)) {
        return "";
    }
    __DEBUG("key offset: %lu",key_offset);
    uint16_t key_len = hashEntry_->GetKeySize();
    char *mkey = new char[key_len+1];
    if (bdev_->pRead(mkey, key_len, key_offset) != (ssize_t) key_len) {
        __ERROR("Could not read data at position");
        delete[] mkey;
        return "";
    }
    mkey[key_len] = '\0';
    string res(mkey, key_len);
    //__INFO("Iterator key is %s, key_offset = %lu, key_len = %u", mkey, key_offset, key_len);
    delete[] mkey;
   
    return res;
}

string KvdbIter::Value() {
    if (!valid_) {
        return "";
    }

    uint64_t data_offset = 0;
    if (!segMgr_->ComputeDataOffsetPhyFromEntry(hashEntry_, data_offset)) {
        return "";
    }

    __DEBUG("data offset: %lu",data_offset);
    uint16_t data_len = hashEntry_->GetDataSize();
    if ( data_len ==0 ) {
        return "";
    }
    char *mdata = new char[data_len+1];
    if (bdev_->pRead(mdata, data_len, data_offset) != (ssize_t) data_len) {
        __ERROR("Could not read data at position");
        delete[] mdata;
        return "";
    }
    mdata[data_len]= '\0';
    string res(mdata, data_len);
    //__INFO("Iterator value is %s, data_offset = %lu, data_len = %u", mdata, data_offset, data_len);
    delete[] mdata;
   
    return res;
}

bool KvdbIter::Valid() const {
    return valid_;
}

Status KvdbIter::status() const {
    return status_;
}

}

#endif
