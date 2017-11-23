#include "KvdbIter.h"
#include "DataStor.h"
#include "IndexManager.h"
#include "Db_Structure.h"

using namespace std;

namespace hlkvds {

KvdbIter::KvdbIter(IndexManager* im, DataStor* ds) :
    idxMgr_(im), dataStor_(ds), valid_(false), hashEntry_(NULL){
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
    for (int i = htSize_ - 1; i >= 0; i--) {
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
    DataHeaderAddress addrs;
    HashEntry entry(data_header, addrs, NULL);
    
    LinkedList<HashEntry> *entry_list;
    hashEntry_ = NULL;
    for (int i = 0; i < htSize_; i++) {
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
    entry_list = idxMgr_->GetEntryListByNo(hashTableCur_);
    hashEntry_ = NULL;
    int entry_list_size = entry_list->get_size();
    if ( entryListCur_ < entry_list_size - 1) {
        entryListCur_++;
        hashEntry_ = entry_list->getByNo(entryListCur_);
    } else {
        hashTableCur_++;
        while (hashTableCur_ < htSize_) {
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
    entry_list = idxMgr_->GetEntryListByNo(hashTableCur_);
    hashEntry_ = NULL;
    int entry_list_size = entry_list->get_size();
    if (entryListCur_ > 0) {
        entryListCur_--;
        hashEntry_ = entry_list->getByNo(entryListCur_);
    } else {
        hashTableCur_--;
        while (hashTableCur_ >= 0) {
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

    return dataStor_->GetKeyByHashEntry(hashEntry_);
}

string KvdbIter::Value() {
    if (!valid_) {
        return "";
    }

    return dataStor_->GetValueByHashEntry(hashEntry_);
}

bool KvdbIter::Valid() const {
    return valid_;
}

Status KvdbIter::status() const {
    return status_;
}

}
