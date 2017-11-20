#include <stdlib.h>
#include <boost/algorithm/string.hpp>

#include "Kvdb_Impl.h"
#include "KvdbIter.h"
#include "Db_Structure.h"

#include "BlockDevice.h"
#include "KernelDevice.h"
#include "SuperBlockManager.h"
#include "IndexManager.h"
#include "MetaStor.h"
#include "DataStor.h"

using namespace std;
using namespace dslab;

namespace hlkvds {

KVDS* KVDS::Create_KVDS(const char* filename, Options opts) {
    if (filename == NULL) {
        return NULL;
    }

    KVDS* kvds = new KVDS(filename, opts);


    if (!kvds->openAllDevices(kvds->paths_)) {
        delete kvds;
        return NULL;
    }

    // TODO:Find DataStor Type;
    // ...
    int datastor_type = opts.datastor_type;
    switch (datastor_type) {
        case 0:
            kvds->dataStor_ = new SimpleDS_Impl(kvds->options_, kvds->bdVec_, kvds->sbMgr_, kvds->idxMgr_);
            break;
        case 1:
            __ERROR("Multi_Volume has not been implement yet!");
            break;
        case 2:
            __ERROR("Multi_Tier has not been implement yet!");
            break;
        default:
            __ERROR("UnKnow DataStor Type!");
            break;
    }
    if (kvds->dataStor_ == NULL) {
        delete kvds;
        return NULL;
    }

    kvds->idxMgr_->InitDataStor(kvds->dataStor_);
    kvds->metaStor_->InitDataStor(kvds->dataStor_);

    if (!kvds->metaStor_->CreateMetaData()) {
        delete kvds;
        return NULL;
    }

    __INFO("\nCreateKVDS Success!!!\n");
    kvds->printDbStates();

    kvds->dataStor_->InitSegment();
    kvds->startThds();

    return kvds;

}

Iterator* KVDS::NewIterator() {
    return new KvdbIter(idxMgr_, dataStor_);
}

void KVDS::printDbStates() {

    uint32_t index_ht_size          = sbMgr_->GetHTSize();
    uint64_t index_region_offset    = sbMgr_->GetIndexRegionOffset();
    uint64_t index_region_length    = sbMgr_->GetIndexRegionLength();

    uint32_t sst_total_num          = sbMgr_->GetSSTTotalNum();
    uint64_t sst_region_offset      = sbMgr_->GetSSTRegionOffset();
    uint64_t sst_region_length      = sbMgr_->GetSSTRegionLength();

    uint32_t data_store_type        = sbMgr_->GetDataStoreType();

    uint32_t entry_count            = sbMgr_->GetEntryCount();
    uint64_t entry_theory_data_size = sbMgr_->GetDataTheorySize();

    __INFO("\n DB Static information:\n"
            "\t index hashtable size        : %d\n"
            "\t index region offset         : %ld\n"
            "\t index region length         : %ld Bytes\n"
            "\t sst total segment num       : %d\n"
            "\t sst region offset           : %ld\n"
            "\t sst region length           : %ld Bytes\n"
            "\t data store type             : %d",
            index_ht_size, index_region_offset,
            index_region_length, sst_total_num,
            sst_region_offset, sst_region_length,
            data_store_type);

    __INFO("\n DB Dynamic information: \n"
            "\t number of entries           : %d\n"
            "\t Entry Theory Data Size      : %ld Bytes\n"
            "\t Request Queue Size          : %d\n"
            "\t Segment Write Queue Size    : %d\n"
            "\t Segment Reaper Queue Size   : %d",
            entry_count, entry_theory_data_size,
            dataStor_->GetReqQueSize(),
            dataStor_->GetSegWriteQueSize(),
            idxMgr_->GetSegReaperQueSize());

    dataStor_->printDeviceTopology();
}

KVDS* KVDS::Open_KVDS(const char* filename, Options opts) {
    KVDS *kvds = new KVDS(filename, opts);

    Status s = kvds->openDB();
    if (!s.ok()) {
        delete kvds;
        return NULL;
    }
    return kvds;

}

Status KVDS::openDB() {
    if (!openAllDevices(paths_)) {
        return Status::IOError("Could not open all device");
    }

    // TODO:Find DataStor Type;
    // ...
    int datastor_type = -1;
    if ( !metaStor_->TryLoadSB(datastor_type)) {
        return  Status::IOError("Could not load SB");
    }
    switch (datastor_type) {
        case 0:
            dataStor_ = new SimpleDS_Impl(options_, bdVec_, sbMgr_, idxMgr_);
            break;
        case 1:
            __ERROR("Multi_Volume has not been implement yet!");
            break;
        case 2:
            __ERROR("Multi_Tier has not been implement yet!");
            break;
        default:
            __ERROR("UnKnow DataStor Type!");
            break;
    }
    if (dataStor_ == NULL) {
        return Status::NotSupported("Could not support this data store");
    }

    idxMgr_->InitDataStor(dataStor_);
    metaStor_->InitDataStor(dataStor_);

    if (!metaStor_->LoadMetaData()) {
        return Status::IOError("Could not read meta data");
    }

    printDbStates();

    dataStor_->InitSegment();
    startThds();
    return Status::OK();
}

Status KVDS::closeDB() {
    if (!metaStor_->PersistMetaData()) {
        __ERROR("Could not to write metadata to device\n");
        return Status::IOError("Could not to write metadata to device");
    }
    stopThds();
    return Status::OK();
}

void KVDS::startThds() {
    idxMgr_->StartThds();
    dataStor_->StartThds();
}

void KVDS::stopThds() {
    dataStor_->StopThds();
    idxMgr_->StopThds();
}

KVDS::~KVDS() {
    closeDB();
    delete idxMgr_;
    delete sbMgr_;
    if(!options_.disable_cache){
        delete rdCache_;
    }
    delete metaStor_;
    if (dataStor_) {
        delete dataStor_;
    }
    closeAllDevices();

}

bool KVDS::openAllDevices(string paths) {

    vector<string> fields;
    //string buf(paths);
    //boost::trim_if(buf, boost::is_any_of(FileDelim));
    //boost::split(fields, buf, boost::is_any_of(FileDelim));
    boost::split(fields, paths, boost::is_any_of(FileDelim));
    vector<string>::iterator iter;
    for(iter = fields.begin(); iter != fields.end(); iter++){
        BlockDevice *bdev = new KernelDevice();

        if (bdev->Open(*iter) < 0) {
            return false;
        }__DEBUG("Open Device %s Success!", (*iter).c_str());

        bdVec_.push_back(bdev);
    }
    return true;
}

void KVDS::closeAllDevices() {
    vector<BlockDevice *>::iterator iter;
    for ( iter = bdVec_.begin() ; iter != bdVec_.end(); ) {
        BlockDevice *bdev = *iter;
        delete bdev;
        iter = bdVec_.erase(iter);
    }
}

KVDS::KVDS(const char* filename, Options opts) :
    paths_(string(filename)), sbMgr_(NULL), idxMgr_(NULL), rdCache_(NULL), metaStor_(NULL), dataStor_(NULL), options_(opts) {

    sbMgr_ = new SuperBlockManager(options_);
    idxMgr_ = new IndexManager(sbMgr_, options_);

    if(!options_.disable_cache){
        rdCache_ = new ReadCache(CachePolicy(options_.cache_policy), (size_t) options_.cache_size, options_.slru_partition);
    }

    metaStor_ = new MetaStor(filename, bdVec_, sbMgr_, idxMgr_, options_);
}

Status KVDS::Insert(const char* key, uint32_t key_len, const char* data,
                      uint16_t length) {
    if (key == NULL || key[0] == '\0') {
        return Status::InvalidArgument("Key is null or empty.");
    }

    if (length > dataStor_->GetMaxValueLength()) {
        return Status::NotSupported("Data length cann't be longer than max segment size");
    }

    KVSlice slice(key, key_len, data, length);

    Status s = dataStor_->WriteData(slice);

    if (s.ok()) {
        if(!options_.disable_cache && !slice.GetDataLen()){
            rdCache_->Put(slice.GetKeyStr(),slice.GetDataStr());
        }
    }

    return s;
}

Status KVDS::Delete(const char* key, uint32_t key_len) {
    return Insert(key, key_len, NULL, 0);
}

Status KVDS::Get(const char* key, uint32_t key_len, string &data) {
    bool res = false;
    if (key == NULL) {
        return Status::InvalidArgument("Key is null.");
    }

    KVSlice slice(key, key_len, NULL, 0);

    if(!options_.disable_cache) {
        if(rdCache_->Get(slice.GetKeyStr(), data)) {
            rdCache_->Put(slice.GetKeyStr(), data);
            return Status::OK();
        }
    }

    res = idxMgr_->GetHashEntry(&slice);
    if (!res) {
        //The key is not exist
        return Status::NotFound("Key is not found.");
    }

    Status s = dataStor_->ReadData(slice, data);

    if (s.ok()) {
        if(!options_.disable_cache) {
            rdCache_->Put(slice.GetKeyStr(), data);
        }
    }

    return s;
}

Status KVDS::InsertBatch(WriteBatch *batch) {
    if (batch->batch_.empty()) {
        return Status::OK();
    }

    Status s = dataStor_->WriteBatchData(batch);

    if (s.ok()) {
        if(!options_.disable_cache) {
            for (std::list<KVSlice *>::iterator iter = batch->batch_.begin();
                    iter != batch->batch_.end(); iter++) {
                if(!(*iter)->GetDataLen()) {//no "" should be put in to the cache
                    rdCache_->Put((*iter)->GetKeyStr(), (*iter)->GetDataStr());
                }
            }
        }
    }

    return s;
}

void KVDS::Do_GC() {
    dataStor_->Do_GC();
}

void KVDS::ClearReadCache() {
    vector<BlockDevice*>::iterator iter;
    for (iter = bdVec_.begin(); iter != bdVec_.end(); iter++) {
        BlockDevice *bdev = *iter;
        bdev->ClearReadCache();
    }
}

}

