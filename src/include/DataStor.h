#ifndef _HLKVDS_DATASTOR_H_
#define _HLKVDS_DATASTOR_H_

#include <string>
#include <vector>

#include "WorkQueue.h"

namespace hlkvds {

static const int DevPathLenLimt = 20;

class BlockDevice;
class SuperBlockManager;
class IndexManager;

class Options;
class Status;
class WriteBatch;

class KVSlice;
class HashEntry;

class Request;
class SegForReq;

class DataStor {
public:
    DataStor();
    virtual ~DataStor();

    // Called by Kvdb_Impl
    static DataStor* Create(Options &opts, std::vector<BlockDevice*> &dev_vec, SuperBlockManager* sb, IndexManager* idx, int datastor_type);

    virtual int GetDataStorType() = 0;
    virtual void CreateAllSegments() = 0;
    virtual void StartThds() = 0;
    virtual void StopThds() = 0;

    virtual void printDeviceTopologyInfo() = 0;
    virtual void printDynamicInfo() = 0;

    virtual Status WriteData(KVSlice& slice) = 0;
    virtual Status WriteBatchData(WriteBatch *batch) =0;
    virtual Status ReadData(KVSlice &slice, std::string &data) = 0;

    virtual void ManualGC() = 0;

    // Called by MetaStor
    virtual bool GetSBReservedContent(char* buf, uint64_t length) = 0;
    virtual bool SetSBReservedContent(char* buf, uint64_t length) = 0;

    virtual bool GetAllSSTs(char* buf, uint64_t length) = 0;
    virtual bool SetAllSSTs(char* buf, uint64_t length) = 0;

    virtual bool CreateAllVolumes(uint64_t sst_offset) = 0;
    virtual bool OpenAllVolumes() = 0;

    virtual uint32_t GetTotalSegNum() = 0;
    virtual uint64_t GetSSTsLengthOnDisk() = 0;

    // Called by IndexManager
    virtual void ModifyDeathEntry(HashEntry &entry) = 0;

    // Called by Iterator
    virtual std::string GetKeyByHashEntry(HashEntry *entry) = 0;
    virtual std::string GetValueByHashEntry(HashEntry *entry) = 0;

    // Request Merge WorkQueue
protected:
    class ReqsMergeWQ : public dslab::WorkQueue<Request> {
    public:
        explicit ReqsMergeWQ(DataStor *ds, int thd_num=1) : dslab::WorkQueue<Request>(thd_num), ds_(ds) {}
    protected:
        void _process(Request* req) override {
            ds_->ReqMerge(req);
        }
    private:
        DataStor *ds_;
    };
    std::vector<ReqsMergeWQ *> reqWQVec_;
    virtual void ReqMerge(Request* req) = 0;

    // Seg Write to device WorkQueue
protected:
    class SegmentWriteWQ : public dslab::WorkQueue<SegForReq> {
    public:
        explicit SegmentWriteWQ(DataStor *ds, int thd_num=1) : dslab::WorkQueue<SegForReq>(thd_num), ds_(ds) {}

    protected:
        void _process(SegForReq* seg) override {
            ds_->SegWrite(seg);
        }
    private:
        DataStor *ds_;
    };
    SegmentWriteWQ * segWteWQ_;
    virtual void SegWrite(SegForReq* req) = 0;

    // Seg Timeout thread
protected:
    std::thread segTimeoutT_;
    std::atomic<bool> segTimeoutT_stop_;
    virtual void SegTimeoutThdEntry() = 0;
};

}// namespace hlkvds

#endif //#ifndef _HLKVDS_DATASTOR_H_
