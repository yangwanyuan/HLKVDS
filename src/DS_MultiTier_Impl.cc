#include <math.h>
#include "DataStor.h"
#include "DS_MultiTier_Impl.h"
#include "Db_Structure.h"
#include "BlockDevice.h"
#include "SuperBlockManager.h"
#include "IndexManager.h"
#include "Volumes.h"
#include "SegmentManager.h"

using namespace std;

namespace hlkvds {

DS_MultiTier_Impl::DS_MultiTier_Impl(Options& opts, vector<BlockDevice*> &dev_vec,
                            SuperBlockManager* sb, IndexManager* idx) :
        options_(opts), bdVec_(dev_vec), sbMgr_(sb), idxMgr_(idx) {
}

DS_MultiTier_Impl::~DS_MultiTier_Impl() {
}

void DS_MultiTier_Impl::CreateAllSegments() {
}

void DS_MultiTier_Impl::StartThds() {
}

void DS_MultiTier_Impl::StopThds() {
}

void DS_MultiTier_Impl::printDeviceTopologyInfo() {
}

void DS_MultiTier_Impl::printDynamicInfo() {
}

Status DS_MultiTier_Impl::WriteData(KVSlice& slice) {
    Status s;
    return s;
}

Status DS_MultiTier_Impl::WriteBatchData(WriteBatch *batch) {
    Status s;
    return s;
}

Status DS_MultiTier_Impl::ReadData(KVSlice &slice, std::string &data) {
    Status s;
    return s;
}

void DS_MultiTier_Impl::ManualGC() {
}

bool DS_MultiTier_Impl::GetSBReservedContent(char* buf, uint64_t length) {
    return true;
}

bool DS_MultiTier_Impl::SetSBReservedContent(char* buf, uint64_t length) {
    return true;
}

bool DS_MultiTier_Impl::GetAllSSTs(char* buf, uint64_t length) {
    return true;
}

bool DS_MultiTier_Impl::SetAllSSTs(char* buf, uint64_t length) {
    return true;
}

bool DS_MultiTier_Impl::CreateAllVolumes(uint64_t sst_offset) {
    return true;
}

bool DS_MultiTier_Impl::OpenAllVolumes() {
    return true;
}

void DS_MultiTier_Impl::ModifyDeathEntry(HashEntry &entry) {
}

std::string DS_MultiTier_Impl::GetKeyByHashEntry(HashEntry *entry) {
    return "";
}

std::string DS_MultiTier_Impl::GetValueByHashEntry(HashEntry *entry) {
    return "";
}

void DS_MultiTier_Impl::ReqMerge(Request* req) {
}

void DS_MultiTier_Impl::SegWrite(SegForReq* seg) {
}

void DS_MultiTier_Impl::SegTimeoutThdEntry() {
}

} // namespace hlkvds
