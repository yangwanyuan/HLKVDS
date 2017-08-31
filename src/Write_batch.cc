#include "Segment.h"
#include "hlkvds/Write_batch.h"
#include "Db_Structure.h"

namespace hlkvds {
WriteBatch::WriteBatch() {}

WriteBatch::~WriteBatch() {
    clear();
}

void WriteBatch::put(const char *key, uint32_t key_len, const char* data,
                    uint16_t length) {
    KVSlice *slice = new KVSlice(key, key_len, data, length, true);
    batch_.push_back(slice);
}

void WriteBatch::del(const char *key, uint32_t key_len) {
    KVSlice *slice = new KVSlice(key, key_len, NULL, 0);
    batch_.push_back(slice);
}
void WriteBatch::clear() {
    while(!batch_.empty()) {
        KVSlice *slice = batch_.front();
        batch_.pop_front();
        delete slice;
    }
}

}
