#ifndef _KV_DB_ITERATOR_H_
#define _KV_DB_ITERATOR_H_

#include <string>
#include "hlkvds/Status.h"

namespace kvdb {

class Iterator {
public:
    Iterator() {}
    virtual ~Iterator() {}

    virtual void SeekToFirst() = 0;
    virtual void SeekToLast() = 0;
    virtual void Seek(const char* key) = 0;
    virtual void Next() = 0;
    virtual void Prev() = 0;

    virtual std::string Key() = 0;
    virtual std::string Value() = 0;

    virtual bool Valid() const = 0;
    virtual Status status() const = 0;

private:
    Iterator(const Iterator&);
    void operator=(const Iterator&);
    
};

}
#endif //#ifndef _KV_DB_ITERATOR_H_
