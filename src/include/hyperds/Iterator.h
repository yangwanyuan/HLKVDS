//  Copyright (c) 2017-present, Intel Corporation.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.

//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.

#ifndef _KV_DB_ITERATOR_H_
#define _KV_DB_ITERATOR_H_

#include <string>
#include "Db_Structure.h"
#include "hyperds/Status.h"

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
