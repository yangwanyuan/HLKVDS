#ifndef WORKQUEUE_H
#define WORKQUEUE_H

#include <queue>
#include <mutex>
#include <iostream>

namespace kvdb {
template <typename T> class WorkQueue{
public:
    typedef T queue_type;
    std::queue<queue_type> _queue;
    std::mutex _queue_lock;

    void enqueue( queue_type _work ){
        std::lock_guard<std::mutex> guard(_queue_lock);
        this->_queue.push( _work );
    }

    queue_type dequeue(){
        _queue_lock.lock();
        if(this->_queue.empty()){
            _queue_lock.unlock();
            return NULL;
        }

        queue_type data = this->_queue.front();
        this->_queue.pop();
        _queue_lock.unlock();
        return data;
    }

    bool empty(){
        std::lock_guard<std::mutex> guard(_queue_lock);
        return this->_queue.empty();
    }
};
}
#endif
