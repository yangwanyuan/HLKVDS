#ifndef WORKQUEUE_H
#define WORKQUEUE_H

#include <queue>
#include <mutex>
#include <chrono>
#include <iostream>

namespace kvdb {
template <typename T> class WorkQueue{
public:
    typedef T QueueType;

    void Enqueue( QueueType _work )
    {
        std::lock_guard<std::mutex> lck(mtx_);
        dataQueue_.push( _work );
    }

    QueueType Dequeue()
    {
        std::unique_lock<std::mutex> lck(mtx_);
        if(dataQueue_.empty()){
            lck.unlock();
            return NULL;
        }

        QueueType data = dataQueue_.front();
        dataQueue_.pop();
        lck.unlock();
        return data;
    }

    void Enqueue_Notify( QueueType _work )
    {
        std::lock_guard<std::mutex> lck(mtx_);
        dataQueue_.push( _work );
        cv_.notify_one();
    }

    QueueType Wait_Dequeue()
    {
        std::unique_lock<std::mutex> lck(mtx_);
        if(cv_.wait_for(lck, std::chrono::milliseconds(10), [this]{ return !dataQueue_.empty();}))
        {
            QueueType data = dataQueue_.front();
            dataQueue_.pop();
            return data;
        }
        else
        {
            return NULL;
        }
    }

    bool empty(){
        std::lock_guard<std::mutex> guard(mtx_);
        return dataQueue_.empty();
    }


private:
    std::queue<QueueType> dataQueue_;
    std::mutex mtx_;
    std::condition_variable cv_;

};
}
#endif
