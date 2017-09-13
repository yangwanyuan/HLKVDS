#ifndef _HLKVDS_HLKVQUEUE_H_
#define _HLKVDS_HLKVQUEUE_H_

#include <queue>
#include <mutex>
#include <chrono>
#include <iostream>

namespace hlkvds {
template <typename T> class HlkvQueue {
public:
    typedef T QueueType;

    void Enqueue(QueueType _work) {
        std::lock_guard < std::mutex > lck(mtx_);
        dataQueue_.push(_work);
    }

    QueueType Dequeue() {
        std::unique_lock < std::mutex > lck(mtx_);
        if (dataQueue_.empty()) {
            lck.unlock();
            return NULL;
        }

        QueueType data = dataQueue_.front();
        dataQueue_.pop();
        lck.unlock();
        return data;
    }

    void Enqueue_Notify(QueueType _work) {
        std::lock_guard < std::mutex > lck(mtx_);
        dataQueue_.push(_work);
        cv_.notify_one();
    }

    QueueType Wait_Dequeue(int msec = 1000) {
        std::unique_lock < std::mutex > lck(mtx_);
if    (cv_.wait_for(lck, std::chrono::milliseconds(msec), [this] {return !dataQueue_.empty();}))
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

                bool empty() {
                    std::lock_guard<std::mutex> lck(mtx_);
                    return dataQueue_.empty();
                }

                uint32_t length()
                {
                    return dataQueue_.size();
                }

            private:
                std::queue<QueueType> dataQueue_;
                mutable std::mutex mtx_;
                std::condition_variable cv_;

            };
}
#endif //#ifndef _HLKVDS_WORKQUEUE_H_
