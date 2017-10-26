#ifndef _WORKQUEUE_H_
#define _WORKQUEUE_H_

#include <queue>
#include <mutex>
#include <chrono>
#include <condition_variable>                                                                         
#include <thread>
#include <atomic>

/* WorkQueue base on product, need instantiation before use
 * example : 
 *
 *   class IntQueue : public MyQueue<int> {
 *   public:
 *       explicit IntQueue(int thd_num=1) : MyQueue<int>(thd_num) {}

 *   protected:
 *       void _process(int* data) override { return; } 
 *   };

 *   IntQueue *mq = new IntQueue(2);
 *   mq->Start();
 *   mq->AddTask(1);
 *   mq->AddTask(2);
 *   mq->Stop();
 *   delete mq;
 *
 */
namespace dslab {
template <typename T> class WorkQueue {
public:

    void Add_task(T* _work) {
        std::lock_guard < std::mutex > lck(mtx_);
        dataQueue_.push(std::move(_work));
        cv_.notify_one();
    }

    WorkQueue(int thd_num=1) : thdNum_(thd_num), stop_(false) {}
    virtual ~WorkQueue() {}

    void Start() {
        for (int i = 0; i < thdNum_; i++) {
            threads_.push_back(std::move(std::thread(&WorkQueue::worker, this)));
        }
    }

    void Stop() {
        stop_.store(true);
        cv_.notify_all();
        for (auto &t : threads_) {
            t.join();
        }
    }

    void _void_process(T* p) {
        _process(static_cast<T*>(p));
    }

    int Size() {
        std::lock_guard < std::mutex > lck(mtx_);
        return dataQueue_.size();
    }

protected:
    virtual void _process(T* p) = 0;

private:
    void worker()
    {
        while(1) {
            std::unique_lock<std::mutex> lck(mtx_, std::defer_lock);
            lck.lock();
            cv_.wait_for(lck, std::chrono::milliseconds(1000), [this] {return !dataQueue_.empty();});

            if (this->stop_.load() && this->dataQueue_.empty()) {
                lck.unlock();
                return;
            }
                
            if  (!this->dataQueue_.empty()) {
                T* data = dataQueue_.front();
                dataQueue_.pop();
                lck.unlock();
                _void_process(data);
            }
            else {
                lck.unlock();
            }
        }
    }


private:
    std::queue<T*> dataQueue_;
    std::mutex mtx_;
    std::condition_variable cv_;
    std::vector<std::thread> threads_;
    int thdNum_;
    std::atomic<bool> stop_;

};
}
#endif //#ifndef _HLKVDS_WORKQUEUE_H_
