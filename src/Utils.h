#ifndef _KV_DB_UTILS_H_
#define _KV_DB_UTILS_H_

#include <time.h>
#include <inttypes.h>
#include <sys/types.h>
#include <pthread.h>
#include <signal.h>
#include "Db_Structure.h"

namespace kvdb{
    class Timing{
    public:
        static size_t GetTimeSizeOf(){ return sizeof(time_t); }
        static char* TimeToChar(Timing& _time);
        static time_t GetNow();
        static char* GetNowChar();

        void SetTime(time_t _time);
        time_t GetTime();
        void Update();

        bool IsLate(Timing& time_now);

        Timing();
        Timing(uint64_t _time);
        ~Timing();
    private:
        time_t m_time_stamp;
    };

    class Thread
    {
    public:
        Thread();
        virtual ~Thread();

        int Start();
        int Join();
        int Detach();
        bool Is_started() const;
        bool Am_self() const;
        pthread_t Self();

        virtual void* Entry() = 0;


    protected:
        pthread_t m_tid;
        int m_running;
        int m_detached;

        static void* runThread(void* arg);
    };

    class Mutex
    {
    public:
        Mutex() { pthread_mutex_init(&m_mutex, NULL); }
        virtual ~Mutex() { pthread_mutex_destroy(&m_mutex); }

        int Lock() { return pthread_mutex_lock(&m_mutex); }
        int Trylock() { return pthread_mutex_trylock(&m_mutex); }
        int Unlock() { return pthread_mutex_unlock(&m_mutex); }

    private:
        friend class Cond;
        pthread_mutex_t m_mutex;
    };


    class Cond
    {
    public:
        Cond(Mutex& mutex) : m_lock(mutex) { pthread_cond_init(&m_cond, NULL); }
        virtual ~Cond() { pthread_cond_destroy(&m_cond); }
        int Wait() { return pthread_cond_wait(&m_cond, &(m_lock.m_mutex)); }
        int Signal() { return pthread_cond_signal(&m_cond); }
        int Broadcast() { return pthread_cond_broadcast(&m_cond); }

    private:
        pthread_cond_t m_cond;
        Mutex& m_lock;
    };
}

#endif //#ifndef _KV_DB_UTILS_H_
