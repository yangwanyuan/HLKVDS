#ifndef _KV_DB_UTILS_H_
#define _KV_DB_UTILS_H_

#include <time.h>
#include <inttypes.h>
#include <sys/types.h>
#include <pthread.h>

namespace kvdb{
    class Timing{
    public:
        static size_t GetTimeSizeOf(){ return sizeof(time_t);}
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
        pthread_t Self();

        virtual void* Entry() = 0;


    private:
        pthread_t m_tid;
        int m_running;
        int m_detached;

        void* entry_wrapper();
        static void* runThread(void* arg);
    };

    class Mutex
    {
    public:
        Mutex() {pthread_mutex_init(&m_mutex, NULL);}
        virtual ~Mutex() {pthread_mutex_destroy(&m_mutex);}

        int Lock() { return pthread_mutex_lock(&m_mutex);}
        int Trylock() { return pthread_mutex_trylock(&m_mutex);}
        int Unlock() { return pthread_mutex_unlock(&m_mutex);}

    private:
        pthread_mutex_t m_mutex;
    };

}

#endif //#ifndef _KV_DB_UTILS_H_
