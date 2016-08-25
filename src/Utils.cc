#include <stdio.h>
#include "Utils.h"

namespace kvdb{
    
    char* KVTime::TimeToChar(KVTime& _time)
    {
        return asctime(localtime(&_time.m_time_stamp));
    }

    time_t KVTime::GetNow()
    {
        time_t now;
        time(&now);
        return now;
    }

    char* KVTime::GetNowChar()
    {
        time_t now;
        time(&now);
        return asctime(localtime(&now));
    }


    void KVTime::SetTime(time_t _time)
    {
        m_time_stamp = _time;
    }

    time_t KVTime::GetTime()
    {
        return m_time_stamp;
    }

    void KVTime::Update()
    {
        time(&m_time_stamp);
    }
    

    KVTime::KVTime()
    {
        time(&m_time_stamp);
    }

    KVTime::KVTime(const KVTime& toBeCopied)
    {
        m_time_stamp = toBeCopied.m_time_stamp;
    }
    
    KVTime& KVTime::operator=(const KVTime& toBeCopied)
    {
        m_time_stamp = toBeCopied.m_time_stamp;
        return *this;
    }

    bool KVTime::operator>(const KVTime& toBeCopied)
    {
        return m_time_stamp > toBeCopied.m_time_stamp;
    }

    bool KVTime::operator<(const KVTime& toBeCopied)
    {
        return m_time_stamp < toBeCopied.m_time_stamp;
    }

    double KVTime::operator-(const KVTime& toBeCopied)
    {
        return difftime(m_time_stamp, toBeCopied.m_time_stamp);
    }

    KVTime::~KVTime(){}

    KVTime::KVTime(uint64_t _time): m_time_stamp(_time){}


    void* Thread::runThread(void* arg)
    {
        return ((Thread*)arg)->Entry();
    }

    Thread::Thread() : m_tid(0), m_running(0), m_detached(0){}

    Thread::~Thread()
    {
    }

    int Thread::Start()
    {
        int result = pthread_create(&m_tid, NULL, runThread, (void *)this);
        if (result == 0)
        {
            m_running = 1;
        }
        return result;
    }

    int Thread::Join()
    {
        int result = -1;
        if (m_running == 1)
        {
            result = pthread_join(m_tid, NULL);
            if (result == 0)
            {
                m_detached = 0;
            }
        }
        return result;
    }

    int Thread::Detach()
    {
        int result = -1;
        if (m_running == 1 && m_detached == 0)
        {
            result = pthread_detach(m_tid);
            if (result == 0)
            {
                m_detached = 1;
            }
        }
        return result;
    }

    bool Thread::Is_started() const
    {
        return m_tid != 0;
    }

    bool Thread::Am_self() const
    {
        return (pthread_self() == m_tid);
    }

    pthread_t Thread::Self()
    {
        return m_tid;
    }
} // namespace kvdb
