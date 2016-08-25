#include <stdio.h>
#include "Utils.h"

namespace kvdb{
    
    char* KVTime::TimeToChar(KVTime& _time)
    {
        return asctime(localtime(&_time.timestamp_));
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
        timestamp_ = _time;
    }

    time_t KVTime::GetTime()
    {
        return timestamp_;
    }

    void KVTime::Update()
    {
        time(&timestamp_);
    }
    

    KVTime::KVTime()
    {
        time(&timestamp_);
    }

    KVTime::KVTime(const KVTime& toBeCopied)
    {
        timestamp_ = toBeCopied.timestamp_;
    }
    
    KVTime& KVTime::operator=(const KVTime& toBeCopied)
    {
        timestamp_ = toBeCopied.timestamp_;
        return *this;
    }

    bool KVTime::operator>(const KVTime& toBeCopied)
    {
        return timestamp_ > toBeCopied.timestamp_;
    }

    bool KVTime::operator<(const KVTime& toBeCopied)
    {
        return timestamp_ < toBeCopied.timestamp_;
    }

    double KVTime::operator-(const KVTime& toBeCopied)
    {
        return difftime(timestamp_, toBeCopied.timestamp_);
    }

    KVTime::~KVTime(){}

    KVTime::KVTime(uint64_t _time): timestamp_(_time){}


    void* Thread::runThread(void* arg)
    {
        return ((Thread*)arg)->Entry();
    }

    Thread::Thread() : tid_(0), running_(0), detached_(0){}

    Thread::~Thread()
    {
    }

    int Thread::Start()
    {
        int result = pthread_create(&tid_, NULL, runThread, (void *)this);
        if (result == 0)
        {
            running_ = 1;
        }
        return result;
    }

    int Thread::Join()
    {
        int result = -1;
        if (running_ == 1)
        {
            result = pthread_join(tid_, NULL);
            if (result == 0)
            {
                detached_ = 0;
            }
        }
        return result;
    }

    int Thread::Detach()
    {
        int result = -1;
        if (running_ == 1 && detached_ == 0)
        {
            result = pthread_detach(tid_);
            if (result == 0)
            {
                detached_ = 1;
            }
        }
        return result;
    }

    bool Thread::Is_started() const
    {
        return tid_ != 0;
    }

    bool Thread::Am_self() const
    {
        return (pthread_self() == tid_);
    }

    pthread_t Thread::Self()
    {
        return tid_;
    }
} // namespace kvdb
