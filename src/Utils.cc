#include <stdio.h>
#include "Utils.h"

namespace kvdb{
    
    char* Timing::TimeToChar(Timing& _time)
    {
        return asctime(localtime(&_time.m_time_stamp));
    }

    time_t Timing::GetNow()
    {
        time_t now;
        time(&now);
        return now;
    }

    char* Timing::GetNowChar()
    {
        time_t now;
        time(&now);
        return asctime(localtime(&now));
    }


    void Timing::SetTime(time_t _time)
    {
        m_time_stamp = _time;
    }

    time_t Timing::GetTime()
    {
        return m_time_stamp;
    }

    void Timing::Update()
    {
        time(&m_time_stamp);
    }
    
    bool Timing::IsLate(Timing &time_now)
    {
        if (time_now.m_time_stamp > m_time_stamp)
        {
            return true;
        }
        return false;
    }
    

    Timing::Timing()
    {
        time(&m_time_stamp);
    }

    Timing::Timing(uint64_t _time): m_time_stamp(_time)
    {
        return;
    }
    
    
    Timing::~Timing()
    {
        ;
    }

} // namespace kvdb
