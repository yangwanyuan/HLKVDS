#include <stdio.h>
#include "Utils.h"

namespace kvdb{
    void Timing::UpdateTimeToNow()
    {
        time(&m_time_stamp);
    }
    
    char* Timing::TimeToChar()
    {
        return asctime(localtime(&m_time_stamp));
    }
    
    bool Timing::IsLate(Timing &time_now)
    {
        if(time_now.m_time_stamp > m_time_stamp)
            return true;
        return false;
    }
    
    Timing::Timing(BlockDevice* bdev):m_bdev(bdev)
    {
        ;
    }
    
    //Timing::Timing(time_t &time_stamp)
    //{
    //    m_time_stamp = time_stamp;
    //}
    
    Timing::~Timing()
    {
        ;
    }
    
    bool Timing::LoadTimeFromDevice(off_t offset)
    {
        ssize_t timeLength = GetTimeSizeOf();
        if(m_bdev->pRead(&m_time_stamp, timeLength, offset) != timeLength){
            perror("Error in reading timestamp from file\n");
            return false;
        }
        return true;
    }
    
    bool Timing::WriteTimeToDevice(off_t offset)
    {
        ssize_t timeLength = GetTimeSizeOf();
        if(m_bdev->pWrite((void *)&m_time_stamp, timeLength, offset ) != timeLength)
        {
            perror("Error write timestamp to file\n");
            return false;
        }
        return true;
    }

} // namespace kvdb
