#ifndef _KV_DB_UTILS_H_
#define _KV_DB_UTILS_H_

#include <time.h>
#include <sys/types.h>

#include "BlockDevice.h"
namespace kvdb{
    class Timing{
    public:
        static size_t GetTimeSizeOf(){ return sizeof(time_t);}

        bool LoadTimeFromDevice(off_t offset);
        bool WriteTimeToDevice(off_t offset);

        void UpdateTimeToNow();

        char* TimeToChar();

        bool IsLate(Timing &time_now);

        Timing(BlockDevice* bdev);
        //Timing(time_t &time_stamp);
        ~Timing();
    private:
        time_t m_time_stamp;
        BlockDevice* m_bdev;
    };

}

#endif //#ifndef _KV_DB_UTILS_H_
