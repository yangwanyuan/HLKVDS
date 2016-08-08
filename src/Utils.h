#ifndef _KV_DB_UTILS_H_
#define _KV_DB_UTILS_H_

#include <time.h>
#include <inttypes.h>
#include <sys/types.h>

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

}

#endif //#ifndef _KV_DB_UTILS_H_
