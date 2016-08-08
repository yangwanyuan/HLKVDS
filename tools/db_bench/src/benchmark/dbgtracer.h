#ifndef DB_BENCH_DBGTRACER
#define DB_BENCH_DBGTRACER
#include "mutexlock.h"
#include <cstdio>

class Tracer{//a tracer for debugging
	public:
		static Tracer* getInstance();
		static void destoryInstance();
		int Put(const char* str);
	private:
		Tracer() : file_(){} //
		FILE* file_;
		
		//no copy allowed
		Tracer(const Tracer &){}
		void operator=(const Tracer &){}
		
		
		port::Mutex mu_;
		
};




#endif