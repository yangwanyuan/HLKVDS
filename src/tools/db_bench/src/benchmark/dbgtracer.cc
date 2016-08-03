#include "dbgtracer.h"

Tracer* Tracer::getInstance()
{
	static Tracer* inst = NULL;
	if(inst == NULL){
		mu_.Lock();
		if(inst == NULL){
			inst = new Tracer();
		}
		mu_.Unlock();
	}
	return inst;
}