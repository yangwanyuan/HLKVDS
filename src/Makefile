CC = g++ 
C_FLAGS = -g -Wall #--std=c++11 
INCLUDES = -I ./
LIBS = #-lpthread #-lcrypto

#PROGNAME = CreateDb ExampleKV DbTest
PROGNAME = DbTest CreateDb ExampleKV Benchmark 

all: $(PROGNAME)

%.o : %.cc
	${CXX} -c ${C_FLAGS} ${INCLUDES} $< -o $@

%.o : %.c
	${CC} -c ${C_FLAGS} ${INCLUDES} $< -o $@

OBJECTS =  HashFun.o Kvdb_Impl.o IndexManager.o SuperBlockManager.o DataHandle.o BlockDevice.o KernelDevice.o Utils.o Kvdb.o 

Benchmark: $(OBJECTS)
	$(CC) $(C_FLAGS) $(OBJECTS) Benchmark.cc -o Benchmark $(INCLUDES) $(LIBS)

CreateDb: $(OBJECTS)
	$(CC) $(C_FLAGS) $(OBJECTS) CreateDb.cc -o CreateDb $(INCLUDES) $(LIBS)

ExampleKV: $(OBJECTS)
	$(CC) $(C_FLAGS) $(OBJECTS) ExampleKV.cc -o ExampleKV $(INCLUDES) $(LIBS)

DbTest : $(OBJECTS)
	$(CC) $(C_FLAGS) $(OBJECTS) DbTest.cc -o DbTest $(INCLUDES) $(LIBS)



.PHONY:clean
clean:
	rm -fr $(OBJECTS) 
	rm -fr $(PROGNAME)

