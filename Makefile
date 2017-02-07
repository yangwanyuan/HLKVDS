#OPT ?= -g
OPT ?= -g2

INCLUDES =  \
			-I ./src \
			-I ./include/hyperds

LIBS = -pthread

CC = gcc
CXX = g++
C_FLAGS = -Wall -fPIC ${OPT}
CXX_FLAGS = --std=c++11 -Wall -fPIC ${OPT}

SRC_DIR = ./src
CORE_C_SRC = $(wildcard ./src/*.c)
CORE_CXX_SRC = $(wildcard ./src/*.cc)

CORE_C_OBJ = $(patsubst %.c, %.o, ${CORE_C_SRC})
CORE_CXX_OBJ = $(patsubst %.cc, %.o, ${CORE_CXX_SRC})

UTILS = \
		src/DbTest \
		src/CreateDb \
		src/ExampleKV \
		src/Benchmark \
		src/GCTest

SHARED_LIB = src/libkv.so

TEST = test/rmdtest

PROGNAME := ${UTILS} ${SHARED_LIB}

SRC_OBJ =  Kvdb_Impl.o IndexManager.o SuperBlockManager.o DataHandle.o BlockDevice.o KernelDevice.o SegmentManager.o GcManager.o Utils.o Kvdb.o KeyDigestHandle.o rmd160.o Options.o
SRC_OBJECTS = $(addprefix ${SRC_DIR}/, ${SRC_OBJ})
COMMON_OBJECTS = ${SRC_OBJECTS}
ALL_OBJECTS = $(CORE_C_OBJ) $(CORE_CXX_OBJ)

.PHONY : all
all: ${PROGNAME}
	@echo 'Done'

.PHONY : test
test: ${TEST}
	@echo 'make test Done'

$(CORE_CXX_OBJ): %.o: %.cc
	${CXX} ${CXX_FLAGS} ${INCLUDES} -c $< -o $@

$(CORE_C_OBJ): %.o: %.c
	${CC} ${C_FLAGS} -c $< -o $@


src/Benchmark: ${SRC_DIR}/Benchmark.cc ${COMMON_OBJECTS}
	${CXX} ${CXX_FLAGS} ${INCLUDES} $^ -o $@ ${LIBS}

src/CreateDb: ${SRC_DIR}/CreateDb.cc ${COMMON_OBJECTS}
	${CXX} ${CXX_FLAGS} ${INCLUDES} $^ -o $@ ${LIBS}

src/ExampleKV: ${SRC_DIR}/ExampleKV.cc ${COMMON_OBJECTS}
	${CXX} ${CXX_FLAGS} ${INCLUDES} $^ -o $@ ${LIBS}

src/DbTest : ${SRC_DIR}/DbTest.cc ${COMMON_OBJECTS}
	${CXX} ${CXX_FLAGS} ${INCLUDES} $^ -o $@ ${LIBS}

src/GCTest: ${SRC_DIR}/GCTest.cc ${COMMON_OBJECTS}
	${CXX} ${CXX_FLAGS} ${INCLUDES} $^ -o $@ ${LIBS}

src/libkv.so: ${COMMON_OBJECTS}
	${CXX} ${CXX_FLAGS} ${INCLUDES} $^ -shared -o $@ ${LIBS}

test/rmdtest: test/rmdtest.cc ${COMMON_OBJECTS} 
	${CXX} ${CXX_FLAGS} ${INCLUDES} $^ -o $@ ${LIBS}

.PHONY : clean
clean:
	rm -fr $(ALL_OBJECTS)
	rm -fr $(PROGNAME)
	rm -fr $(TEST)

.PHONY : install
install:
	cp src/*.h /usr/local/include
	cp src/libkv.so /usr/local/lib
