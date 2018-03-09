OPT ?= -g2 -O3
#OPT ?= -g2

SRC_DIR = ./src
TOOLS_DIR = ./tools
TEST_DIR = ./test
PMDK_DIR = ./src/pmdk

INCLUDES = -I ${SRC_DIR}/include \
		   -I ${TEST_DIR}/include \
		   -I ${PMDK_DIR}/src/include

GTEST_INCLUDES = -I ${TEST_DIR}/include/gtest ${TEST_DIR}/lib/libgmock.a

LIBS = -pthread -lboost_thread -lboost_system ${PMDK_DIR}/src/nondebug/libpmem.a

CC = gcc
CXX = g++
C_FLAGS = -Wall -fPIC ${OPT}
CXX_FLAGS = --std=c++11 -Wall -fPIC ${OPT}


CORE_C_SRC = $(wildcard ${SRC_DIR}/*.c)
CORE_CXX_SRC = $(wildcard ${SRC_DIR}/*.cc)
CORE_C_OBJ = $(patsubst %.c, %.o, ${CORE_C_SRC})
CORE_CXX_OBJ = $(patsubst %.cc, %.o, ${CORE_CXX_SRC})
COMMON_OBJECTS = ${CORE_C_OBJ} ${CORE_CXX_OBJ}

TEST_SRC = ${TEST_DIR}/test_base.cc ${TEST_DIR}/test_new_base.cc
TEST_OBJECTS  = $(patsubst %.cc, %.o, ${TEST_SRC})

TOOLS_LIST = \
		${TOOLS_DIR}/DbTest \
		${TOOLS_DIR}/CreateDb \
		${TOOLS_DIR}/ExampleKV \
		${TOOLS_DIR}/Benchmark \
		${TOOLS_DIR}/LoadDB

SHARED_LIB = ${SRC_DIR}/libhlkvds.so \
             ${SRC_DIR}/libhlkvds.a

TESTS_LIST = \
		${TEST_DIR}/test_rmd \
		${TEST_DIR}/test_db \
		${TEST_DIR}/test_operations \
		${TEST_DIR}/test_batch \
		${TEST_DIR}/test_iterator \
		${TEST_DIR}/test_status \
		${TEST_DIR}/test_superblock_manager \
		${TEST_DIR}/test_index_manager \
		${TEST_DIR}/test_metastor \
		${TEST_DIR}/test_datastor \
		${TEST_DIR}/test_readcache \
		${TEST_DIR}/test_multivolume \
		${TEST_DIR}/test_multitier

PROGNAME := ${TOOLS_LIST} ${SHARED_LIB}

.PHONY : all
all: ${PROGNAME}
	@echo 'Done'

.PHONY : test
test: ${TESTS_LIST}
	@echo 'make test Done'

$(CORE_CXX_OBJ): %.o: %.cc
	${CXX} ${CXX_FLAGS} ${INCLUDES} -c $< -o $@

$(CORE_C_OBJ): %.o: %.c
	${CC} ${C_FLAGS} ${INCLUDES} -c $< -o $@

$(TEST_OBJECTS): %.o: %.cc
	${CXX} ${CXX_FLAGS} ${INCLUDES} -c $< -o $@

${SRC_DIR}/libhlkvds.so: ${COMMON_OBJECTS}
	${CXX} ${CXX_FLAGS} ${INCLUDES} $^ -shared -o $@ ${LIBS}

${SRC_DIR}/libhlkvds.a: ${COMMON_OBJECTS}
	ar rcs src/libhlkvds.a $^

${TOOLS_DIR}/Benchmark: ${TOOLS_DIR}/Benchmark.cc ${COMMON_OBJECTS}
	${CXX} ${CXX_FLAGS} ${INCLUDES} $^ -o $@ ${LIBS}
${TOOLS_DIR}/CreateDb: ${TOOLS_DIR}/CreateDb.cc ${COMMON_OBJECTS}
	${CXX} ${CXX_FLAGS} ${INCLUDES} $^ -o $@ ${LIBS}
${TOOLS_DIR}/ExampleKV: ${TOOLS_DIR}/ExampleKV.cc ${COMMON_OBJECTS}
	${CXX} ${CXX_FLAGS} ${INCLUDES} $^ -o $@ ${LIBS}
${TOOLS_DIR}/DbTest : ${TOOLS_DIR}/DbTest.cc ${COMMON_OBJECTS}
	${CXX} ${CXX_FLAGS} ${INCLUDES} $^ -o $@ ${LIBS}
${TOOLS_DIR}/LoadDB: ${TOOLS_DIR}/LoadDB.cc ${COMMON_OBJECTS}
	${CXX} ${CXX_FLAGS} ${INCLUDES} $^ -o $@ ${LIBS}

${TEST_DIR}/test_rmd: ${TEST_DIR}/test_rmd.cc ${COMMON_OBJECTS} $(TEST_OBJECTS)
	${CXX} ${CXX_FLAGS} ${INCLUDES} $^ -o $@ ${LIBS} ${GTEST_INCLUDES}
${TEST_DIR}/test_db: ${TEST_DIR}/test_db.cc ${COMMON_OBJECTS} $(TEST_OBJECTS)
	${CXX} ${CXX_FLAGS} ${INCLUDES} $^ -o $@ ${LIBS} ${GTEST_INCLUDES}
${TEST_DIR}/test_operations: ${TEST_DIR}/test_operations.cc ${COMMON_OBJECTS} $(TEST_OBJECTS)
	${CXX} ${CXX_FLAGS} ${INCLUDES} $^ -o $@ ${LIBS} ${GTEST_INCLUDES}
${TEST_DIR}/test_batch: ${TEST_DIR}/test_batch.cc ${COMMON_OBJECTS} $(TEST_OBJECTS)
	${CXX} ${CXX_FLAGS} ${INCLUDES} $^ -o $@ ${LIBS} ${GTEST_INCLUDES}
${TEST_DIR}/test_iterator: ${TEST_DIR}/test_iterator.cc ${COMMON_OBJECTS} $(TEST_OBJECTS)
	${CXX} ${CXX_FLAGS} ${INCLUDES} $^ -o $@ ${LIBS} ${GTEST_INCLUDES}
${TEST_DIR}/test_status: ${TEST_DIR}/test_status.cc ${COMMON_OBJECTS} $(TEST_OBJECTS)
	${CXX} ${CXX_FLAGS} ${INCLUDES} $^ -o $@ ${LIBS} ${GTEST_INCLUDES}
${TEST_DIR}/test_superblock_manager: ${TEST_DIR}/test_superblock_manager.cc ${COMMON_OBJECTS} $(TEST_OBJECTS)
	${CXX} ${CXX_FLAGS} ${INCLUDES} $^ -o $@ ${LIBS} ${GTEST_INCLUDES}
${TEST_DIR}/test_index_manager: ${TEST_DIR}/test_index_manager.cc ${COMMON_OBJECTS} $(TEST_OBJECTS)
	${CXX} ${CXX_FLAGS} ${INCLUDES} $^ -o $@ ${LIBS} ${GTEST_INCLUDES}
${TEST_DIR}/test_metastor: ${TEST_DIR}/test_metastor.cc ${COMMON_OBJECTS} $(TEST_OBJECTS)
	${CXX} ${CXX_FLAGS} ${INCLUDES} $^ -o $@ ${LIBS} ${GTEST_INCLUDES}
${TEST_DIR}/test_datastor: ${TEST_DIR}/test_datastor.cc ${COMMON_OBJECTS} $(TEST_OBJECTS)
	${CXX} ${CXX_FLAGS} ${INCLUDES} $^ -o $@ ${LIBS} ${GTEST_INCLUDES}
${TEST_DIR}/test_readcache: ${TEST_DIR}/test_readcache.cc ${COMMON_OBJECTS} $(TEST_OBJECTS)
	${CXX} ${CXX_FLAGS} ${INCLUDES} $^ -o $@ ${LIBS} ${GTEST_INCLUDES}
${TEST_DIR}/test_multivolume: ${TEST_DIR}/test_multivolume.cc ${COMMON_OBJECTS} $(TEST_OBJECTS)
	${CXX} ${CXX_FLAGS} ${INCLUDES} $^ -o $@ ${LIBS} ${GTEST_INCLUDES}
${TEST_DIR}/test_multitier: ${TEST_DIR}/test_multitier.cc ${COMMON_OBJECTS} $(TEST_OBJECTS)
	${CXX} ${CXX_FLAGS} ${INCLUDES} $^ -o $@ ${LIBS} ${GTEST_INCLUDES}

.PHONY : clean
clean:
	rm -fr $(COMMON_OBJECTS) $(TEST_OBJECTS)
	rm -fr $(PROGNAME)
	rm -fr $(TESTS_LIST)

.PHONY : install
install:
	cp -r src/include/hlkvds /usr/local/include
	cp src/libhlkvds.so /usr/local/lib
	cp src/libhlkvds.a /usr/local/lib

.PHONY : uninstall
uninstall:
	rm -fr /usr/local/include/hlkvds
	rm -f /usr/local/lib/libhlkvds.so
	rm -f /usr/local/lib/libhlkvds.a
