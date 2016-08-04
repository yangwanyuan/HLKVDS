current_dir=$(shell pwd)
leveldb_dir=$(current_dir)/src/db/leveldb
benchmark_dir=$(current_dir)/src/benchmark
bin=db_bench

all: leveldb_test kv_db_test

leveldb_test:
	@echo "tests for leveldb"
	$(MAKE) -C $(benchmark_dir) KV_PATH=$(leveldb_dir)
	cp $(benchmark_dir)/$(bin) .

kv_db_test:
	@echo "tests for kv-db"

clean:
	$(MAKE) -C src/benchmark clean
	rm -f $(bin)
