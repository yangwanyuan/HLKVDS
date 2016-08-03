# a db_bench for kv database
  ported from leveldb's db_bench
# benchmark 
only support fillseq, fillrandom
.
├── Makefile
├── README.md
└── src
    ├── benchmark
    └── db  //a header file && implement for a particular db  - will be `cp` to "benchmark" folder by `make`


# quick start
```
cd src/benchmark/
make clean && make
./db_bench
```

