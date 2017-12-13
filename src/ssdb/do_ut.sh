#!/bin/bash

#g++ -std=c++11 -lrocksdb -lgtest -o t_hash_test t_hash_test.cc
make test
unset TerarkZipTable_localTempDir
#env LD_PRELOAD=librocksdb.so:libterark-zip-rocksdb-r.so ./t_hash_test
./t_hash_test
