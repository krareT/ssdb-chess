#!/bin/bash

#g++ -std=c++11 -lrocksdb -lgtest -o t_hash_test t_hash_test.cc
make test
./t_hash_test
