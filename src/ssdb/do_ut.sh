#!/bin/bash

g++ -std=c++11 -L. -L../util -lssdb -lutil -lrocksdb -lgtest -o t_hash_test t_hash_test.cc t_hash.cpp
./t_hash_test
