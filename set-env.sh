#!/bin/bash

BASE_DIR=`cd .. && pwd`
export PREFIX=$BASE_DIR
export PATH=$PREFIX/ssdb-chess/rocksdb:$PATH
export ROCKSDB_INC=$PREFIX/ssdb-chess/rocksdb/include
export ROCKSDB_LIB=$PREFIX/ssdb-chess/rocksdb
export PKG_TERARK_HOME=$PREFIX/terark-zip-rocksdb/pkg/terark-zip-rocksdb-Linux-x86_64-g++-4.8-bmi2-0
#export PKG_TERARK_HOME=$PREFIX/ssdb-chess/terark-zip-rocksdb
export LD_LIBRARY_PATH=$PKG_TERARK_HOME/lib:$PREFIX/ssdb-chess/rocksdb:/usr/local/lib:/usr/mpc/lib:/usr/gmp/lib:/usr/mpfr/lib:/usr/isl/lib
export TerarkZipTable_localTempDir=$PREFIX/ssdb-chess/terark-temp
export PPROF_PATH=/usr/local/bin/pprof
echo "hi, there"
