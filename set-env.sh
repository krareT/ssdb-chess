#!/bin/bash

BASE_DIR=`cd .. && pwd`
export PREFIX=$BASE_DIR
export PATH=$PREFIX/ssdb-chess/rocksdb:/opt/gcc-5.4/bin:$PATH
export ROCKSDB_INC=$PREFIX/ssdb-chess/rocksdb/include
export ROCKSDB_LIB=$PREFIX/ssdb-chess/rocksdb
export TERARK_HOME=$PREFIX/terark-zip-rocksdb
export LD_LIBRARY_PATH=$TERARK_HOME/lib:$PREFIX/ssdb-chess/rocksdb:/usr/mpc/lib:/usr/gmp/lib:/usr/mpfr/lib:/usr/isl/lib:/opt/gcc-5.4/lib64
export TerarkZipTable_localTempDir=$PREFIX/ssdb-chess/temp
echo "hi, there"
