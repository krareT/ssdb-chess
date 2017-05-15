CC=gcc
CXX=g++ -std=c++11
MAKE=make
LEVELDB_PATH=/Users/xa_xx/ssdb-chess/deps/leveldb-1.18
JEMALLOC_PATH=/Users/xa_xx/ssdb-chess/deps/jemalloc-4.1.0
SNAPPY_PATH=/Users/xa_xx/ssdb-chess/deps/snappy-1.1.0
CFLAGS=
CFLAGS = -DNDEBUG -D__STDC_FORMAT_MACROS -Wall -O2 -Wno-sign-compare
CFLAGS += 
CFLAGS += -I "/Users/xa_xx/ssdb-chess/deps/rocksdb/include"
CLIBS=
CLIBS += "/Users/xa_xx/ssdb-chess/deps/rocksdb/librocksdb.dylib"
CLIBS += "/Users/xa_xx/ssdb-chess/deps/snappy-1.1.0/.libs/libsnappy.a"
CLIBS += "/Users/xa_xx/ssdb-chess/deps/jemalloc-4.1.0/lib/libjemalloc.a"
CFLAGS += -I "/Users/xa_xx/ssdb-chess/deps/jemalloc-4.1.0/include"
CLIBS += 
CFLAGS += -DNEW_MAC
