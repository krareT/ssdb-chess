include ../build_config.mk

OBJS += ../src/net/link.o ../src/net/fde.o ../src/util/log.o ../src/util/bytes.o
CFLAGS += -I../src
EXES = ssdb-bench ssdb-dump ssdb-repair leveldb-import ssdb-migrate ssdb-bench-update ssdb-bench-update2

export $CFLAGS
#all: ssdb-bench.o ssdb-dump.o ssdb-repair.o leveldb-import.o ssdb-migrate.o
all: ssdb-bench.o ssdb-dump.o ssdb-migrate.o ssdb-bench-update.o ssdb-bench-update2.o ssdb-confirm.o
	${CXX} -o ssdb-bench ssdb-bench.o ${OBJS} ${UTIL_OBJS} ${CLIBS}
	${CXX} -o ssdb-dump ssdb-dump.o ${OBJS} ${UTIL_OBJS} ${CLIBS} -lrocksdb
	${CXX} -g -o ssdb-migrate ssdb-migrate.o ../api/cpp/libssdb-client.a ../src/util/libutil.a -lpthread
	${CXX} -g -o ssdb-bench-update ssdb-bench-update.o ${OBJS} ${UTIL_OBJS} ${CLIBS}
	${CXX} -g -o ssdb-bench-update2 ssdb-bench-update2.o ${OBJS} ${UTIL_OBJS} ${CLIBS} -lpthread ../api/cpp/libssdb-client.a ../src/util/libutil.a
	${CXX} -g -o ssdb-confirm ssdb-confirm.o ../api/cpp/libssdb-client.a ../src/util/libutil.a
#	${CXX} -o ssdb-repair ssdb-repair.o ${OBJS} ${UTIL_OBJS} ${CLIBS}
#	${CXX} -o leveldb-import leveldb-import.o ${OBJS} ${UTIL_OBJS} ${CLIBS}

ssdb-migrate.o: ssdb-migrate.cpp
	${CXX} -g ${CFLAGS} -I../api/cpp -c ssdb-migrate.cpp
ssdb-bench.o: ssdb-bench.cpp
	${CXX} ${CFLAGS} -c ssdb-bench.cpp
ssdb-dump.o: ssdb-dump.cpp block-queue.h
	${CXX} -g ${CFLAGS} -c ssdb-dump.cpp
ssdb-bench-update.o: ssdb-bench-update.cpp
	${CXX} -g ${CFLAGS} -c ssdb-bench-update.cpp
ssdb-bench-update2.o: ssdb-bench-update2.cpp
	${CXX} -g ${CFLAGS} -c ssdb-bench-update2.cpp
ssdb-confirm.o: ssdb-confirm.cpp
	${CXX} -g ${CFLAGS} -c ssdb-confirm.cpp
#ssdb-repair.o: ssdb-repair.cpp
#	${CXX} ${CFLAGS} -c ssdb-repair.cpp
#leveldb-import.o: leveldb-import.cpp
#	${CXX} ${CFLAGS} -c leveldb-import.cpp

clean:
	rm -f *.exe *.exe.stackdump *.o ${EXES}
	rm -rf _cpy_

