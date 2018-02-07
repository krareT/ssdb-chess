#!/bin/bash

. set-env.sh

# file descritor: for sockets
ulimit -n 8192
# defaults is 7200 which is too long
echo 1800 > /proc/sys/net/ipv4/tcp_keepalive_time


nohup env LD_PRELOAD=libterark-zip-rocksdb-r.so:librocksdb.so \
    TerarkUseDivSufSort=1 \
    TerarkZipTable_write_buffer_size=2G \
    TerarkZipTable_indexNestLevel=4 \
    TerarkZipTable_indexCacheRatio=0.001 \
    TerarkZipTable_sampleRatio=0.015 \
    TerarkZipTable_smallTaskMemory=1G \
    TerarkZipTable_softZipWorkingMemLimit=16G \
    TerarkZipTable_hardZipWorkingMemLimit=32G \
    TerarkZipTable_minDictZipValueSize=1024 \
    TerarkZipTable_offsetArrayBlockUnits=128 \
    TerarkZipTable_max_background_flushes=2 \
    TerarkZipTable_base_background_compactions=2 \
    TerarkZipTable_max_background_compactions=2 \
    TerarkZipTable_extendedConfigFile=/root/.terark_license \
    ./ssdb-server ./chess.conf &

#TerarkZipTable_level0_file_num_compaction_trigger=20 \
#HEAPCHECK=normal \

