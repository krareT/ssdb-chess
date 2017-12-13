#!/bin/bash

. set-env.sh

#nohup env LD_PRELOAD=libterark-zip-rocksdb-r.so:librocksdb.so \
nohup env LD_PRELOAD=libterark-zip-rocksdb-r.so \
    TerarkZipTable_blackListColumnFamily=oplogCF \
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
    ./ssdb-server ./chess.conf &
