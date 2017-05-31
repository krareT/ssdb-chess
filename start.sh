#!/bin/bash

. set-env.sh

env LD_PRELOAD=libterark-zip-rocksdb-r.so:libterark-core-r.so:libterark-fsa-r.so:libterark-zbs-r.so \
    TerarkZipTable_blackListColumnFamily=oplogCF \
    TerarkZipTable_indexNestLevel=2 \
    TerarkZipTable_indexCacheRatio=0.005 \
    TerarkZipTable_smallTaskMemory=1G \
    TerarkZipTable_softZipWorkingMemLimit=16G \
    TerarkZipTable_hardZipWorkingMemLimit=32G \
    ./ssdb-server -d ./chess.conf -s start
