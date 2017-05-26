/*
  Copyright (c) 2012-2014 The SSDB Authors. All rights reserved.
  Use of this source code is governed by a BSD-style license that can be
  found in the LICENSE file.
*/
#include "binlog.h"
#include "const.h"
#include "../include.h"
#include "../util/log.h"
#include "../util/strings.h"
#include <map>

/* Binlog */

Binlog::Binlog(uint64_t seq, char type, char cmd, const rocksdb::Slice &key){
    buf.append((char *)(&seq), sizeof(uint64_t));
    buf.push_back(type);
    buf.push_back(cmd);
    buf.append(key.data(), key.size());
}

uint64_t Binlog::seq() const{
    return *((uint64_t *)(buf.data()));
}

char Binlog::type() const{
    return buf[sizeof(uint64_t)];
}

char Binlog::cmd() const{
    return buf[sizeof(uint64_t) + 1];
}

// key after strip seq, type, cmd
const Bytes Binlog::key() const{
    return Bytes(buf.data() + HEADER_LEN, buf.size() - HEADER_LEN);
}

int Binlog::load(const Bytes &s){
    if(s.size() < HEADER_LEN){
	return -1;
    }
    buf.assign(s.data(), s.size());
    return 0;
}

int Binlog::load(const rocksdb::Slice &s){
    if(s.size() < HEADER_LEN){
	return -1;
    }
    buf.assign(s.data(), s.size());
    return 0;
}

int Binlog::load(const std::string &s){
    if(s.size() < HEADER_LEN){
	return -1;
    }
    buf.assign(s.data(), s.size());
    return 0;
}

std::string Binlog::dumps() const{
    std::string str;
    if(buf.size() < HEADER_LEN){
	return str;
    }
    str.reserve(128);

    char buf[20];
    snprintf(buf, sizeof(buf), "%" PRIu64 " ", this->seq());
    str.append(buf);

    switch(this->type()){
    case BinlogType::NOOP:
	str.append("noop ");
	break;
    case BinlogType::SYNC:
	str.append("sync ");
	break;
    case BinlogType::MIRROR:
	str.append("mirror ");
	break;
    case BinlogType::COPY:
	str.append("copy ");
	break;
    case BinlogType::CTRL:
	str.append("control ");
	break;
    }
    switch(this->cmd()){
    case BinlogCommand::NONE:
	str.append("none ");
	break;
    case BinlogCommand::KSET:
	str.append("set ");
	break;
    case BinlogCommand::KDEL:
	str.append("del ");
	break;
    case BinlogCommand::HSET:
	str.append("hset ");
	break;
    case BinlogCommand::HDEL:
	str.append("hdel ");
	break;
    case BinlogCommand::ZSET:
	str.append("zset ");
	break;
    case BinlogCommand::ZDEL:
	str.append("zdel ");
	break;
    case BinlogCommand::BEGIN:
	str.append("begin ");
	break;
    case BinlogCommand::END:
	str.append("end ");
	break;
    case BinlogCommand::QPUSH_BACK:
	str.append("qpush_back ");
	break;
    case BinlogCommand::QPUSH_FRONT:
	str.append("qpush_front ");
	break;
    case BinlogCommand::QPOP_BACK:
	str.append("qpop_back ");
	break;
    case BinlogCommand::QPOP_FRONT:
	str.append("qpop_front ");
	break;
    case BinlogCommand::QSET:
	str.append("qset ");
	break;
    }
    Bytes b = this->key();
    str.append(hexmem(b.data(), b.size()));
    return str;
}


/* SyncLogQueue */

static inline std::string encode_seq_key(uint64_t seq){
    seq = big_endian(seq);
    std::string ret;
    ret.push_back(DataType::SYNCLOG);
    ret.append((char *)&seq, sizeof(seq));
    return ret;
}

static inline uint64_t decode_seq_key(const rocksdb::Slice &key){
    uint64_t seq = 0;
    if(key.size() == (sizeof(uint64_t) + 1) && key.data()[0] == DataType::SYNCLOG){
	seq = *((uint64_t *)(key.data() + 1));
	seq = big_endian(seq);
    }
    return seq;
}

BinlogQueue::BinlogQueue(rocksdb::DB *db, std::vector<rocksdb::ColumnFamilyHandle*> handles,
			 bool enabled, int capacity) {
    this->db = db;
    this->_cfHandles = handles;
    this->_min_seq = 0;
    this->_last_seq = 0;
    this->_tran_seq = 0;
    this->_capacity = capacity;
    this->enabled = enabled;
	
    Binlog log;
    if(this->find_last(&log) == 1){
	this->_last_seq = log.seq();
    }
    // 下面这段代码是可能性能非常差!
    //if(this->find_next(0, &log) == 1){
    //	this->_min_seq = log.seq();
    //}
    if(this->_last_seq > this->_capacity){
	this->_min_seq = this->_last_seq - this->_capacity;
    }else{
	this->_min_seq = 0;
    }
    if(this->find_next(this->_min_seq, &log) == 1){
	this->_min_seq = log.seq();
    }
    if(this->enabled){
	log_info("binlogs capacity: %d, min: %" PRIu64 ", max: %" PRIu64 ",",
		 this->_capacity, this->_min_seq, this->_last_seq);
	// 这个方法有性能问题
	// 但是, 如果不执行清理, 如果将 capacity 修改大, 可能会导致主从同步问题
	//this->clean_obsolete_binlogs();
    }

    // start cleaning thread
    if(this->enabled){
	thread_quit = false;
	pthread_t tid;
	int err = pthread_create(&tid, NULL, &BinlogQueue::log_clean_thread_func, this);
	if(err != 0){
	    log_fatal("can't create thread: %s", strerror(err));
	    exit(0);
	}
    }
}

BinlogQueue::~BinlogQueue(){
    if(this->enabled){
	thread_quit = true;
	for(int i=0; i<100; i++){
	    if(thread_quit == false){
		break;
	    }
	    usleep(10 * 1000);
	}
    }
    Locking l(&this->mutex);
    db = NULL;
}

std::string BinlogQueue::stats() const{
    std::string s;
    s.append("    capacity : " + str(_capacity) + "\n");
    s.append("    min_seq  : " + str(_min_seq) + "\n");
    s.append("    max_seq  : " + str(_last_seq) + "");
    return s;
}

void BinlogQueue::begin(){
    _tran_seq = _last_seq;
    _batch.Clear();
}

void BinlogQueue::rollback(){
    _tran_seq = 0;
}

rocksdb::Status BinlogQueue::commit(){
    rocksdb::WriteOptions write_opts;
    rocksdb::Status s = db->Write(write_opts, &_batch);
    if(s.ok()){
	_last_seq = _tran_seq;
	_tran_seq = 0;
    }
    return s;
}

void BinlogQueue::add_log(char type, char cmd, const rocksdb::Slice &key){
    if(!enabled){
	return;
    }
    _tran_seq ++;
    Binlog log(_tran_seq, type, cmd, key);
    _batch.Put(_cfHandles[kOplogCFHandle], encode_seq_key(_tran_seq), log.repr());
}

void BinlogQueue::add_log(char type, char cmd, const std::string &key){
    if(!enabled){
	return;
    }
    rocksdb::Slice s(key);
    this->add_log(type, cmd, s);
}

// rocksdb put
void BinlogQueue::Put(const rocksdb::Slice& key, const rocksdb::Slice& value){
    _batch.Put(_cfHandles[kDefaultCFHandle], key, value);
}

// rocksdb merge
void BinlogQueue::Merge(const rocksdb::Slice& key, const rocksdb::Slice& value) {
    _batch.Merge(_cfHandles[kDefaultCFHandle], key, value);
}

// rocksdb delete
void BinlogQueue::Delete(const rocksdb::Slice& key){
    _batch.Delete(_cfHandles[kDefaultCFHandle], key);
}
	
int BinlogQueue::find_next(uint64_t next_seq, Binlog *log) const{
    if(this->get(next_seq, log) == 1){
	return 1;
    }
    uint64_t ret = 0;
    std::string key_str = encode_seq_key(next_seq);
    rocksdb::ReadOptions iterate_options;
    rocksdb::Iterator *it = db->NewIterator(iterate_options,
					    _cfHandles[kOplogCFHandle]);
    it->Seek(key_str);
    if(it->Valid()){
	rocksdb::Slice key = it->key();
	if(decode_seq_key(key) != 0){
	    rocksdb::Slice val = it->value();
	    if(log->load(val) == -1){
		ret = -1;
	    }else{
		ret = 1;
	    }
	}
    }
    delete it;
    return ret;
}

int BinlogQueue::find_last(Binlog *log) const{
    uint64_t ret = 0;
    std::string key_str = encode_seq_key(UINT64_MAX);
    rocksdb::ReadOptions iterate_options;
    rocksdb::Iterator *it = db->NewIterator(iterate_options,
					    _cfHandles[kOplogCFHandle]);
    it->Seek(key_str);
    if(!it->Valid()){
	// Iterator::prev requires Valid, so we seek to last
	it->SeekToLast();
    }else{
	// UINT64_MAX is not used 
	it->Prev();
    }
    if(it->Valid()){
	rocksdb::Slice key = it->key();
	if(decode_seq_key(key) != 0){
	    rocksdb::Slice val = it->value();
	    if(log->load(val) == -1){
		ret = -1;
	    }else{
		ret = 1;
	    }
	}
    }
    delete it;
    return ret;
}

int BinlogQueue::get(uint64_t seq, Binlog *log) const{
    std::string val;
    rocksdb::Status s = db->Get(rocksdb::ReadOptions(), _cfHandles[kOplogCFHandle],
				encode_seq_key(seq), &val);
    if(s.ok()){
	if(log->load(val) != -1){
	    return 1;
	}
    }
    return 0;
}

int BinlogQueue::update(uint64_t seq, char type, char cmd, const std::string &key){
    Binlog log(seq, type, cmd, key);
    rocksdb::Status s = db->Put(rocksdb::WriteOptions(), _cfHandles[kOplogCFHandle],
				encode_seq_key(seq), log.repr());
    if(s.ok()){
	return 0;
    }
    return -1;
}

int BinlogQueue::del(uint64_t seq){
    rocksdb::Status s = db->Delete(rocksdb::WriteOptions(), _cfHandles[kOplogCFHandle],
				   encode_seq_key(seq));
    if(!s.ok()){
	return -1;
    }
    return 0;
}

void BinlogQueue::flush(){
    del_range(this->_min_seq, this->_last_seq);
}

// TBD(kg): Del may use DeleteFilesInRange() ?
int BinlogQueue::del_range(uint64_t start, uint64_t end){
    while(start <= end){
	rocksdb::WriteBatch _batch;
	for(int count = 0; start <= end && count < 1000; start++, count++){
	    _batch.Delete(_cfHandles[kOplogCFHandle], encode_seq_key(start));
	}
		
	Locking l(&this->mutex);
	if(!this->db){
	    return -1;
	}
	rocksdb::Status s = this->db->Write(rocksdb::WriteOptions(), &_batch);
	if(!s.ok()){
	    return -1;
	}
    }
    return 0;
}

void* BinlogQueue::log_clean_thread_func(void *arg){
    BinlogQueue *logs = (BinlogQueue *)arg;
	
    while(!logs->thread_quit){
	if(!logs->db){
	    break;
	}
	assert(logs->_last_seq >= logs->_min_seq);

	if(logs->_last_seq - logs->_min_seq < logs->_capacity + 10000){
	    usleep(50 * 1000);
	    continue;
	}
		
	uint64_t start = logs->_min_seq;
	uint64_t end = logs->_last_seq - logs->_capacity;
	logs->del_range(start, end);
	logs->_min_seq = end + 1;
	log_info("clean %d logs[%" PRIu64 " ~ %" PRIu64 "], %d left, max: %" PRIu64 "",
		 end-start+1, start, end, logs->_last_seq - logs->_min_seq + 1, logs->_last_seq);
    }
    log_debug("binlog clean_thread quit");
	
    logs->thread_quit = false;
    return (void *)NULL;
}

// 因为老版本可能产生了断续的binlog
// 例如, binlog-1 存在, 但后面的被删除了, 然后到 binlog-100000 时又开始存在.
void BinlogQueue::clean_obsolete_binlogs(){
    std::string key_str = encode_seq_key(this->_min_seq);
    rocksdb::ReadOptions iterate_options;
    rocksdb::Iterator *it = db->NewIterator(iterate_options,
					    _cfHandles[kOplogCFHandle]);
    it->Seek(key_str);
    if(it->Valid()){
	it->Prev();
    }
    uint64_t count = 0;
    while(it->Valid()){
	rocksdb::Slice key = it->key();
	uint64_t seq = decode_seq_key(key);
	if(seq == 0){
	    break;
	}
	this->del(seq);
		
	it->Prev();
	count ++;
    }
    delete it;
    if(count > 0){
	log_info("clean_obsolete_binlogs: %" PRIu64, count);
    }
}

// TESTING, slow, so not used
void BinlogQueue::merge(){
    std::map<std::string, uint64_t> key_map;
    uint64_t start = _min_seq;
    uint64_t end = _last_seq;
    int reduce_count = 0;
    int total = 0;
    total = end - start + 1;
    (void)total; // suppresses warning
    log_trace("merge begin");
    for(; start <= end; start++){
	Binlog log;
	if(this->get(start, &log) == 1){
	    if(log.type() == BinlogType::NOOP){
		continue;
	    }
	    std::string key = log.key().String();
	    std::map<std::string, uint64_t>::iterator it = key_map.find(key);
	    if(it != key_map.end()){
		uint64_t seq = it->second;
		this->update(seq, BinlogType::NOOP, BinlogCommand::NONE, "");
		//log_trace("merge update %" PRIu64 " to NOOP", seq);
		reduce_count ++;
	    }
	    key_map[key] = log.seq();
	}
    }
    log_trace("merge reduce %d of %d binlogs", reduce_count, total);
}
