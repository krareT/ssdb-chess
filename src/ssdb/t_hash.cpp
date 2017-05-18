/*
  Copyright (c) 2012-2014 The SSDB Authors. All rights reserved.
  Use of this source code is governed by a BSD-style license that can be
  found in the LICENSE file.
*/
#include "t_hash.h"

static int hset_one(SSDBImpl *ssdb, const Bytes &name, const Bytes &key, const Bytes &val, char log_type);
static int hdel_one(SSDBImpl *ssdb, const Bytes &name, const Bytes &key, char log_type);
//static int incr_hsize(SSDBImpl *ssdb, const Bytes &name, int64_t incr);

/**
 * @return -1: error, 0: item updated, 1: new item inserted
 */
int SSDBImpl::hset(const Bytes &name, const Bytes &key, const Bytes &val, char log_type){
    Transaction trans(_binlogs);

    int ret = hset_one(this, name, key, val, log_type);
    if(ret >= 0){
	rocksdb::Status s = _binlogs->commit();
	if(!s.ok()){
	    return -1;
	}
    }
    return ret;
}

int SSDBImpl::hdel(const Bytes &name, const Bytes &key, char log_type){
    Transaction trans(_binlogs);

    int ret = hdel_one(this, name, key, log_type);
    if(ret >= 0){
	rocksdb::Status s = _binlogs->commit();
	if(!s.ok()){
	    return -1;
	}
    }
    return ret;
}

// TBD(kg): not supported yet
int SSDBImpl::hincr(const Bytes &name, const Bytes &key, int64_t by, int64_t *new_val, char log_type){
    /*Transaction trans(_binlogs);

    std::string old;
    int ret = this->hget(name, key, &old);
    if(ret == -1){
	return -1;
    }else if(ret == 0){
	*new_val = by;
    }else{
	*new_val = str_to_int64(old) + by;
	if(errno != 0){
	    return 0;
	}
    }

    ret = hset_one(this, name, key, str(*new_val), log_type);
    if(ret == -1){
	return -1;
    }
    if(ret >= 0){
	rocksdb::Status s = _binlogs->commit();
	if(!s.ok()){
	    return -1;
	}
    }
    return 1;*/
    return 0;
}

// field count under the key
int64_t SSDBImpl::hsize(const Bytes &key) {
    std::string dbkey = encode_hash_key(key);
    std::string val;
    rocksdb::Status s = ldb->Get(rocksdb::ReadOptions(), dbkey, &val);
    if (s.IsNotFound()) {
	return 0;
    } else if(!s.ok()) {
	return -1;
    } else {
	return get_hash_value_count(val);
    }
}

// remove key
int64_t SSDBImpl::hclear(const Bytes &key) {
    std::string dbkey = encode_hash_key(key);
    ldb->Delete(rocksdb::WriteOptions(), dbkey);
    return 0;

    /*int64_t count = 0;
    while(1){
	HIterator *it = this->hscan(name, "", "", 1000);
	int num = 0;
	while(it->next()){
	    int ret = this->hdel(name, it->key);
	    if(ret == -1){
		delete it;
		return 0;
	    }
	    num ++;
	};
	delete it;

	if(num == 0){
	    break;
	}
	count += num;
    }
    return count;
    */
}

int SSDBImpl::hget(const Bytes &key, std::string *val) {
    std::string dbkey = encode_hash_key(key);
    rocksdb::Status s = ldb->Get(rocksdb::ReadOptions(), dbkey, val);
    if (s.IsNotFound()) {
	return 0;
    }
    if (!s.ok()) {
	log_error("%s", s.ToString().c_str());
	return -1;
    }
    return 1;
}

int SSDBImpl::hget(const Bytes &key, const Bytes &field, std::string *val) {
    std::string dbkey = encode_hash_key(key);
    std::string dbval;
    rocksdb::Status s = ldb->Get(rocksdb::ReadOptions(), dbkey, &dbval);
    if (s.IsNotFound()) {
	return 0;
    }
    if (!s.ok()) {
	log_error("%s", s.ToString().c_str());
	return -1;
    }
    return get_hash_value(Bytes(*val), field, val);
}

// TBD(kg): only support iter within one key right now
HIterator* SSDBImpl::hscan(const Bytes &key, const Bytes &start, const Bytes &end,
			   uint64_t limit) {
    /*std::string field_start, field_end;

    key_start = encode_hash_key(key, field_start);
    if (!end.empty()) {
	field_end = encode_hash_key(name, end);
	}
    //dump(key_start.data(), key_start.size(), "scan.start");
    //dump(key_end.data(), key_end.size(), "scan.end");

    return new HIterator(this->iterator(key_start, key_end, limit), name);
    */
    std::string key_start = encode_hash_key(key);
    return new HIterator(this->iterator(key_start, "", limit), key);
}

// TBD(kg)...
HIterator* SSDBImpl::hrscan(const Bytes &name, const Bytes &start, const Bytes &end, uint64_t limit){
    /*std::string key_start, key_end;

    key_start = encode_hash_key(name, start);
    if(start.empty()){
	key_start.append(1, 255);
    }
    if(!end.empty()){
	key_end = encode_hash_key(name, end);
    }
    //dump(key_start.data(), key_start.size(), "scan.start");
    //dump(key_end.data(), key_end.size(), "scan.end");

    return new HIterator(this->rev_iterator(key_start, key_end, limit), name);
    */
    return 0;
}

static void get_hnames(Iterator *it, std::vector<std::string> *list) {
    while (it->next()) {
	Bytes ks = it->key();
	if (ks.data()[0] != DataType::HASH) {
	    break;
	}
	std::string n;
	if (decode_hash_key(ks, &n) == -1) {
	    continue;
	}
	list->push_back(n);
    }
}

// list of keys between [key_s, key_e]
int SSDBImpl::hlist(const Bytes &key_s, const Bytes &key_e, uint64_t limit,
		    std::vector<std::string> *list) {
    std::string start = encode_hash_key(key_s);
    std::string end = (!key_e.empty()) ? encode_hash_key(key_e) : "";
    
    Iterator *it = this->iterator(start, end, limit);
    get_hnames(it, list);
    delete it;
    return 0;
}

// TBD(kg):...
int SSDBImpl::hrlist(const Bytes &name_s, const Bytes &name_e, uint64_t limit,
		     std::vector<std::string> *list){
    /*std::string start;
    std::string end;
	
    start = encode_hsize_key(name_s);
    if(name_s.empty()){
	start.append(1, 255);
    }
    if(!name_e.empty()){
	end = encode_hsize_key(name_e);
    }
	
    Iterator *it = this->rev_iterator(start, end, limit);
    get_hnames(it, list);
    delete it;
    */
    return 0;
}

// returns the number of newly added items
static int hset_one(SSDBImpl *ssdb, const Bytes &key, const Bytes &field, const Bytes &val,
		    char log_type) {
    if (key.empty() || field.empty()) {
	log_error("empty key or field!");
	return -1;
    }
    if (key.size() > SSDB_KEY_LEN_MAX) {
	log_error("key too long! %s", hexmem(key.data(), key.size()).c_str());
	return -1;
    }
    if (field.size() > SSDB_KEY_LEN_MAX) {
	log_error("field too long! %s", hexmem(field.data(), field.size()).c_str());
	return -1;
    }
    // TBD(kg): should change to Merge()
    std::string hkey = encode_hash_key(key);
    std::string old_value, new_value;
    ssdb->hget(hkey, &old_value);
    int ret = insert_update_hash_value(Bytes(old_value), field, val, &new_value);
    if (ret != -1) {
	ssdb->_binlogs->Put(hkey, slice(new_value));
	ssdb->_binlogs->add_log(log_type, BinlogCommand::HSET, hkey);
    }
    return ret;
}

static int hdel_one(SSDBImpl *ssdb, const Bytes &key, const Bytes &field, char log_type) {
    if (key.size() > SSDB_KEY_LEN_MAX) {
	log_error("name too long! %s", hexmem(key.data(), key.size()).c_str());
	return -1;
    }
    if (field.size() > SSDB_KEY_LEN_MAX) {
	log_error("key too long! %s", hexmem(field.data(), field.size()).c_str());
	return -1;
    }
    // TBD(kg): should change to Merge()
    std::string hkey = encode_hash_key(key);
    std::string old_value, new_value;
    ssdb->hget(hkey, &old_value);
    int ret = remove_hash_value(Bytes(old_value), field, &new_value);
    if (ret == 1) { // remove as expected
	ssdb->_binlogs->Put(hkey, slice(new_value));
	ssdb->_binlogs->add_log(log_type, BinlogCommand::HSET, hkey);
    }
    return ret;
}

