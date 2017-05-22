/*
  Copyright (c) 2012-2014 The SSDB Authors. All rights reserved.
  Use of this source code is governed by a BSD-style license that can be
  found in the LICENSE file.
*/
#include <iostream>

#include "t_hash.h"

using std::cout;
using std::endl;

static int hset_one(SSDBImpl *ssdb, const Bytes &key, const Bytes &field, const Bytes &val, char log_type);
static int hset_one(SSDBImpl *ssdb, const Bytes &key, const Bytes &val, char log_type);
static int hdel_one(SSDBImpl *ssdb, const Bytes &key, const Bytes &field, char log_type);
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

// hreplace actually
int SSDBImpl::hset(const Bytes &key, const Bytes &val, char log_type) {
    Transaction trans(_binlogs);

    int ret = hset_one(this, key, val, log_type);
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
    return get_hash_value(Bytes(dbval), field, val);
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

// TBD(kg): make sure enum all hset values is provided

// TBD(kg): ...
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
    /*std::string old_value, new_value;
    int ret = 0;
    if ((ret = ssdb->hget(key, &old_value)) == -1) {
	log_error("failed to hget on %s!", key.data());
	return -1;
    }
    ret = insert_update_hash_value(Bytes(old_value), field, val, &new_value);
    if (ret != -1) {
    */
    // use 'hkey' to log, not 'key'
    std::string hkey = encode_hash_key(key);
    std::string new_value = encode_hash_value(field, val);
    ssdb->_binlogs->Merge(hkey, slice(new_value));
    ssdb->_binlogs->add_log(log_type, BinlogCommand::HSET, hkey);

    return 0;
}

static int hset_one(SSDBImpl *ssdb, const Bytes &key, const Bytes &val, char log_type) {
    if (key.empty()) {
	log_error("empty key or field!");
	return -1;
    }
    if (key.size() > SSDB_KEY_LEN_MAX) {
	log_error("key too long! %s", hexmem(key.data(), key.size()).c_str());
	return -1;
    }

    std::string hkey = encode_hash_key(key);
    ssdb->_binlogs->Merge(hkey, slice(val));
    ssdb->_binlogs->add_log(log_type, BinlogCommand::HSET, hkey);

    return 0;
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
    /*std::string old_value, new_value;
    int ret = 0;
    if ((ret = ssdb->hget(key.data(), &old_value)) == -1) {
	log_error("failed to hget on %s!", key.data());
	return -1;
    }
     ret = remove_hash_value(Bytes(old_value), field, &new_value);
    if (ret == 1) { // remove as expected
    */
    std::string hkey = encode_hash_key(key);
    std::string new_value = encode_hash_value(field, "_deleted_");
    ssdb->_binlogs->Merge(hkey, slice(new_value));
    ssdb->_binlogs->add_log(log_type, BinlogCommand::HSET, hkey);

    return 0;
}


std::string encode_hash_key(const Bytes &key) {
    std::string buf;
    buf.reserve(1 + kKeyByteLen + key.size());
    buf.append(1, DataType::HASH);
    buf.append(kKeyByteLen, (uint8_t)key.size());
    buf.append(key.data(), key.size());
    return buf;
}

int decode_hash_key(const Bytes &slice, std::string *key) {
    Decoder decoder(slice.data(), slice.size());
    if (decoder.skip(1) == -1) {
	return -1;
    }
    if (decoder.read_8_data(key) == -1) {
	return -1;
    }
    return 0;
}

std::string encode_hash_value(const Bytes &field, const Bytes &value) {
    std::string buf;
    buf.reserve(kFieldByteLen + field.size() + 1 + kValueByteLen + value.size());
    buf.append(kFieldByteLen, (uint8_t)field.size());
    buf.append(field.data(), field.size());
    buf.append(1, ':');
    buf.append(kValueByteLen, (uint8_t)value.size());
    buf.append(value.data(), value.size());
    return buf;
}

int decode_hash_value(const Bytes& slice, std::string* field, std::string* value) {
    Decoder decoder(slice.data(), slice.size());
    if (!decoder.is_end()) {
	int ret = 0;
	if ((ret = decoder.read_8_data(field)) == -1) {
	    return -1;
	}
	decoder.skip(1); // ':'
	if ((ret = decoder.read_8_data(value)) == -1) {
	    return -1;
	}
    }

    return 0;
}

// TBD(kg): string op should be optimized
int insert_update_hash_value(const Bytes& slice, const Bytes& field,
			     const Bytes& value, std::string* ret) {
    /*
     * buffer.reserve(slice.size() + encoded.size() + N)
     * find field in slice, if exist:
     *   buffer.append(slice).append(';').append(encoded)
     *   return
     * iter { field, value } pair:
     *   if iter.field == field:
     *     skip
     *   else:
     *     buf.append(field, iter.value)
     *     update total size
     * buf.append(encoded)
     * shrink buf size
     */
    std::string encoded = encode_hash_value(field, value);
    if (slice.empty()) {
	*ret = encoded;
	return 0;
    }
    std::string buf;
    buf.reserve(slice.size() + 1 + encoded.size());
    auto iter = std::search(slice.data(), slice.data() + slice.size(),
			    field.data(), field.data() + field.size());
    if (iter == slice.data() + slice.size()) { // insert, just append & return
	buf.append(slice.data(), slice.size())
	    .append(1, ';')
	    .append(encoded.data(), encoded.size());
	*ret = buf;
	return 0;
    }
    Decoder decoder(slice.data(), slice.size());
    int len = 0;
    while (!decoder.is_end()) {
	std::string elem_field, elem_value;
	int field_len, value_len;
	if ((field_len = decoder.read_8_data(&elem_field)) == -1) {
	    return -1;
	}
	decoder.skip(1); // ':'
	if ((value_len = decoder.read_8_data(&elem_value)) == -1) {
	    return -1;
	}
	if (Bytes(field) == Bytes(elem_field)) {
	    ; // skip
	} else {
	    std::string encoded = encode_hash_value(elem_field, elem_value);
	    buf.append(encoded.data(), encoded.size())
		.append(1, ';');
	    len += encoded.size() + 1;
	}
	if (!decoder.is_end()) {
	    decoder.skip(1); // ';'
	}
    }
    // format till now: a4b4:14;
    buf.append(encoded.data(), encoded.size());
    // shrink() could only be applied on 'string'
    buf.resize(len + encoded.size());
    *ret = buf;
    return 1;
}

// -1: error
// 0: not exist
// 1: done
int remove_hash_value(const Bytes& slice, const Bytes& field,
		      std::string* ret) {
    if (slice.empty()) {
	return 0;
    }
    auto iter = std::search(slice.data(), slice.data() + slice.size(),
			    field.data(), field.data() + field.size());
    if (iter == slice.data() + slice.size()) { // not exist
	return 0;
    }
    Decoder decoder(slice.data(), slice.size());
    int len = 0;
    std::string buf;
    buf.reserve(slice.size());
    while (!decoder.is_end()) {
	std::string elem_field, elem_value;
	int field_len, value_len;
	if ((field_len = decoder.read_8_data(&elem_field)) == -1) {
	    return -1;
	}
	decoder.skip(1); // ':'
	if ((value_len = decoder.read_8_data(&elem_value)) == -1) {
	    return -1;
	}
	if (Bytes(field) == Bytes(elem_field)) {
	    ; // skip
	} else {
	    std::string encoded = encode_hash_value(elem_field, elem_value);
	    buf.append(encoded.data(), encoded.size())
		.append(1, ';');
	    len += encoded.size() + 1;
	}
	if (!decoder.is_end()) {
	    decoder.skip(1); // ';'
	}
    }
    // remove tail ';'
    if (!buf.empty()) {
	len -= 1;
    }
    // shrink() could only be applied on 'string'
    buf.resize(len);
    *ret = buf;
    return 1;
}

int get_hash_value(const Bytes& slice, const Bytes& field, std::string* value) {
    if (slice.empty()) {
	return 0;
    }
    auto iter = std::search(slice.data(), slice.data() + slice.size(),
			    field.data(), field.data() + field.size());
    if (iter == slice.data() + slice.size()) { // not exist
	return 0;
    }
    Decoder decoder(slice.data(), slice.size());
    while (!decoder.is_end()) {
	std::string elem_field, elem_value;
	int field_len, value_len;
	if ((field_len = decoder.read_8_data(&elem_field)) == -1) {
	    return -1;
	}
	decoder.skip(1); // ':'
	if ((value_len = decoder.read_8_data(&elem_value)) == -1) {
	    return -1;
	}
	if (Bytes(field) == Bytes(elem_field)) {
	    *value = elem_value;
	    return 1;
	}
	if (!decoder.is_end()) {
	    decoder.skip(1); // ';'
	}
    }
    return 0;
}

int get_hash_values(const Bytes& slice, std::deque<StrPair>& values) {
    if (slice.empty()) {
	return 0;
    }
    Decoder decoder(slice.data(), slice.size());
    while (!decoder.is_end()) {
	std::string elem_field, elem_value;
	int field_len, value_len;
	if ((field_len = decoder.read_8_data(&elem_field)) == -1) {
	    return -1;
	}
	decoder.skip(1); // ':'
	if ((value_len = decoder.read_8_data(&elem_value)) == -1) {
	    return -1;
	}
	values.push_back(std::make_pair(elem_field, elem_value));
	if (!decoder.is_end()) {
	    decoder.skip(1); // ';'
	}
    }
    return 0;
}

int get_hash_value_count(const Bytes& slice) {
    int cnt = 0;
    if (slice.empty()) {
	return cnt;
    }
    Decoder decoder(slice.data(), slice.size());
    while (!decoder.is_end()) {
	std::string elem_field, elem_value;
	int field_len, value_len;
	if ((field_len = decoder.read_8_data(&elem_field)) == -1) {
	    return -1;
	}
	decoder.skip(1); // ':'
	if ((value_len = decoder.read_8_data(&elem_value)) == -1) {
	    return -1;
	}
	if (!decoder.is_end()) {
	    decoder.skip(1); // ';'
	}
	cnt++;
    }
    return cnt;
}

int TEST_insert_update_hash_value(const Bytes& slice, const Bytes& field, const Bytes& value,
			     std::string* ret) {
    return insert_update_hash_value(slice, field, value, ret);
}

int TEST_remove_hash_value(const Bytes& slice, const Bytes& field,
		      std::string* ret) {
    return remove_hash_value(slice, field, ret);
}

int TEST_get_hash_values(const Bytes& slice, std::deque<StrPair>& values) {
    return get_hash_values(slice, values);
}

int TEST_get_hash_value_count(const Bytes& slice) {
    return get_hash_value_count(slice);
}
