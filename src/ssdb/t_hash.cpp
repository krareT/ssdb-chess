/*
  Copyright (c) 2012-2014 The SSDB Authors. All rights reserved.
  Use of this source code is governed by a BSD-style license that can be
  found in the LICENSE file.
*/
#include <iostream>

#include "t_hash.h"

using std::cout;
using std::endl;

static HashEncoder* gEncoder = new ChessHashEncoder;

static int hset_one(SSDBImpl *ssdb, const Bytes &key, const Bytes &field, const Bytes &val, char log_type);
static int hset_one(SSDBImpl *ssdb, const Bytes &key, const Bytes &val, char log_type);
static int hdel_one(SSDBImpl *ssdb, const Bytes &key, const Bytes &field, char log_type);
//static int incr_hsize(SSDBImpl *ssdb, const Bytes &name, int64_t incr);

/**
 * @return -1: error, 0: item updated, 1: new item inserted
 */
int SSDBImpl::hset(const Bytes &name, const Bytes &key, const Bytes &val, char log_type) {
  std::unique_lock<std::mutex> lock(_mutex);
  Transaction trans(_binlogs);
  int ret = hset_one(this, name, key, val, log_type);
  if (ret >= 0) {
    rocksdb::Status s = _binlogs->commit();
    if (!s.ok()) {
      return -1;
    }
  }
  return ret;
}

// hreplace actually
int SSDBImpl::hset(const Bytes &key, const Bytes &val, char log_type) {
  std::unique_lock<std::mutex> lock(_mutex);
  Transaction trans(_binlogs);
  int ret = hset_one(this, key, val, log_type);
  if (ret >= 0) {
    rocksdb::Status s = _binlogs->commit();
    if (!s.ok()) {
      return -1;
    }
  }
  return ret;
}

int SSDBImpl::hdel(const Bytes &name, const Bytes &key, char log_type) {
  std::unique_lock<std::mutex> lock(_mutex);
  Transaction trans(_binlogs);
  int ret = hdel_one(this, name, key, log_type);
  if (ret >= 0) {
    rocksdb::Status s = _binlogs->commit();
    if (!s.ok()) {
      return -1;
    }
  }
  return ret;
}

// only used during migration...
int SSDBImpl::migrate_hset(const std::vector<Bytes>& items, char log_type) {
  std::unique_lock<std::mutex> lock(_mutex);
  Transaction trans(_binlogs);
  bool suc = true;
  for (int i = 0; i < items.size(); i += 3) {
    int ret = hset_one(this, items[i], items[i + 1], items[i + 2], log_type);
    if (ret == -1) {
      suc = false;
      break;
    }
  }
  if (suc) {
    rocksdb::Status s = _binlogs->commit();
    if (!s.ok()) {
      return -1;
    }
  }
  return items.size() / 3;
}

int SSDBImpl::hincr(const Bytes &name, const Bytes &key, int64_t by, int64_t *new_val, char log_type){
  assert(0);
  return 0;
}

// field count under the key
int64_t SSDBImpl::hsize(const Bytes &key) {
  std::string dbkey = gEncoder->encode_key(key);
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
  std::string dbkey = gEncoder->encode_key(key);
  ldb->Delete(rocksdb::WriteOptions(), dbkey);
  return 0;
}

int SSDBImpl::hget(const Bytes &key, std::string *val) {
  std::string dbkey = gEncoder->encode_key(key);
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
  std::string dbkey = gEncoder->encode_key(key);
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

// only support iter within one key right now
HIterator* SSDBImpl::hscan(const Bytes &key, const Bytes &start, const Bytes &end,
			   uint64_t limit) {
  std::string key_start = gEncoder->encode_key(key);
  return new HIterator(this->iterator(key_start, "", limit), key, gEncoder);
}

HIterator* SSDBImpl::hrscan(const Bytes &name, 
			    const Bytes &start, const Bytes &end, uint64_t limit) {
  assert(0);
  return 0;
}

static void get_hnames(Iterator *it, std::vector<std::string> *list) {
  ChessHashEncoder encoder;
  while (it->next()) {
    Bytes ks = it->key();
    if (ks.data()[0] != DataType::HASH) {
      break;
    }
    std::string n;
    if (gEncoder->decode_key(ks, &n) == -1) {
      continue;
    }
    list->push_back(n);
  }
}

// list of keys between [key_s, key_e]
int SSDBImpl::hlist(const Bytes &key_s, const Bytes &key_e, uint64_t limit,
		    std::vector<std::string> *list) {
  std::string start = gEncoder->encode_key(key_s);
  std::string end = (!key_e.empty()) ? gEncoder->encode_key(key_e) : "";
    
  Iterator *it = this->iterator(start, end, limit);
  get_hnames(it, list);
  delete it;
  return 0;
}

int SSDBImpl::hrlist(const Bytes &name_s, const Bytes &name_e, uint64_t limit,
		     std::vector<std::string> *list) {
  assert(0);
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
  // use 'hkey' to log, not 'key'
  std::string hkey = gEncoder->encode_key(key);
  std::string new_value = gEncoder->encode_value(field, val);
  if (new_value.empty()) {
    log_error("invalid field/value %s, %s", field.data(), val.data());
    return -1;
  }
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

  std::string hkey = gEncoder->encode_key(key);
  // Get complete value from master, should Put() not Merge
  ssdb->_binlogs->Put(hkey, slice(val));
  //ssdb->_binlogs->Merge(hkey, slice(val));
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
  std::string hkey = gEncoder->encode_key(key);
  //std::string new_value = gEncoder->encode_value(field, "_deleted_");
  std::string new_value = gEncoder->encode_value(field, kDelTag);
  if (new_value.empty()) {
    log_error("invalid field %s", field.data());
    return -1;
  }
  ssdb->_binlogs->Merge(hkey, slice(new_value));
  ssdb->_binlogs->add_log(log_type, BinlogCommand::HSET, hkey);

  return 0;
}

int get_hash_value(const Bytes& slice, const Bytes& field, std::string* value) {
  if (slice.empty()) {
    return 0;
  }
  for (int i = 0; i < slice.size(); ) {
    std::string elem_field, elem_value;
    gEncoder->decode_value(slice.data() + i, &elem_field, &elem_value);
    if (Bytes(elem_field) == Bytes(field)) {
      *value = elem_value;
      return 1;
    }
    i += 5; // plus ';' as delimiter
  }
  return 0;
}

int get_hash_values(const Bytes& slice, std::deque<StrPair>& values) {
  if (slice.empty()) {
    return 0;
  }
  for (int i = 0; i < slice.size(); ) {
    std::string elem_field, elem_value;
    gEncoder->decode_value(slice.data() + i, &elem_field, &elem_value);
    values.push_back(std::make_pair(elem_field, elem_value));
    i += 5; // plus ';' as delimiter
  }
  return 0;
}

int get_hash_value_count(const Bytes& slice) {
  int cnt = 0;
  if (slice.empty()) {
    return cnt;
  }
  for (int i = 0; i < slice.size(); ) {
    std::string elem_field, elem_value;
    gEncoder->decode_value(slice.data() + i, &elem_field, &elem_value);
    i += 5; // plus ';' as delimiter
    cnt ++;
  }
  return cnt;
}
