/*
  Copyright (c) 2012-2014 The SSDB Authors. All rights reserved.
  Use of this source code is governed by a BSD-style license that can be
  found in the LICENSE file.
*/
#include "../include.h"

#include "iterator.h"
#include "t_kv.h"
#include "t_hash.h"
#include "t_zset.h"
#include "t_queue.h"
#include "../util/log.h"
#include "../util/config.h"
#include "rocksdb/iterator.h"

Iterator::Iterator(rocksdb::Iterator *it,
		   const std::string &end,
		   uint64_t limit,
		   Direction direction) {
    this->it = it;
    this->end = end;
    this->limit = limit;
    this->is_first = true;
    this->direction = direction;
}

Iterator::~Iterator(){
    delete it;
}

Bytes Iterator::key(){
    rocksdb::Slice s = it->key();
    return Bytes(s.data(), s.size());
}

Bytes Iterator::val(){
    rocksdb::Slice s = it->value();
    return Bytes(s.data(), s.size());
}

bool Iterator::skip(uint64_t offset){
    while(offset-- > 0){
	if(this->next() == false){
	    return false;
	}
    }
    return true;
}

bool Iterator::next(){
    if(limit == 0){
	return false;
    }
    if(is_first){
	is_first = false;
    }else{
	if(direction == FORWARD){
	    it->Next();
	}else{
	    it->Prev();
	}
    }

    if(!it->Valid()){
	// make next() safe to be called after previous return false.
	limit = 0;
	return false;
    }
    if(direction == FORWARD){
	if(!end.empty() && it->key().compare(end) > 0){
	    limit = 0;
	    return false;
	}
    }else{
	if(!end.empty() && it->key().compare(end) < 0){
	    limit = 0;
	    return false;
	}
    }
    limit --;
    return true;
}


/* KV */

KIterator::KIterator(Iterator *it){
    this->it = it;
    this->return_val_ = true;
}

KIterator::~KIterator(){
    delete it;
}

void KIterator::return_val(bool onoff){
    this->return_val_ = onoff;
}

bool KIterator::next(){
    while(it->next()){
	Bytes ks = it->key();
	Bytes vs = it->val();
	//dump(ks.data(), ks.size(), "z.next");
	//dump(vs.data(), vs.size(), "z.next");
	if(ks.data()[0] != DataType::KV){
	    return false;
	}
	if(decode_kv_key(ks, &this->key) == -1){
	    continue;
	}
	if(return_val_){
	    this->val.assign(vs.data(), vs.size());
	}
	return true;
    }
    return  false;
}

/* HASH */
// by employing 'key', iterator could only reply
// all {fields, value} under one key
HIterator::HIterator(Iterator *it, const Bytes &key) {
    this->_it = it;
    this->_key.assign(key.data(), key.size());
    this->_return_val = true;
    this->_index = -1;
    this->_values.clear();
}

HIterator::~HIterator(){
    delete _it;
}

void HIterator::return_val(bool onoff){
    this->_return_val = onoff;
}

// TBD(kg): refactor HIterator since we've changed the layout of key, field, value
bool HIterator::next() {
    if (_index == -1) { // init first, mutex ?
	_index = 0;
	if (_it->next()) {
	    Bytes ks = _it->key();
	    std::string key;
	    if (ks.data()[0] != DataType::HASH ||
		decode_hash_key(ks, &key) == -1 ||
		key != this->_key) {
		return false;
	    }

	    if (_return_val) {
		Bytes vs = _it->val();
		std::vector<StrPair> values;
		if (get_hash_values(Bytes(key), values) == -1) {
		    return false;
		}
	    }
	}
    }
    if (_index >= _values.size()) {
	return false;
    } else {
	this->_field = _values[_index].first;
	this->_value = _values[_index].second;
	_index++;
	return true;
    }
}

/* ZSET */

ZIterator::ZIterator(Iterator *it, const Bytes &name){
    this->it = it;
    this->name.assign(name.data(), name.size());
}

ZIterator::~ZIterator(){
    delete it;
}
		
bool ZIterator::skip(uint64_t offset){
    while(offset-- > 0){
	if(this->next() == false){
	    return false;
	}
    }
    return true;
}

bool ZIterator::next(){
    while(it->next()){
	Bytes ks = it->key();
	//Bytes vs = it->val();
	//dump(ks.data(), ks.size(), "z.next");
	//dump(vs.data(), vs.size(), "z.next");
	if(ks.data()[0] != DataType::ZSCORE){
	    return false;
	}
	if(decode_zscore_key(ks, NULL, &key, &score) == -1){
	    continue;
	}
	return true;
    }
    return false;
}
