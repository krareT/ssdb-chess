/*
Copyright (c) 2012-2014 The SSDB Authors. All rights reserved.
Use of this source code is governed by a BSD-style license that can be
found in the LICENSE file.
*/
#ifndef SSDB_ITERATOR_H_
#define SSDB_ITERATOR_H_

#include <inttypes.h>
#include <deque>
#include <string>
#include "../include.h"
#include "../util/bytes.h"

namespace rocksdb{
    class Iterator;
}

class HashEncoder;
class Iterator{
 public:
    enum Direction{
	FORWARD, BACKWARD
    };
    Iterator(rocksdb::Iterator *it,
	     const std::string &end,
	     uint64_t limit,
	     Direction direction=Iterator::FORWARD);
    ~Iterator();
    bool skip(uint64_t offset);
    bool next();
    Bytes key();
    Bytes val();
 private:
    rocksdb::Iterator *it;
    std::string end;
    uint64_t limit;
    bool is_first;
    int direction;
};


class KIterator{
 public:
    std::string key;
    std::string val;

    KIterator(Iterator *it);
    ~KIterator();
    void return_val(bool onoff);
    bool next();
 private:
    Iterator *it;
    bool return_val_;
};


class HIterator{
 public:
    std::string _key;
    std::string _field;
    std::string _value;

    HIterator(Iterator *it, const Bytes &key, HashEncoder*);
    ~HIterator();
    void return_val(bool onoff);
    bool next();
 private:
    HashEncoder *_encoder;
    Iterator *_it;
    bool _return_val;
    bool _valid;
    int _index = -1;
    std::deque<StrPair> _values;
};


class ZIterator{
 public:
    std::string name;
    std::string key;
    std::string score;

    ZIterator(Iterator *it, const Bytes &name);
    ~ZIterator();
    bool skip(uint64_t offset);
    bool next();
 private:
    Iterator *it;
};


#endif
