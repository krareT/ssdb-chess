/*
  Copyright (c) 2012-2014 The SSDB Authors. All rights reserved.
  Use of this source code is governed by a BSD-style license that can be
  found in the LICENSE file.
*/
#ifndef SSDB_HASH_H_
#define SSDB_HASH_H_

#include "ssdb_impl.h"

static const int kKeyByteLen = 1;
static const int kFieldByteLen = 1;
static const int kValueByteLen = 1;
static const int kTotalByteLen = 2;

inline static
std::string encode_hash_key(const Bytes &key) {
    std::string buf;
    buf.reserve(1 + kKeyByteLen + key.size());
    buf.append(1, DataType::HASH);
    buf.append(kKeyByteLen, (uint8_t)key.size());
    buf.append(key.data(), key.size());
    return buf;
}

inline static
int decode_hash_key(const Bytes &slice, std::string *key) {
    Decoder decoder(slice.data(), slice.size());
    if (decoder.skip(1) == -1) {
	return -1;
    }
    if (decoder.read_8_data_s(key) == -1) {
	return -1;
    }
    return 0;
}


inline static
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

/*
inline static
std::string append_hash_value(const Bytes& slice, const Bytes& field, const Bytes& value) {
    std::string encoded_value = encode_hash_value(field, value);
    if (slice.empty()) {
	return encoded_value;
    } else {
	// total len + {field1:value1} ; {field2:value2} ; ...
	std::string buf;
	buf.reserve(slice.size() + 1 + encoded_value.size());
	buf.append(slice.size(), slice.data());
	buf.append(1, ';');
	buf.append(encoded_value.size(), encoded_value.data());
    }
    }*/

// TBD(kg): string op should be optimized
static
int insert_update_hash_value(const Bytes& slice, const Bytes& field, const Bytes& value,
			     std::string* ret) {
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
	if ((field_len = decoder.read_8_data_s(&elem_field)) == -1) {
	    return -1;
	}
	decoder.skip(1);
	if ((value_len = decoder.read_8_data_s(&elem_value)) == -1) {
	    return -1;
	}
	if (Bytes(field) == Bytes(elem_field)) {
	    continue;
	} else {
	    buf.append(elem_field.data(), field_len)
		.append(1, ':')
		.append(elem_value.size(), value_len);
	    len += field_len + 1 + value_len;
	}
    }
    buf.append(1, ';')
	.append(encoded.data(), encoded.size());
    // shrink() could only be applied on 'string'
    buf.resize(len + 1 + encoded.size());
    *ret = buf;
    return 0;
}

// -1: error
// 0: not exist
// 1: done
static
int remove_hash_value(const Bytes& slice, const Bytes& field,
		      std::string* ret) {
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
	if ((field_len = decoder.read_8_data_s(&elem_field)) == -1) {
	    return -1;
	}
	decoder.skip(1);
	if ((value_len = decoder.read_8_data_s(&elem_value)) == -1) {
	    return -1;
	}
	if (Bytes(field) == Bytes(elem_field)) {
	    continue;
	} else {
	    buf.append(elem_field.data(), field_len)
		.append(1, ':')
		.append(elem_value.size(), value_len);
	    len += field_len + 1 + value_len;
	}
    }
    // shrink() could only be applied on 'string'
    buf.resize(len);
    *ret = buf;
    return 1;
}

static
int get_hash_value(const Bytes& slice, const Bytes& field, std::string* value) {
    auto iter = std::search(slice.data(), slice.data() + slice.size(),
			    field.data(), field.data() + field.size());
    if (iter == slice.data() + slice.size()) { // not exist
	return 0;
    }
    Decoder decoder(slice.data(), slice.size());
    while (!decoder.is_end()) {
	std::string elem_field, elem_value;
	int field_len, value_len;
	if ((field_len = decoder.read_8_data_s(&elem_field)) == -1) {
	    return -1;
	}
	if (Bytes(field) == Bytes(elem_field)) {
	    *value = elem_value;
	    return 1;
	} else {
	    decoder.skip(1);
	    if ((value_len = decoder.read_8_data_s(&elem_value)) == -1) {
		return -1;
	    }
	}
    }
    return 0;
}

static
int decode_hash_value(const Bytes &slice, std::string *field, std::string *value) {
    Decoder decoder(slice.data(), slice.size());
    if (decoder.read_8_data_s(field) == -1) {
	return -1;
    }
    if (decoder.skip(1) == -1) { // field:value, separater
	return -1;
    }
    if (decoder.read_8_data_s(value) == -1) {
	return -1;
    }
    return 0;
}

inline static
int find_hash_value(const Bytes& slice, const Bytes& field, std::string* value) {
    Decoder decoder(slice.data(), slice.size());
    if (decoder.is_end()) {
	// TBD(kg): some log needed
	return -1;
    }
    decoder.skip(kTotalByteLen);
    while (!decoder.is_end()) {
	std::string elem_field, elem_value;
	if (decoder.read_8_data_s(&elem_field) == -1) {
	    return -1;
	}
	decoder.skip(1);
	if (decoder.read_8_data_s(&elem_value) == -1) {
	    return -1;
	}
	if (field == elem_field) {
	    *value = elem_value;
	    return 0;
	}
    }
    return -1;
}

#endif
