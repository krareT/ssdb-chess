
#ifndef HASH_ENCODER_
#define HASH_ENCODER_

#include <string>
#include "../util/bytes.h"
#include "const.h"

class HashEncoder {
 public:
    virtual ~HashEncoder() {}
    virtual std::string encode_key(const Bytes&) = 0;
    virtual int decode_key(const Bytes&, std::string*) = 0;
    virtual std::string encode_value(const Bytes& field, const Bytes& value) = 0;
    virtual int decode_value(const Bytes&, std::string* field, std::string* value) = 0;
};


static const std::string kDelTag = "32767";

class ChessHashEncoder : public HashEncoder {
 public:
    std::string encode_key(const Bytes& key) override {
		std::string buf;
		buf.reserve(1 + key.size());
		buf.append(1, DataType::HASH);
		buf.append(key.data(), key.size());
		return buf;
    }

    int decode_key(const Bytes& slice, std::string* key) override {
		Decoder decoder(slice.data(), slice.size());
		if (decoder.skip(1) == -1) {
			return -1;
		}
		if (decoder.read_data(key) == -1) {
			return -1;
		}
		return 0;
    }
	
	static const int kFieldLen = 2;
	static const int kValueLen = 2;
    std::string encode_value(const Bytes& field, const Bytes& value) override {
		std::string buf;
		buf.resize(kFieldLen + kValueLen);
		int i = 0;
		{
			if (!isFieldValid(field)) { // empty means invalid input
				return std::string();
			}
			// format: [a ~ i][0 ~ 9][a ~ i][0 ~ 9]
			// -> [0 ~ 9] * 4, each with 4 bit
			const char* arr = field.data();
			char ifield_1st = (arr[0] - 'a') << 4 | (arr[1] - '0');
			char ifield_2nd = (arr[2] - 'a') << 4 | (arr[3] - '0');
			buf[i++] = ifield_1st & 0xFF;
			buf[i++] = ifield_2nd & 0xFF;
		}
		{
			// use little-endian right now
			int val = atoi(value.data());
			if (val < -30000 ||
				(val > 30000 && value != kDelTag)) {
				return std::string();
			}
			int16_t ival = static_cast<int16_t>(val);
			//int16_t ival = static_cast<int16_t>(atoi(value.data()));
			buf[i++] = ival & 0xFF;
			buf[i++] = (ival >> 8) & 0xFF;
		}
		return buf;
    }

    int decode_value(const Bytes& slice, std::string* field, std::string* value) override {
		const char* arr = slice.data();
		{
			int i = 0;
			field->resize(4);
			(*field)[i++] = ((arr[0] >> 4) & 0xF) + 'a';
			(*field)[i++] = (arr[0] & 0xF) + '0';
			(*field)[i++] = ((arr[1] >> 4) & 0xF) + 'a';
			(*field)[i++] = (arr[1] & 0xF) + '0';
		}
		{
			int i = kFieldLen;
			int16_t ival = arr[i++] & 0xFF;
			ival |=	(arr[i++] << 8) & 0xFF00;
			*value = std::to_string(ival);
		}
		return 0;
    }

	bool isFieldValid(const Bytes& field) {
		if (field.size() != 4) {
			return false;
		}
		const char* arr = field.data();
		if ('a' <= arr[0] && arr[0] <= 'i' &&
			'a' <= arr[2] && arr[2] <= 'i' &&
			'0' <= arr[1] && arr[1] <= '9' &&
			'0' <= arr[3] && arr[3] <= '9') {
			return true;
		}
		return false;
	}
};

#endif
