
#ifndef HASH_ENCODER_
#define HASH_ENCODER_

#include <string>

#include "../util/bytes.h"

class HashEncoder {
 public:
    virtual std::string encode_key(const Bytes&) = 0;
    virtual int decode_key(const Bytes&, std::string*) = 0;
    virtual std::string encode_value(const Bytes& field, const Bytes& value) = 0;
    virtual int decode_value(const Bytes&, std::string* field, std::string* value) = 0;
};

class ChessHashEncoder : public HashEncoder {
 public:
    std::string encode_key(const Bytes&) override {
		std::string buf;
		buf.reserve(1 + key.size());
		buf.append(1, DataType::HASH);
		buf.append(key.data(), key.size());
		return buf;
    }

    int decode_key(const Bytes&, std::string*) override {
		Decoder decoder(slice.data(), slice.size());
		if (decoder.skip(1) == -1) {
			return -1;
		}
		if (decoder.read_data(key) == -1) {
			return -1;
		}
		return 0;
    }
	
	static const int kFieldLen = 4;
	static const int kDelimLen = 1;
	static const int kValueLen = 2;
    std::string encode_value(const Bytes& field, const Bytes& value) override {
		std::string buf;
		buf.reserve(kFieldLen + kDelimLen + kValueLen);
		buf.append(field.data(), fieldLen);
		buf.append(1, ':');
		{
			int i = kFieldLen + kDelimLen;
			int16 ival = static_cast<int16>(atoi(value.data()));
			buf.append(ival, kValueLen);
			// use little-endian right now
			//buf[i++] = ival & 0xFF;
			//buf[i++] = (ival >> 8) & 0xFF;
		}
		return buf;
    }

    int decode_value(const Bytes& slice, std::string* field, std::string* value) override {
		field->assign(slice.data(), kFieldLen);
		value->assign(slice.data() + kFieldLen, kDelimLen, kValueLen);
		return 0;
    }
};

#endif
