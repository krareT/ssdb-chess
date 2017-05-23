
#ifndef CHESS_MERGE_H
#define CHESS_MERGE_H

#include <algorithm>
#include <deque>
#include <iostream>

#include "rocksdb/merge_operator.h"
#include "../include.h"
#include "ssdb.h"
#include "ssdb_impl.h"
#include "t_hash.h"

// The Merge Operator
//
// Essentially, a MergeOperator specifies the SEMANTICS of a merge, which only
// client knows. It could be numeric addition, list append, string
// concatenation, edit data structure, ... , anything.
class ChessMergeOperator : public rocksdb::MergeOperator {
 public:
    virtual ~ChessMergeOperator() {}

    /*
    struct MergeOperationInput {
	explicit MergeOperationInput(const Slice& _key,
				     const Slice* _existing_value,
				     const std::vector<Slice>& _operand_list,
				     Logger* _logger)
	    : key(_key),
	    existing_value(_existing_value),
	    operand_list(_operand_list),
	    logger(_logger) {}

	// The key associated with the merge operation.
	const Slice& key;
	// The existing value of the current key, nullptr means that the
	// value dont exist.
	const Slice* existing_value;
	// A list of operands to apply.
	const std::vector<Slice>& operand_list;
	// Logger could be used by client to log any errors that happen during
	// the merge operation.
	Logger* logger;
    };

    struct MergeOperationOutput {
	explicit MergeOperationOutput(std::string& _new_value,
				      Slice& _existing_operand)
	    : new_value(_new_value), existing_operand(_existing_operand) {}

	// Client is responsible for filling the merge result here.
	std::string& new_value;
	// If the merge result is one of the existing operands (or existing_value),
	// client can set this field to the operand (or existing_value) instead of
	// using new_value.
	Slice& existing_operand;
    };
    */
    // Gives the client a way to express the read -> modify -> write semantics
    // key:      (IN)    The key that's associated with this merge operation.
    //                   Client could multiplex the merge operator based on it
    //                   if the key space is partitioned and different subspaces
    //                   refer to different types of data which have different
    //                   merge operation semantics
    // existing: (IN)    null indicates that the key does not exist before this op
    // operand_list:(IN) the sequence of merge operations to apply, front() first.
    // new_value:(OUT)   Client is responsible for filling the merge result here.
    // The string that new_value is pointing to will be empty.
    // logger:   (IN)    Client could use this to log errors during merge.
    //
    // Return true on success.
    // All values passed in will be client-specific values. So if this method
    // returns false, it is because client specified bad data or there was
    // internal corruption. This will be treated as an error by the library.
    //
    // Also make use of the *logger for error messages.
    // TBD(kg): optimize string related ops...
    virtual bool FullMergeV2(const MergeOperationInput& merge_in,
			     MergeOperationOutput* merge_out) const {
	// keep newest at front() in arr
	std::deque<StrPair> arr;
	static const int ExtraLen = 4;
	int len = 0;
	for (int i = merge_in.operand_list.size() - 1; i >= 0; i--) {
	    const auto& item = merge_in.operand_list[i];
	    Bytes slice(item.data(), item.size());
	    std::string field, value;
	    int ret = decode_hash_value(slice, &field, &value);
	    if (ret == -1) {
		return false;
	    }
	    auto it = std::find_if(arr.begin(), arr.end(), [&field](const StrPair& p) {
		    return (p.first == field);
		});
	    if (it == arr.end()) {
		arr.push_back(std::make_pair(field, value));
		len += field.length() + value.length() + ExtraLen;
	    }
	}
	// filter existing value as well
	if (merge_in.existing_value && !merge_in.existing_value->empty()) {
	    std::deque<StrPair> exists;
	    Bytes slice(merge_in.existing_value->data(), merge_in.existing_value->size());
	    if (get_hash_values(slice, exists) == -1) {
		return false;
	    }
	    for (auto& item : exists) {
		auto it = std::find_if(arr.begin(), arr.end(), [&item](const StrPair& p) {
			return (p.first == item.first);
		    });
		if (it == arr.end()) {
		    arr.push_back(std::make_pair(item.first, item.second));
		    len += item.first.length() + item.second.length() + ExtraLen;
		}
	    }
	}
	//
	std::string& new_value = merge_out->new_value;
	new_value.clear();  new_value.reserve(len);
	len = 0;
	for (auto& item : arr) {
	    if (!isDeleted(item.first, item.second)) {
		std::string& field = item.first, &value = item.second;
		new_value.append(kFieldByteLen, (uint8_t)field.size());
		new_value.append(field.data(), field.size());
		new_value.append(1, ':');
		new_value.append(kValueByteLen, (uint8_t)value.size());
		new_value.append(value.data(), value.size());
		new_value.append(1, ';');
		len += kFieldByteLen + field.size() + 1 + kValueByteLen + value.size() + 1;
	    }
	}
	if (!new_value.empty()) {
	    new_value.resize(len - 1);
	}
	return true;
    }

    bool isDeleted(const std::string& field, const std::string& value) const {
	return (value == "_deleted_");
    }

    // This function performs merge(left_op, right_op)
    // when both the operands are themselves merge operation types.
    // Save the result in *new_value and return true. If it is impossible
    // or infeasible to combine the two operations, return false instead.
    /*virtual bool PartialMerge(const rocksdb::Slice& key,
			      const rocksdb::Slice& left_operand,
			      const rocksdb::Slice& right_operand,
			      std::string* new_value,
			      Logger* logger) const {
	//return true;
	return false;
	}*/

    // The name of the MergeOperator. Used to check for MergeOperator
    // mismatches (i.e., a DB created with one MergeOperator is
    // accessed using a different MergeOperator)
    virtual const char* Name() const {
	return "Chessmergeoperator";
    }
};

#endif
