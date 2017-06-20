
#ifndef CHESS_MERGE_H
#define CHESS_MERGE_H

#include <algorithm>
#include <deque>
#include <iostream>

#include "rocksdb/merge_operator.h"
#include "../include.h"
#include "hash_encoder.h"
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
	ChessMergeOperator() {
		_encoder = new ChessHashEncoder;
	}
	
    virtual ~ChessMergeOperator() {}
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
		for (int i = merge_in.operand_list.size() - 1; i >= -1; i--) {
			std::deque<StrPair> exists;
			if (i >= 0) {
				const auto& item = merge_in.operand_list[i];
				Bytes slice(item.data(), item.size());
				if (get_hash_values(slice, exists) == -1) {
					return false;
				}
			} else if (merge_in.existing_value && !merge_in.existing_value->empty()) {
				// filter existing value as well
				Bytes slice(merge_in.existing_value->data(), merge_in.existing_value->size());
				if (get_hash_values(slice, exists) == -1) {
					return false;
				}
			} else {
				break;
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
		std::string& new_value = merge_out->new_value;
		new_value.clear();  new_value.reserve(len);
		len = 0;
		for (auto& item : arr) {
			if (!isDeleted(item.first, item.second)) {
				std::string val = _encoder->encode_value(item.first, item.second);
				new_value.append(val.data(), val.size());
				new_value.append(1, ';');
				len += val.size() + 1;
			}
		}
		if (!new_value.empty()) {
			new_value.resize(len - 1);  // skip the last ';'
		}
		return true;
    }

    bool isDeleted(const std::string& field, const std::string& value) const {
		return (value == kDelTag);
    }

    // This function performs merge(left_op, right_op)
    // when both the operands are themselves merge operation types
    // that you would have passed to a DB::Merge() call in the same order
    // (i.e.: DB::Merge(key,left_op), followed by DB::Merge(key,right_op)).
    //
    // PartialMerge should combine them into a single merge operation that is
    // saved into *new_value, and then it should return true.
    // *new_value should be constructed such that a call to
    // DB::Merge(key, *new_value) would yield the same result as a call
    // to DB::Merge(key, left_op) followed by DB::Merge(key, right_op).
    //
    // The string that new_value is pointing to will be empty.
    //
    // The default implementation of PartialMergeMulti will use this function
    // as a helper, for backward compatibility.  Any successor class of
    // MergeOperator should either implement PartialMerge or PartialMergeMulti,
    // although implementing PartialMergeMulti is suggested as it is in general
    // more effective to merge multiple operands at a time instead of two
    // operands at a time.
    //
    // If it is impossible or infeasible to combine the two operations,
    // leave new_value unchanged and return false. The library will
    // internally keep track of the operations, and apply them in the
    // correct order once a base-value (a Put/Delete/End-of-Database) is seen.
    //
    // TODO: Presently there is no way to differentiate between error/corruption
    // and simply "return false". For now, the client should simply return
    // false in any case it cannot perform partial-merge, regardless of reason.
    // If there is corruption in the data, handle it in the FullMergeV2() function
    // and return false there.  The default implementation of PartialMerge will
    // always return false.
    virtual bool PartialMerge(const rocksdb::Slice& key, const rocksdb::Slice& left_operand,
							  const rocksdb::Slice& right_operand, std::string* new_value,
							  Logger* logger) const {
		std::deque<StrPair> arr;
		static const int ExtraLen = 4;
		int len = 0;
		for (int i = 0; i < 2; i++) {
			std::deque<StrPair> exists;
			if (i == 0) { // right is newer
				Bytes slice(right_operand.data(), right_operand.size());
				if (get_hash_values(slice, exists) == -1) {
					return false;
				}
			} else {
				Bytes slice(left_operand.data(), left_operand.size());
				if (get_hash_values(slice, exists) == -1) {
					return false;
				}
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
		new_value->clear();  new_value->reserve(len);
		len = 0;
		for (auto& item : arr) {
			if (!isDeleted(item.first, item.second)) {
				std::string val = _encoder->encode_value(item.first, item.second);
				new_value->append(val.data(), val.size());
				new_value->append(1, ';');
				len += val.size() + 1;
			}
		}
		if (!new_value->empty()) {
			new_value->resize(len - 1); // skip the last ';'
		}
		return true;
    }

    // The name of the MergeOperator. Used to check for MergeOperator
    // mismatches (i.e., a DB created with one MergeOperator is
    // accessed using a different MergeOperator)
    virtual const char* Name() const {
		return "Chessmergeoperator";
    }
	
 private:
	HashEncoder* _encoder;
};

#endif
