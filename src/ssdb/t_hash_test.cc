
#include "gtest/gtest.h"

#include "../include.h"
#include "const.h"
#include "ssdb.h"
#include "ssdb_impl.h"
#include "chess_merger.h"

int Factorial(int n) {
    if (n == 1 || n == 2) return 1;
    return Factorial(n - 1) * Factorial(n - 2);
}

using std::cout;
using std::endl;

static const std::string kDBPath = "./testdb";

// The fixture for testing class Foo.
class THashTest : public ::testing::Test {
protected:
    virtual void SetUp() {
	// Code here will be called immediately after the constructor (right
	// before each test).
	Options options;

	// open DB
	_ssdb = SSDB::open(Options(), kDBPath);
	//rocksdb::Status s = rocksdb::DB::Open(options, kDBPath, &_db);
	//assert(s.ok());
    }

    virtual void TearDown() {
	// Code here will be called immediately after each test (right
	// before the destructor).
	delete _ssdb;

	std::string cmd = "rm -rf " + kDBPath + "/*";
	system (cmd.c_str());
    }

    // Objects declared here can be used by all tests in the test case for ....
protected:
    rocksdb::DB* _db;
    SSDB *_ssdb;
};

TEST_F(THashTest, SetAndCnt) {

    std::string key1 = "key1", field1 = "field1";
    int cnt = _ssdb->hsize(key1);
    ASSERT_EQ(0, cnt);
	
    int ret = _ssdb->hset(key1, field1, "value1", BinlogCommand::HSET);
    ASSERT_NE(-1, ret);
    cnt = _ssdb->hsize(key1);
    ASSERT_EQ(1, cnt);

    std::string field2 = "field2";
    ret = _ssdb->hset(key1, field2, "value2", BinlogCommand::HSET);
    cnt = _ssdb->hsize(key1);
    ASSERT_EQ(2, cnt);

    // update field1 & field2
    ret = _ssdb->hset(key1, field1, "value1-update", BinlogCommand::HSET);
    cnt = _ssdb->hsize(key1);
    ASSERT_EQ(2, cnt);

    ret = _ssdb->hset(key1, field2, "value2-update", BinlogCommand::HSET);
    cnt = _ssdb->hsize(key1);
    ASSERT_EQ(2, cnt);
}

TEST_F(THashTest, SetAndGet) {
    std::string key1 = "key1", field1 = "field1", value1 = "value1";
    int cnt = _ssdb->hsize(key1);
    ASSERT_EQ(0, cnt);

    int ret = _ssdb->hset(key1, field1, value1, BinlogCommand::HSET);
 
    std::string result;
    _ssdb->hget(key1, field1, &result);
    EXPECT_EQ(value1, result);

    std::string field2 = "field2", value2 = "value2";
    ret = _ssdb->hset(key1, field2, value2, BinlogCommand::HSET);
    cnt = _ssdb->hsize(key1);
    ASSERT_EQ(2, cnt);

    result = "";
    _ssdb->hget(key1, field2, &result);
    EXPECT_EQ(value2, result);
}

TEST_F(THashTest, DelAndCnt) {

    std::string key1 = "key1", field1 = "field1", field2 = "field2";
    int ret = _ssdb->hset(key1, field1, "value1", BinlogCommand::HSET);
    int cnt = _ssdb->hsize(key1);
    ASSERT_EQ(1, cnt);

    ret = _ssdb->hdel(key1, field1, BinlogCommand::HSET);
    std::string val;
    ret = _ssdb->hget(key1, &val);
	
    cnt = _ssdb->hsize(key1);
    ASSERT_EQ(0, cnt);

    // add 2 fields, remove 1st one
    _ssdb->hset(key1, field1, "value1", BinlogCommand::HSET);
    _ssdb->hset(key1, field2, "value2", BinlogCommand::HSET);
    ret = _ssdb->hdel(key1, field1, BinlogCommand::HSET);
    cnt = _ssdb->hsize(key1);
    ASSERT_EQ(1, cnt);

    _ssdb->hclear(key1);
    cnt = _ssdb->hsize(key1);
    ASSERT_EQ(0, cnt);
}

TEST_F(THashTest, ListKeys) {

    std::string key1 = "key1", field1 = "field1";
    std::string key2 = "key2";
    std::vector<std::string> names;
    _ssdb->hlist(key1, key2, 100, &names);
    ASSERT_EQ(0, names.size());
    
    _ssdb->hset(key1, field1, "value1", BinlogCommand::HSET);
    _ssdb->hset(key2, field1, "value1", BinlogCommand::HSET);
    
    _ssdb->hlist(key1, key2, 100, &names);
    ASSERT_EQ(2, names.size());
    ASSERT_TRUE(names[0] == "key1");
    ASSERT_TRUE(names[1] == "key2");
}

TEST_F(THashTest, Scan) {
    std::string key1 = "key1",
	field1 = "field1", field2 = "field2",
	value1 = "value1", value2 = "value2";
    
    int ret = _ssdb->hset(key1, field1, value1, BinlogCommand::HSET);
    _ssdb->hset(key1, field2, value2, BinlogCommand::HSET);

    HIterator* iter = _ssdb->hscan(key1, "", "", 100);
    std::deque<StrPair> arr;
    while (iter->next()) {
	arr.push_back(std::make_pair(iter->_field, iter->_value));
    }

    ASSERT_EQ(2, arr.size());
    ASSERT_TRUE(arr[0].first == field2);
    ASSERT_TRUE(arr[0].second == value2);
    ASSERT_TRUE(arr[1].first == field1);
    ASSERT_TRUE(arr[1].second == value1);

    // empty
    iter = _ssdb->hscan("not exist", "", "", 100);
    ASSERT_FALSE(iter->next());
}

TEST(FullMergerTest, EmptyExistingTest) {
    std::string key, field, value;
    std::string new_value, result, expected;
    
    std::vector<rocksdb::Slice> operands;
    rocksdb::Slice existing_operand;
    ChessMergeOperator chessMerger;

    {
	// two inserts
	operands.clear();

	field = "a3b4"; value = "112";
	std::string ep1 = encode_hash_value(field, value);
	operands.push_back(ep1);

	field = "a3b3"; value = "-11159";
	std::string ep2 = encode_hash_value(field, value);
	operands.push_back(ep2);

	new_value = "";
	expected = ep2 + ";" + ep1;
	rocksdb::MergeOperator::MergeOperationInput merge_in(key, nullptr,
						    operands, nullptr);
	rocksdb::MergeOperator::MergeOperationOutput merge_out(new_value, existing_operand);
	chessMerger.FullMergeV2(merge_in, &merge_out);

	EXPECT_EQ(new_value, expected);
    }

    {
	// one insert, one update
	operands.clear();
	
	field = "a3b4"; value = "112";
	std::string ep1 = encode_hash_value(field, value);
	operands.push_back(ep1);

	field = "a3b4"; value = "-11159";
	std::string ep2 = encode_hash_value(field, value);
	operands.push_back(ep2);

	new_value = "";
	expected = ep2;
	rocksdb::MergeOperator::MergeOperationInput merge_in(key, nullptr,
						    operands, nullptr);
	rocksdb::MergeOperator::MergeOperationOutput merge_out(new_value, existing_operand);
	chessMerger.FullMergeV2(merge_in, &merge_out);

	EXPECT_EQ(new_value, expected);
    }
    {
	// one insert, one delete
	operands.clear();

	field = "a3b4"; value = "112";
	std::string ep1 = encode_hash_value(field, value);
	operands.push_back(ep1);

	field = "a3b4"; value = "_deleted_";
	std::string ep2 = encode_hash_value(field, value);
	operands.push_back(ep2);

	expected = ""; new_value = "";
	rocksdb::MergeOperator::MergeOperationInput merge_in(key, nullptr,
						    operands, nullptr);
	rocksdb::MergeOperator::MergeOperationOutput merge_out(new_value, existing_operand);
	chessMerger.FullMergeV2(merge_in, &merge_out);

	EXPECT_EQ(new_value, expected);
    }
    {
	// one delete, one insert
	operands.clear();
	field = value = expected = "";
	field = "a3b4"; value = "_deleted_";
	std::string ep1 = encode_hash_value(field, value);
	operands.push_back(ep1);
	
	field = "a3b4"; value = "112";
	std::string ep2 = encode_hash_value(field, value);
	operands.push_back(ep2);

	new_value = "";
	expected = ep2;
	rocksdb::MergeOperator::MergeOperationInput merge_in(key, nullptr,
						    operands, nullptr);
	rocksdb::MergeOperator::MergeOperationOutput merge_out(new_value, existing_operand);
	chessMerger.FullMergeV2(merge_in, &merge_out);

	EXPECT_EQ(new_value, expected);
    }
    {
	// two inserts, one update
	operands.clear();

	field = "a3b4"; value = "112";
	std::string ep1 = encode_hash_value(field, value);
	operands.push_back(ep1);

	field = "a3b3"; value = "-11159";
	std::string ep2 = encode_hash_value(field, value);
	operands.push_back(ep2);

	field = "a3b4"; value = "11445";
	std::string ep3 = encode_hash_value(field, value);
	operands.push_back(ep3);

	new_value = "";
	expected = ep3 + ";" + ep2;
	rocksdb::MergeOperator::MergeOperationInput merge_in(key, nullptr,
						    operands, nullptr);
	rocksdb::MergeOperator::MergeOperationOutput merge_out(new_value, existing_operand);
	chessMerger.FullMergeV2(merge_in, &merge_out);

	EXPECT_EQ(new_value, expected);
    }
    {
	// two inserts, one delete
	operands.clear();

	field = "a3b4"; value = "112";
	std::string ep1 = encode_hash_value(field, value);
	operands.push_back(ep1);

	field = "a3b3"; value = "-11159";
	std::string ep2 = encode_hash_value(field, value);
	operands.push_back(ep2);

	field = "a3b3"; value = "_deleted_";
	std::string ep3 = encode_hash_value(field, value);
	operands.push_back(ep3);

	new_value = "";
	expected = ep1;
	rocksdb::MergeOperator::MergeOperationInput merge_in(key, nullptr,
						    operands, nullptr);
	rocksdb::MergeOperator::MergeOperationOutput merge_out(new_value, existing_operand);
	chessMerger.FullMergeV2(merge_in, &merge_out);

	EXPECT_EQ(new_value, expected);
    }
}

TEST(FullMergerTest, NoneEmptyExistingTest) {
    std::string key, field, value;
    std::string new_value, result, expected;

    // prepare existing_value
    key = "";
    std::string field1("a3b4"), value1("112"),
	field2("a3b3"), value2("-11159");
    std::string ep1 = encode_hash_value(field1, value1),
	ep2 = encode_hash_value(field2, value2);

    rocksdb::Slice existing_operand;
    std::vector<rocksdb::Slice> operands;
    ChessMergeOperator chessMerger;
    {
	// one insert
	operands.clear();
	new_value = "";
	std::string existing_value = ep1 + ";" + ep2;
	rocksdb::Slice slice(existing_value);
	
	field = "b3b4"; value = "112";
	std::string temp = encode_hash_value(field, value);
	operands.push_back(temp);

	expected = temp + ";" + existing_value;
	rocksdb::MergeOperator::MergeOperationInput merge_in(key, &slice,
						    operands, nullptr);
	rocksdb::MergeOperator::MergeOperationOutput merge_out(new_value, existing_operand);
	chessMerger.FullMergeV2(merge_in, &merge_out);

	EXPECT_EQ(new_value, expected);
    }
    {
	// one update
	operands.clear();
	new_value = "";
	std::string existing_value = ep1 + ";" + ep2;
	rocksdb::Slice slice(existing_value);

	value = "whatever";
	std::string temp = encode_hash_value(field1, value);
	operands.push_back(temp);

	expected = temp + ";" + ep2;
	rocksdb::MergeOperator::MergeOperationInput merge_in(key, &slice,
							     operands, nullptr);
	rocksdb::MergeOperator::MergeOperationOutput merge_out(new_value, existing_operand);
	chessMerger.FullMergeV2(merge_in, &merge_out);
	
	EXPECT_EQ(new_value, expected);
    }
    {
	// one delete
	operands.clear();
	new_value = "";
	std::string existing_value = ep1 + ";" + ep2;
	rocksdb::Slice slice(existing_value);

	value = "_deleted_";
	std::string temp = encode_hash_value(field1, value);
	operands.push_back(temp);

	expected = ep2;
	rocksdb::MergeOperator::MergeOperationInput merge_in(key, &slice,
							     operands, nullptr);
	rocksdb::MergeOperator::MergeOperationOutput merge_out(new_value, existing_operand);
	chessMerger.FullMergeV2(merge_in, &merge_out);

	EXPECT_EQ(new_value, expected);
    }
    {
	// one insert, one update, one delete
	operands.clear();
	new_value = "";
	std::string existing_value = ep1 + ";" + ep2;
	rocksdb::Slice slice(existing_value);

	field = "cdeft"; value = "112";
	std::string itemp = encode_hash_value(field, value);
	operands.push_back(itemp);

	value = "_deleted_";
	std::string dtemp = encode_hash_value(field2, value);
	operands.push_back(dtemp);

	value = "sowhat";
	std::string utemp = encode_hash_value(field1, value);
	operands.push_back(utemp);

	expected = utemp + ";" + itemp;
	rocksdb::MergeOperator::MergeOperationInput merge_in(key, &slice,
							     operands, nullptr);
	rocksdb::MergeOperator::MergeOperationOutput merge_out(new_value, existing_operand);
	chessMerger.FullMergeV2(merge_in, &merge_out);

	EXPECT_EQ(new_value, expected);
    }
}

TEST(PartialMergerTest, BaseTest) {
    std::string key, field, value;
    std::string new_value, result, expected;
    
    ChessMergeOperator chessMerger;
    {
	// two inserts
	field = "a3b4"; value = "112";
	std::string ep1 = encode_hash_value(field, value);

	field = "a3b3"; value = "-11159";
	std::string ep2 = encode_hash_value(field, value);

	new_value = "";
	expected = ep2 + ";" + ep1;
	chessMerger.PartialMerge(key, ep1, ep2, &new_value, nullptr);

	EXPECT_EQ(new_value, expected);
    }
    {
	// one insert, one update
	field = "a3b4"; value = "112";
	std::string ep1 = encode_hash_value(field, value);

	field = "a3b4"; value = "-11159";
	std::string ep2 = encode_hash_value(field, value);

	new_value = "";
	expected = ep2;
	chessMerger.PartialMerge(key, ep1, ep2, &new_value, nullptr);

	EXPECT_EQ(new_value, expected);
    }
    {
	// one insert, one delete
	field = "a3b4"; value = "112";
	std::string ep1 = encode_hash_value(field, value);

	field = "a3b4"; value = "_deleted_";
	std::string ep2 = encode_hash_value(field, value);

	expected = ""; new_value = "";
	chessMerger.PartialMerge(key, ep1, ep2, &new_value, nullptr);

	EXPECT_EQ(new_value, expected);
    }
    {
	// one delete, one insert
	field = value = expected = "";
	field = "a3b4"; value = "_deleted_";
	std::string ep1 = encode_hash_value(field, value);
	
	field = "a3b4"; value = "112";
	std::string ep2 = encode_hash_value(field, value);

	new_value = "";
	expected = ep2;
	chessMerger.PartialMerge(key, ep1, ep2, &new_value, nullptr);

	EXPECT_EQ(new_value, expected);
    }
    {
	// two inserts, one update
	field = "a3b4"; value = "112";
	std::string ep1 = encode_hash_value(field, value);

	field = "a3b3"; value = "-11159";
	std::string ep2 = encode_hash_value(field, value);

	field = "a3b4"; value = "11445";
	std::string ep3 = encode_hash_value(field, value);

	new_value = "";
	expected = ep3 + ";" + ep2;
	chessMerger.PartialMerge(key, ep1, ep3 + ";" + ep2, &new_value, nullptr);

	EXPECT_EQ(new_value, expected);
    }
    {
	// two inserts, one delete
	field = "a3b4"; value = "112";
	std::string ep1 = encode_hash_value(field, value);

	field = "a3b3"; value = "-11159";
	std::string ep2 = encode_hash_value(field, value);

	field = "a3b3"; value = "_deleted_";
	std::string ep3 = encode_hash_value(field, value);

	new_value = "";
	expected = ep1;
	chessMerger.PartialMerge(key, ep1 + ";" + ep2, ep3, &new_value, nullptr);

	EXPECT_EQ(new_value, expected);
    }
}


//TEST(ValueCntTest, BaseTest) {
void foo0() {
    std::string prev_value, key, field, value;
    std::string output, expected;
    {
	// from empty set
	prev_value = key = field = value = output = expected = "";
	int cnt = TEST_get_hash_value_count(key);
	ASSERT_EQ(cnt, 0);
    }
    
    {
	prev_value = field = value = output = expected = "";
	prev_value = ""; field = "a3b4"; value = "112";
	expected.push_back((char)field.length()); expected += field; expected.push_back(':');
	expected.push_back((char)value.length()); expected += value;
	TEST_insert_update_hash_value(prev_value, field, value, &output);
	int cnt = TEST_get_hash_value_count(output);
	ASSERT_EQ(cnt, 1);
	
	prev_value = output; field = "a3b3"; value = "-11159";
	expected.push_back(';');
	expected.push_back((char)field.length()); expected += field; expected.push_back(':');
	expected.push_back((char)value.length()); expected += value;
	TEST_insert_update_hash_value(prev_value, field, value, &output);

	EXPECT_EQ(expected, output);
	cnt = TEST_get_hash_value_count(output);
	ASSERT_EQ(cnt, 2);
    }
}

//TEST(InsertUpdateTest, BaseTest) {
void foo1() {
    std::string prev_value, field, value;
    std::string output, expected;
    {
	// from empty value
	prev_value = field = value = output = expected = "";
	prev_value = ""; field = "a3b4"; value = "112";
	expected.push_back((char)field.length()); expected += field; expected.push_back(':');
	expected.push_back((char)value.length()); expected += value;
	int ret = TEST_insert_update_hash_value(prev_value, field, value, &output);

	EXPECT_NE("a3b4:112", output);
	EXPECT_EQ(expected, output);
    }
    {  
	// insert
	prev_value = field = value = output = expected = "";
	prev_value = ""; field = "a3b4"; value = "112";
	expected.push_back((char)field.length()); expected += field; expected.push_back(':');
	expected.push_back((char)value.length()); expected += value;
	TEST_insert_update_hash_value(prev_value, field, value, &output);

	prev_value = output; field = "a3b3"; value = "-11159";
	expected.push_back(';');
	expected.push_back((char)field.length()); expected += field; expected.push_back(':');
	expected.push_back((char)value.length()); expected += value;
	TEST_insert_update_hash_value(prev_value, field, value, &output);

	EXPECT_EQ(expected, output);
    }
    {
	// update 1
	prev_value = field = value = output = expected = "";
	prev_value = ""; field = "a3b4"; value = "112";
	TEST_insert_update_hash_value(prev_value, field, value, &output);
	
	prev_value = output; field = "a3b4"; value = "-11159";
	expected.push_back((char)field.length()); expected += field; expected.push_back(':');
	expected.push_back((char)value.length()); expected += value;
	TEST_insert_update_hash_value(prev_value, field, value, &output);

	EXPECT_EQ(expected, output);
    }
    {  
	// update 2
	prev_value = field = value = output = expected = "";
	prev_value = ""; field = "a3b4"; value = "112";
	expected.push_back((char)field.length()); expected += field; expected.push_back(':');
	expected.push_back((char)value.length()); expected += value;
	TEST_insert_update_hash_value(prev_value, field, value, &output);

	prev_value = output; field = "a3b3"; value = "-11159";
	TEST_insert_update_hash_value(prev_value, field, value, &output);

	prev_value = output; field = "a3b3"; value = "1724";
	expected.push_back(';');
	expected.push_back((char)field.length()); expected += field; expected.push_back(':');
	expected.push_back((char)value.length()); expected += value;

	int ret = TEST_insert_update_hash_value(prev_value, field, value, &output);
	EXPECT_EQ(expected, output);
    }
}

//TEST(RemoveTest, BaseTest) {
void foo2() {
    std::string prev_value, field, value;
    std::string output, expected;
    {
	// remove none-exist 1
	prev_value = field = value = output = expected = "";
	prev_value = ""; field = "a3b4"; value = "112";
	expected = "";
	int ret = TEST_remove_hash_value(prev_value, field, &output);

	ASSERT_EQ(0, ret);
	EXPECT_EQ(expected, output);
    }
    {
	// remove none-exist 2
	prev_value = field = value = output = expected = "";
	prev_value = ""; field = "a3b4"; value = "112";
	expected.push_back((char)field.length()); expected += field; expected.push_back(':');
	expected.push_back((char)value.length()); expected += value;
	TEST_insert_update_hash_value(prev_value, field, value, &output);

	prev_value = output; field = "sdfsafds";
	int ret = TEST_remove_hash_value(prev_value, field, &output);

	ASSERT_EQ(0, ret);
	EXPECT_EQ(expected, output);
    }
    {
	// remove exist 1
	prev_value = field = value = output = expected = "";
	prev_value = ""; field = "a3b4"; value = "112";
	expected = "";
	TEST_insert_update_hash_value(prev_value, field, value, &output);

	prev_value = output;
	int ret = TEST_remove_hash_value(prev_value, field, &output);

	ASSERT_EQ(1, ret);
	EXPECT_EQ(expected, output);
    }
    {
	// remove exist 2: remove tail
	prev_value = field = value = output = expected = "";
	prev_value = ""; field = "a3b4"; value = "112";
	expected.push_back((char)field.length()); expected += field; expected.push_back(':');
	expected.push_back((char)value.length()); expected += value;
	TEST_insert_update_hash_value(prev_value, field, value, &output);

	prev_value = output; field = "u3y4"; value = "-112";
	TEST_insert_update_hash_value(prev_value, field, value, &output);

	prev_value = output;
	int ret = TEST_remove_hash_value(prev_value, field, &output);

	
	EXPECT_EQ(expected, output);
    }
    {
	// remove exist 3: remove head
	prev_value = field = value = output = expected = "";
	prev_value = ""; field = "a3b4"; value = "112";
	TEST_insert_update_hash_value(prev_value, field, value, &output);

	prev_value = output; field = "u3y4"; value = "-112";
	expected.push_back((char)field.length()); expected += field; expected.push_back(':');
	expected.push_back((char)value.length()); expected += value;
	TEST_insert_update_hash_value(prev_value, field, value, &output);

	prev_value = output; field = "a3b4";
	int ret = TEST_remove_hash_value(prev_value, field, &output);

	ASSERT_EQ(1, ret);
	EXPECT_EQ(expected, output);
    }
}

    
int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}


