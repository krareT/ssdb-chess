#include "../include.h"
#include "const.h"
#include "ssdb.h"
#include "ssdb_impl.h"
#include "t_hash.h"
#include "hash_encoder.h"
#include "chess_merger.h"

int Factorial(int n) {
  if (n == 1 || n == 2) return 1;
  return Factorial(n - 1) * Factorial(n - 2);
 }

using std::cout;
using std::endl;

static HashEncoder* gEncoder = new ChessHashEncoder;
const std::string kDBPath = "./testdb";

rocksdb::DB* _db;
SSDB *_ssdb;

void SetUp(const std::string& msg) {
  Options options;
  _ssdb = SSDB::open(options, kDBPath);
  //rocksdb::Status s = rocksdb::DB::Open(options, kDBPath, &_db);
  //assert(s.ok());
  std::cout << msg;
}

void TearDown(const std::string& msg) {
  std::cout << msg;
  delete _ssdb;
  std::string cmd = "rm -rf " + kDBPath + "/*";
  system (cmd.c_str());
}

void THashTest_SetAndCnt() {
  SetUp("==== THashTest_SetAndCnt start\n");

  std::string key1 = "key1", field1 = "e7b8";
  int cnt = _ssdb->hsize(key1);
  assert(0 == cnt);
	
  int ret = _ssdb->hset(key1, field1, "04", BinlogCommand::HSET);
  assert(-1 != ret);
  cnt = _ssdb->hsize(key1);
  assert(1 == cnt);

  std::string field2 = "b8e9";
  ret = _ssdb->hset(key1, field2, "14", BinlogCommand::HSET);
  cnt = _ssdb->hsize(key1);
  assert(2 == cnt);

  // update field1 & field2
  ret = _ssdb->hset(key1, field1, "-4", BinlogCommand::HSET);
  cnt = _ssdb->hsize(key1);
  assert(2 == cnt);

  ret = _ssdb->hset(key1, field2, "-14", BinlogCommand::HSET);
  cnt = _ssdb->hsize(key1);
  assert(2 == cnt);

  TearDown("\tdone\n");
}

void THashTest_SetAndGet() {
  SetUp("==== THashTest_SetAndGet start\n");

  std::string key1 = "key1", field1 = "e7b8", value1 = "30000";
  int cnt = _ssdb->hsize(key1);
  assert(0 == cnt);

  _ssdb->hset(key1, field1, value1, BinlogCommand::HSET);
 
  std::string result;
  _ssdb->hget(key1, field1, &result);
  assert(value1 == result);

  std::string field2 = "a0b0", value2 = "-30000";
  _ssdb->hset(key1, field2, value2, BinlogCommand::HSET);
  cnt = _ssdb->hsize(key1);
  assert(2 == cnt);

  result = "";
  _ssdb->hget(key1, field2, &result);
  assert(value2 == result);
    
  TearDown("\tdone\n");
}

void THashTest_DelAndCnt() {
  SetUp("==== THashTest_DelAndCnt start\n");

  std::string key1 = "key1", field1 = "a9i9", field2 = "b2c3";
  _ssdb->hset(key1, field1, "0", BinlogCommand::HSET);
  int cnt = _ssdb->hsize(key1);
  assert(1 == cnt);

  _ssdb->hdel(key1, field1, BinlogCommand::HSET);
  std::string val;
  _ssdb->hget(key1, &val);
	
  cnt = _ssdb->hsize(key1);
  assert(0 == cnt);

  // add 2 fields, remove 1st one
  _ssdb->hset(key1, field1, "1414", BinlogCommand::HSET);
  _ssdb->hset(key1, field2, "4141", BinlogCommand::HSET);
  _ssdb->hdel(key1, field1, BinlogCommand::HSET);
  cnt = _ssdb->hsize(key1);
  assert(1 == cnt);

  _ssdb->hclear(key1);
  cnt = _ssdb->hsize(key1);
  assert(0 == cnt);

  TearDown("\tdone\n");
}


void THashTest_ListKeys() {
  SetUp("==== THashTest_ListKeys start\n");

  std::string key1 = "key1", field1 = "a7b4";
  std::string key2 = "key2";
  std::vector<std::string> names;
  _ssdb->hlist(key1, key2, 100, &names);
  assert(0 == names.size());
    
  _ssdb->hset(key1, field1, "-2134", BinlogCommand::HSET);
  _ssdb->hset(key2, field1, "29999", BinlogCommand::HSET);
    
  _ssdb->hlist(key1, key2, 100, &names);
  assert(2 == names.size());
  assert(names[0] == "key1");
  assert(names[1] == "key2");

  TearDown("\tdone\n");
}

void THashTest_Scan() {
  SetUp("==== THashTest_Scan\n");

  std::string key1 = "key1",
    field1 = "f9h7", field2 = "c4d4",
    value1 = "-900", value2 = "900";
    
  _ssdb->hset(key1, field1, value1, BinlogCommand::HSET);
  _ssdb->hset(key1, field2, value2, BinlogCommand::HSET);

  HIterator* iter = _ssdb->hscan(key1, "", "", 100);
  std::deque<StrPair> arr;
  while (iter->next()) {
    arr.push_back(std::make_pair(iter->_field, iter->_value));
  }

  assert(2 == arr.size());
  assert(arr[0].first == field2);
  assert(arr[0].second == value2);
  assert(arr[1].first == field1);
  assert(arr[1].second == value1);

  // empty
  iter = _ssdb->hscan("not exist", "", "", 100);
  assert(iter->next() == false);

  TearDown("\tdone\n");
}

void FullMergerTest_EmptyExistingTest() {
  SetUp("==== FullMergerTest_EmptyExistingTest\n");

  std::string key, field, value;
  std::string new_value, result, expected;
    
  std::vector<rocksdb::Slice> operands;
  rocksdb::Slice existing_operand;
  ChessMergeOperator chessMerger;

  {
    // two inserts
    operands.clear();

    field = "a3b4"; value = "112";
    std::string ep1 = gEncoder->encode_value(field, value);
    operands.push_back(ep1);
		
    field = "a3b3"; value = "-11159";
    std::string ep2 = gEncoder->encode_value(field, value);
    operands.push_back(ep2);

    new_value = "";
    expected= ep2 + ";" + ep1;
    rocksdb::MergeOperator::MergeOperationInput merge_in(key, nullptr,
							 operands, nullptr);
    rocksdb::MergeOperator::MergeOperationOutput merge_out(new_value, existing_operand);
    chessMerger.FullMergeV2(merge_in, &merge_out);

    assert(new_value == expected);
  }
  {
    // one insert, one update
    operands.clear();
	
    field = "a3b4"; value = "112";
    std::string ep1 = gEncoder->encode_value(field, value);
    operands.push_back(ep1);

    field = "a3b4"; value = "-11159";
    std::string ep2 = gEncoder->encode_value(field, value);
    operands.push_back(ep2);

    new_value = "";
    expected = ep2;
    rocksdb::MergeOperator::MergeOperationInput merge_in(key, nullptr,
							 operands, nullptr);
    rocksdb::MergeOperator::MergeOperationOutput merge_out(new_value, existing_operand);
    chessMerger.FullMergeV2(merge_in, &merge_out);

    assert(new_value == expected);
  }
  {
    // one insert, one delete
    operands.clear();

    field = "a3b4"; value = "112";
    std::string ep1 = gEncoder->encode_value(field, value);
    operands.push_back(ep1);

    field = "a3b4"; value = kDelTag;
    std::string ep2 = gEncoder->encode_value(field, value);
    operands.push_back(ep2);

    expected = ""; new_value = "";
    rocksdb::MergeOperator::MergeOperationInput merge_in(key, nullptr,
							 operands, nullptr);
    rocksdb::MergeOperator::MergeOperationOutput merge_out(new_value, existing_operand);
    chessMerger.FullMergeV2(merge_in, &merge_out);

    assert(new_value == expected);
  }
  {
    // one delete, one insert
    operands.clear();
    field = value = expected = "";
    field = "a3b4"; value = kDelTag;
    std::string ep1 = gEncoder->encode_value(field, value);
    operands.push_back(ep1);
	
    field = "a3b4"; value = "112";
    std::string ep2 = gEncoder->encode_value(field, value);
    operands.push_back(ep2);

    new_value = "";
    expected = ep2;
    rocksdb::MergeOperator::MergeOperationInput merge_in(key, nullptr,
							 operands, nullptr);
    rocksdb::MergeOperator::MergeOperationOutput merge_out(new_value, existing_operand);
    chessMerger.FullMergeV2(merge_in, &merge_out);

    assert(new_value == expected);
  }
  {
    // two inserts, one update
    operands.clear();

    field = "a3b4"; value = "112";
    std::string ep1 = gEncoder->encode_value(field, value);
    operands.push_back(ep1);

    field = "a3b3"; value = "-11159";
    std::string ep2 = gEncoder->encode_value(field, value);
    operands.push_back(ep2);

    field = "a3b4"; value = "11445";
    std::string ep3 = gEncoder->encode_value(field, value);
    operands.push_back(ep3);

    new_value = "";
    expected = ep3 + ";" + ep2;
    rocksdb::MergeOperator::MergeOperationInput merge_in(key, nullptr,
							 operands, nullptr);
    rocksdb::MergeOperator::MergeOperationOutput merge_out(new_value, existing_operand);
    chessMerger.FullMergeV2(merge_in, &merge_out);

    assert(new_value == expected);
  }
  {
    // two inserts, one delete
    operands.clear();

    field = "a3b4"; value = "112";
    std::string ep1 = gEncoder->encode_value(field, value);
    operands.push_back(ep1);

    field = "a3b3"; value = "-11159";
    std::string ep2 = gEncoder->encode_value(field, value);
    operands.push_back(ep2);

    field = "a3b3"; value = kDelTag;
    std::string ep3 = gEncoder->encode_value(field, value);
    operands.push_back(ep3);

    new_value = "";
    expected = ep1;
    rocksdb::MergeOperator::MergeOperationInput merge_in(key, nullptr,
							 operands, nullptr);
    rocksdb::MergeOperator::MergeOperationOutput merge_out(new_value, existing_operand);
    chessMerger.FullMergeV2(merge_in, &merge_out);

    assert(new_value == expected);
  }

  TearDown("\tdone\n");
}

void FullMergerTest_NoneEmptyExistingTest() {
  SetUp("==== FullMergerTest_NoneEmptyExistingTest start\n");

  std::string key, field, value;
  std::string new_value, result, expected;

  // prepare existing_value
  key = "";
  std::string field1("a3b4"), value1("112"),
    field2("a3b3"), value2("-11159");
  std::string ep1 = gEncoder->encode_value(field1, value1),
    ep2 = gEncoder->encode_value(field2, value2);

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
    std::string temp = gEncoder->encode_value(field, value);
    operands.push_back(temp);

    expected = temp + ";" + existing_value;
    rocksdb::MergeOperator::MergeOperationInput merge_in(key, &slice,
							 operands, nullptr);
    rocksdb::MergeOperator::MergeOperationOutput merge_out(new_value, existing_operand);
    chessMerger.FullMergeV2(merge_in, &merge_out);

    assert(new_value == expected);
  }
  {
    // one update
    operands.clear();
    new_value = "";
    std::string existing_value = ep1 + ";" + ep2;
    rocksdb::Slice slice(existing_value);

    value = "whatever";
    std::string temp = gEncoder->encode_value(field1, value);
    operands.push_back(temp);

    expected = temp + ";" + ep2;
    rocksdb::MergeOperator::MergeOperationInput merge_in(key, &slice,
							 operands, nullptr);
    rocksdb::MergeOperator::MergeOperationOutput merge_out(new_value, existing_operand);
    chessMerger.FullMergeV2(merge_in, &merge_out);
	
    assert(new_value == expected);
  }
  {
    // one delete
    operands.clear();
    new_value = "";
    std::string existing_value = ep1 + ";" + ep2;
    rocksdb::Slice slice(existing_value);

    value = kDelTag;
    std::string temp = gEncoder->encode_value(field1, value);
    operands.push_back(temp);

    expected = ep2;
    rocksdb::MergeOperator::MergeOperationInput merge_in(key, &slice,
							 operands, nullptr);
    rocksdb::MergeOperator::MergeOperationOutput merge_out(new_value, existing_operand);
    chessMerger.FullMergeV2(merge_in, &merge_out);

    assert(new_value == expected);
  }
  {
    // one insert, one update, one delete
    operands.clear();
    new_value = "";
    std::string existing_value = ep1 + ";" + ep2;
    rocksdb::Slice slice(existing_value);

    field = "c7d0"; value = "112";
    std::string itemp = gEncoder->encode_value(field, value);
    operands.push_back(itemp);

    value = kDelTag;
    std::string dtemp = gEncoder->encode_value(field2, value);
    operands.push_back(dtemp);

    value = "211";
    std::string utemp = gEncoder->encode_value(field1, value);
    operands.push_back(utemp);

    expected = utemp + ";" + itemp;
    rocksdb::MergeOperator::MergeOperationInput merge_in(key, &slice,
							 operands, nullptr);
    rocksdb::MergeOperator::MergeOperationOutput merge_out(new_value, existing_operand);
    chessMerger.FullMergeV2(merge_in, &merge_out);

    assert(new_value == expected);
  }

  TearDown("\tdone\n");
}

void PartialMergerTest_BaseTest() {
  SetUp("==== PartialMergerTest_BaseTest start\n");

  std::string key, field, value;
  std::string new_value, result, expected;
    
  ChessMergeOperator chessMerger;
  {
    // two inserts
    field = "a3b4"; value = "112";
    std::string ep1 = gEncoder->encode_value(field, value);

    field = "a3b3"; value = "-11159";
    std::string ep2 = gEncoder->encode_value(field, value);

    new_value = "";
    expected = ep2 + ";" + ep1;
    chessMerger.PartialMerge(key, ep1, ep2, &new_value, nullptr);

    assert(new_value == expected);
  }
  {
    // one insert, one update
    field = "a3b4"; value = "112";
    std::string ep1 = gEncoder->encode_value(field, value);

    field = "a3b4"; value = "-11159";
    std::string ep2 = gEncoder->encode_value(field, value);

    new_value = "";
    expected = ep2;
    chessMerger.PartialMerge(key, ep1, ep2, &new_value, nullptr);

    assert(new_value == expected);
  }
  {
    // one insert, one delete
    field = "a3b4"; value = "112";
    std::string ep1 = gEncoder->encode_value(field, value);

    field = "a3b4"; value = kDelTag;
    std::string ep2 = gEncoder->encode_value(field, value);

    expected = ep2; new_value = "";
    chessMerger.PartialMerge(key, ep1, ep2, &new_value, nullptr);

    assert(new_value == expected);
  }
  {
    // one delete, one insert
    field = value = expected = "";
    field = "a3b4"; value = kDelTag;
    std::string ep1 = gEncoder->encode_value(field, value);
	
    field = "a3b4"; value = "112";
    std::string ep2 = gEncoder->encode_value(field, value);

    new_value = "";
    expected = ep2;
    chessMerger.PartialMerge(key, ep1, ep2, &new_value, nullptr);

    assert(new_value == expected);
  }
  {
    // two inserts, one update
    field = "a3b4"; value = "112";
    std::string ep1 = gEncoder->encode_value(field, value);

    field = "a3b3"; value = "-11159";
    std::string ep2 = gEncoder->encode_value(field, value);

    field = "a3b4"; value = "11445";
    std::string ep3 = gEncoder->encode_value(field, value);

    new_value = "";
    expected = ep3 + ";" + ep2;
    chessMerger.PartialMerge(key, ep1, ep3 + ";" + ep2, &new_value, nullptr);

    assert(new_value == expected);
  }
  {
    // two inserts, one delete
    field = "a3b4"; value = "112";
    std::string ep1 = gEncoder->encode_value(field, value);

    field = "a3b3"; value = "-11159";
    std::string ep2 = gEncoder->encode_value(field, value);

    field = "a3b3"; value = kDelTag;
    std::string ep3 = gEncoder->encode_value(field, value);

    new_value = "";
    expected = ep3 + ";" + ep1;
    chessMerger.PartialMerge(key, ep1 + ";" + ep2, ep3, &new_value, nullptr);

    assert(new_value == expected);
  }

  TearDown("\tdone\n");
}

void THashTest_BugPartial() {
  SetUp("==== PartialBug start\n");

  std::string key, field, value;
  std::string new_value, result, expected;
    
  ChessMergeOperator chessMerger;
  // make sure partial merge will keep 'DEL'
  // a -> 1, b -> 2, del a
  field = "a3b4"; value = "112";
  std::string ep1 = gEncoder->encode_value(field, value);

  field = "a3b3"; value = "-11159";
  std::string ep2 = gEncoder->encode_value(field, value);

  field = "a3b4"; value = kDelTag;
  std::string ep3 = gEncoder->encode_value(field, value);
    
  std::string inter = "";
  expected = ep3 + ";" + ep2;
  chessMerger.PartialMerge(key, ep2, ep3, &inter, nullptr);
  assert(expected == inter);

  new_value = "";
  chessMerger.PartialMerge(key, ep1, inter, &new_value, nullptr);
  assert(new_value == expected);

  // now use full merge, 'DEL' will be skipped
  rocksdb::Slice slice, existing_operand;
  std::vector<rocksdb::Slice> operands = { new_value };
  expected = ep2;
  rocksdb::MergeOperator::MergeOperationInput merge_in(key, &slice,
						       operands, nullptr);
  rocksdb::MergeOperator::MergeOperationOutput merge_out(new_value, existing_operand);
  chessMerger.FullMergeV2(merge_in, &merge_out);
  assert(new_value == expected);

  TearDown("\tdone\n");
}


void ValueEncodeTest_BaseTest() {
  SetUp("==== ValueEncodeTest_BaseTest start\n");

  ChessHashEncoder encoder;
  std::string ifield, ivalue;
  {
    ifield = "e3h5"; ivalue = "12345";
    std::string result = encoder.encode_value(ifield, ivalue);
    std::string ofield, ovalue;
    encoder.decode_value(result, &ofield, &ovalue);
    assert(ifield == ofield);
    assert(ivalue == ovalue);
  }
  {
    ifield = "a0h9"; ivalue = "-29999";
    std::string result = encoder.encode_value(ifield, ivalue);
    std::string ofield, ovalue;
    encoder.decode_value(result, &ofield, &ovalue);
    assert(ifield == ofield);
    assert(ivalue == ovalue);
  }
  {
    // del item
    ifield = "a7b8"; ivalue = kDelTag;
    std::string result = encoder.encode_value(ifield, ivalue);
    std::string ofield, ovalue;
    encoder.decode_value(result, &ofield, &ovalue);
    assert(ifield == ofield);
    assert(ivalue == ovalue);
  }
  {
    // invalid field
    ifield = "b8j9"; ivalue = "0";
    std::string result = encoder.encode_value(ifield, ivalue);
    std::string expected = "";
    assert(result == expected);
  }
  {
    // invalid field 2
    ifield = "b8c99"; ivalue = "0";
    std::string result = encoder.encode_value(ifield, ivalue);
    std::string expected = "";
    assert(result == expected);
  }
  {
    // invalid field 3
    ifield = "8c9d"; ivalue = "0";
    std::string result = encoder.encode_value(ifield, ivalue);
    std::string expected = "";
    assert(result == expected);
  }
  {
    // invalid value
    ifield = "c9d0"; ivalue = "-30100";
    std::string result = encoder.encode_value(ifield, ivalue);
    std::string expected = "";
    assert(result == expected);
  }

  TearDown("\tdone\n");
}

int main() {
  printf("EXAGGERATE\n");
  THashTest_SetAndCnt();
  THashTest_SetAndGet();
  THashTest_DelAndCnt();
  THashTest_ListKeys();
  THashTest_Scan();
  FullMergerTest_EmptyExistingTest();
  FullMergerTest_NoneEmptyExistingTest();
  PartialMergerTest_BaseTest();
  ValueEncodeTest_BaseTest();
  THashTest_BugPartial();
  return 0;
}
