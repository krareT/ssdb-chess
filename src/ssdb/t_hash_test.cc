
#include "gtest/gtest.h"

#include "../include.h"
#include "ssdb_impl.h"

int Factorial(int n) {
    if (n == 1 || n == 2) return 1;
    return Factorial(n - 1) * Factorial(n - 2);
}

using std::cout;
using std::endl;

TEST(InsertUpdateTest, BaseTest) {
    std::string prev_value, field, value;
    std::string output, expected;
    {
	// from empty value
	prev_value = field = value = output = expected = "";
	prev_value = ""; field = "a3b4"; value = "112";
	expected.push_back((char)field.length()); expected += field; expected.push_back(':');
	expected.push_back((char)value.length()); expected += value;
	int ret = insert_update_hash_value(prev_value, field, value, &output);

	EXPECT_NE("a3b4:112", output);
	EXPECT_EQ(expected, output);
    }
    {  
	// insert
	prev_value = field = value = output = expected = "";
	prev_value = ""; field = "a3b4"; value = "112";
	expected.push_back((char)field.length()); expected += field; expected.push_back(':');
	expected.push_back((char)value.length()); expected += value;
	insert_update_hash_value(prev_value, field, value, &output);

	prev_value = output; field = "a3b3"; value = "-11159";
	expected.push_back(';');
	expected.push_back((char)field.length()); expected += field; expected.push_back(':');
	expected.push_back((char)value.length()); expected += value;
	insert_update_hash_value(prev_value, field, value, &output);

	EXPECT_EQ(expected, output);
    }
    {
	// update 1
	prev_value = field = value = output = expected = "";
	prev_value = ""; field = "a3b4"; value = "112";
	insert_update_hash_value(prev_value, field, value, &output);
	
	prev_value = output; field = "a3b4"; value = "-11159";
	expected.push_back((char)field.length()); expected += field; expected.push_back(':');
	expected.push_back((char)value.length()); expected += value;
	insert_update_hash_value(prev_value, field, value, &output);

	EXPECT_EQ(expected, output);
    }
    {  
	// update 2
	prev_value = field = value = output = expected = "";
	prev_value = ""; field = "a3b4"; value = "112";
	expected.push_back((char)field.length()); expected += field; expected.push_back(':');
	expected.push_back((char)value.length()); expected += value;
	insert_update_hash_value(prev_value, field, value, &output);

	prev_value = output; field = "a3b3"; value = "-11159";
	insert_update_hash_value(prev_value, field, value, &output);

	prev_value = output; field = "a3b3"; value = "1724";
	expected.push_back(';');
	expected.push_back((char)field.length()); expected += field; expected.push_back(':');
	expected.push_back((char)value.length()); expected += value;

	int ret = insert_update_hash_value(prev_value, field, value, &output);
	EXPECT_EQ(expected, output);
    }
}

TEST(RemoveTest, BaseTest) {
    std::string prev_value, field, value;
    std::string output, expected;
    {
	// remove none-exist 1
	prev_value = field = value = output = expected = "";
	prev_value = ""; field = "a3b4"; value = "112";
	expected = "";
	int ret = remove_hash_value(prev_value, field, &output);

	ASSERT_EQ(0, ret);
	EXPECT_EQ(expected, output);
    }
    {
	// remove none-exist 2
	prev_value = field = value = output = expected = "";
	prev_value = ""; field = "a3b4"; value = "112";
	expected.push_back((char)field.length()); expected += field; expected.push_back(':');
	expected.push_back((char)value.length()); expected += value;
	insert_update_hash_value(prev_value, field, value, &output);

	prev_value = output; field = "sdfsafds";
	int ret = remove_hash_value(prev_value, field, &output);

	ASSERT_EQ(0, ret);
	EXPECT_EQ(expected, output);
    }
    {
	// remove exist 1
	prev_value = field = value = output = expected = "";
	prev_value = ""; field = "a3b4"; value = "112";
	expected = "";
	insert_update_hash_value(prev_value, field, value, &output);

	prev_value = output;
	int ret = remove_hash_value(prev_value, field, &output);

	ASSERT_EQ(1, ret);
	EXPECT_EQ(expected, output);
    }
    {
	// remove exist 2: remove tail
	prev_value = field = value = output = expected = "";
	prev_value = ""; field = "a3b4"; value = "112";
	expected.push_back((char)field.length()); expected += field; expected.push_back(':');
	expected.push_back((char)value.length()); expected += value;
	insert_update_hash_value(prev_value, field, value, &output);

	prev_value = output; field = "u3y4"; value = "-112";
	insert_update_hash_value(prev_value, field, value, &output);

	prev_value = output;
	int ret = remove_hash_value(prev_value, field, &output);

	
	EXPECT_EQ(expected, output);
    }
    {
	// remove exist 3: remove head
	prev_value = field = value = output = expected = "";
	prev_value = ""; field = "a3b4"; value = "112";
	insert_update_hash_value(prev_value, field, value, &output);

	prev_value = output; field = "u3y4"; value = "-112";
	expected.push_back((char)field.length()); expected += field; expected.push_back(':');
	expected.push_back((char)value.length()); expected += value;
	insert_update_hash_value(prev_value, field, value, &output);

	prev_value = output; field = "a3b4";
	int ret = remove_hash_value(prev_value, field, &output);

	ASSERT_EQ(1, ret);
	EXPECT_EQ(expected, output);
    }
}

    
int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}


