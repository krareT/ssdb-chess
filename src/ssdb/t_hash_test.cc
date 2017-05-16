
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
    std::string key, field, value;
    std::string output, expected;
    {
	// from empty key
	key = field = value = output = expected = "";
	key = ""; field = "a3b4"; value = "112";
	expected.push_back((char)field.length()); expected += field; expected.push_back(':');
	expected.push_back((char)value.length()); expected += value;
	int ret = insert_update_hash_value(key, field, value, &output);

	EXPECT_NE("a3b4:112", output);
	EXPECT_EQ(expected, output);
    }
    {  
	// insert
	
    }
    {
	// update
    }
}

TEST(RemoveTest, BaseTest) {
    std::string key, field, value;
    std::string output, expected;
    {
	// remove none-exist
    }
    {
	// remove exist
    }
}

    
int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}


