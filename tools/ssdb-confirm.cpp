// this is only a demo, DO NOT use
#include <stdio.h>
#include <stdlib.h>
#include <algorithm>
#include <chrono>
#include <fstream>
#include <random>       // std::default_random_engine
#include <map>
#include <string>
#include <vector>
#include "rocksdb/db.h"
#include "util/log.h"
#include "util/strings.h"
#include "../src/client/SSDB_client.h"
#include "../src/net/link.h"
#include "../src/ssdb/const.h"

#define BATCH_SIZE 100

std::vector<std::string> chess_ds;
ssdb::Client *src = NULL;
ssdb::Client *dst = NULL;

void welcome(){
  printf("ssdb-migrate - SSDB server migration tool\n");
  printf("Copyright (c) 2012-2015 ssdb.io\n");
  printf("\n");
}

void usage(int argc, char **argv) {
  printf("Usage:\n"
	 "    %s src_ip src_port dst_ip dst_port limit\n"
	 "\n"
	 "Options:\n"
	 "    src_ip    IP addr of the SSDB server to move data from, example: 127.0.0.1\n"
	 "    src_port  Port number of source SSDB server\n"
	 "    src_ip    IP addr of the SSDB server to move data to, example: 127.0.0.1\n"
	 "    dst_port  Port number of destination SSDB server\n"
	 "    limit     Approximated number of keys to be moved, example: 1000\n"
	 "    -h        Show this message"
	 "\n"
	 "Example:\n"
	 "    %s 127.0.0.1 8887 127.0.0.1 8889 13\n"
	 "\n",
	 argv[0], argv[0]);
  exit(1);   
}

struct AppArgs{
  std::string type;
  std::string src_ip;
  int src_port;
  std::string dst_ip;
  int dst_port;
  int limit;
};

void parse_args(AppArgs *args, int argc, char **argv){
  if (argc < 6) {
    usage(argc, argv);
  }
  for (int i = 1; i < argc; i++) {
    if (std::string("-h") == argv[i]) {
      usage(argc, argv);
    }
    /*if (argv[i][0] == '-') {
      fprintf(stderr, "ERROR: Invalid argument: %s!\n", argv[i]);
      exit(1);   
      }*/
  }
  args->src_ip = argv[1];
  args->src_port = str_to_int(argv[2]);
  args->dst_ip = argv[3];
  args->dst_port = str_to_int(argv[4]);
  args->limit = str_to_int(argv[5]);
  /*if(args->limit <= 0){
    fprintf(stderr, "ERROR: invalid limit option!\n");
    exit(1);   
    }*/
}

ssdb::Client* init_client(const std::string &ip, int port){
  ssdb::Client *client = ssdb::Client::connect(ip, port);
  if( client == NULL) {
    log_error("fail to connect to server!");
    return NULL;
  }

  const std::vector<std::string>* resp;
  resp = client->request("ignore_key_range");
  if(!resp || resp->empty() || resp->at(0) != "ok"){
    log_error("src server ignore_key_range error!");
    delete client;
    return NULL;
  }
  return client;
}

int hgetall(ssdb::Client *client, const std::string& key,
	    std::vector<std::string>* val) {
  ssdb::Status s = client->hgetall(key, val);
  if(!s.ok()){
    log_error("dst hgetall error! %s", s.code().c_str());
    return -1;
  }
  return 0;
}

bool to_binary(const char* src, int len, std::string& dst) {
  static std::map<char, int> dict = { 
    {'0', 0}, {'1', 1}, {'2', 2}, {'3', 3}, {'4', 4},
    {'5', 5}, {'6', 6}, {'7', 7}, {'8', 8}, {'9', 9}, {'a', 10},
    {'b', 11}, {'c', 12}, {'d', 13}, {'e', 14}, {'f', 15}
  };
  dst.clear(); dst.reserve(len);
  int cnt = 0;
  for (int i = 0; i < len; i++) {
    if (src[i] == '\\' && src[i + 1] == 'x') {
      if (i + 3 >= len) {
	return false;
      }
      unsigned char c = 0;
      i += 2; // skip \x
      if (dict.count(src[i]) == 0 ||
	  dict.count(src[i + 1]) == 0) {
	return false;
      }
      c |= (dict[src[i]] << 4) & 0xF0;
      i ++;
      c |= dict[src[i]] & 0x0F;
      dst.append(1, c);
    } else {
      dst.append(1, src[i]);
    }
    cnt ++;
  }
  dst.resize(cnt);
  return true;
}

void init_data(int num) {
  std::string path = "./key_sample.txt";
  std::ifstream src_file(path.c_str());
  std::string line;
  int cnt = 0;
  while (std::getline(src_file, line)) {
    int pos = 0;
    if ((pos = line.find("key: ")) == std::string::npos) {
      continue;
    }
    std::string src = line.substr(5);
    std::string dst;
    if (to_binary(src.data(), src.size(), dst)) {
      chess_ds.emplace_back(dst.data(), dst.size());
    } else {
      printf("Fail to convert %s\n", src.c_str());
      //exit(1);
    }
    if (num != -1 && ++cnt >= num) {
      break;
    }
  }
  // obtain a time-based seed:
  unsigned seed = std::chrono::system_clock::now().time_since_epoch().count();
  std::shuffle(chess_ds.begin(), chess_ds.end(), std::default_random_engine(seed));
  printf("item count is %lu\n", chess_ds.size());
}

void print_val(const std::string& tag, std::vector<std::string>& arr) {
    printf("\t%s\n", tag.c_str());
    for (auto& str : arr) {
	printf("\t%s", str.c_str());
    }
    printf("\n");
}

int main(int argc, char **argv){
  welcome();
  set_log_level(Logger::LEVEL_MIN);

  AppArgs args;
  parse_args(&args, argc, argv);

  src = init_client(args.src_ip, args.src_port);
  dst = init_client(args.dst_ip, args.dst_port);
  init_data(args.limit);

  int diff_cnt = 0;
  for (auto& key : chess_ds) {
      std::string bin_key;
      if (!to_binary(key.data(), key.size(), bin_key)) {
	  printf("Fail to convert back to binary %s\n", key.c_str());
      }
      std::vector<std::string> src_arr, dst_arr;
      if (hgetall(src, bin_key, &src_arr) == -1 ||
	  hgetall(dst, bin_key, &dst_arr) == -1) {
	  printf("Fail to get %s\n", key.c_str());
	  continue;
      }
      if (src_arr.size() != dst_arr.size()) {
	  printf("Diff Value %s\n", str_escape(key.data(), key.size()).c_str());
	  print_val("src: ", src_arr);
	  print_val("dst: ", dst_arr);
	  continue;
      }
      std::map<std::string, std::string> src_dict, dst_dict;
      for (int i = 0; i + 1 < src_arr.size(); i += 2) {
	  src_dict[src_arr[i]] = src_arr[i + 1];
	  dst_dict[dst_arr[i]] = dst_arr[i + 1];
      }
      bool diff = false;
      for (int i = 0; i + 1 < src_arr.size(); i += 2) {
	  std::string& str = src_arr[i];
	  if (src_dict[str] != dst_dict[str]) {
	      diff = true;
	      break;
	  }
      }
      if (diff) {
	  printf("Diff Value %s\n", str_escape(key.data(), key.size()).c_str());
	  print_val("src: ", src_arr);
	  print_val("dst: ", dst_arr);
	  diff_cnt ++;
      } 
  }
  printf("diff_cnt is %d\n", diff_cnt);
  /*
    {
    std::string val;
    if(db->GetProperty("rocksdb.stats", &val)){
    printf("%s\n", val.c_str());
    }
    }

    printf("compacting data...\n");
    db->CompactRange(NULL, NULL);
	
    {
    std::string val;
    if(db->GetProperty("rocksdb.stats", &val)){
    printf("%s\n", val.c_str());
    }
    }
    
    printf("backup has been made to folder: %s\n", config.output_folder.c_str());
  */
  delete src;
  delete dst;
  return 0;
}

