// this is only a demo, DO NOT use
#include <stdio.h>
#include <stdlib.h>
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

int hashset(ssdb::Client *client, const std::string& key,
	    const std::string& field, const std::string& value) {
  ssdb::Status s = client->hset(key, field, value);
  if(!s.ok()){
    log_error("dst hset error! %s", s.code().c_str());
    return -1;
  }
  return 0;
}

void check_version(ssdb::Client *client){
  const std::vector<std::string>* resp;
  resp = client->request("version");
  if(!resp || resp->size() < 2 || resp->at(0) != "ok"){
    fprintf(stderr, "ERROR: ssdb-server 1.9.0 or higher is required!\n");
    exit(1);
  }
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
    if (src[i] == '\\') {
      if (i + 3 >= len || src[i + 1] != 'x') {
	return false;
      }
      unsigned char c = 0;
      if (dict.count(src[i]) == 0 ||
	  dict.count(src[i + 1]) == 0) {
	return false;
      }
      c |= (dict[src[i]] << 4) & 0xF0;
      i ++;
      c |= dict[src[i]] & 0x0F;
    } else {
      dst.append(1, src[i]);
    }
    cnt ++;
  }
  dst.resize(cnt);
  return true;
}

int main(int argc, char **argv){
  welcome();
  set_log_level(Logger::LEVEL_MIN);

  AppArgs args;
  parse_args(&args, argc, argv);

  // connect to src server
  Link *link = Link::connect(args.src_ip.c_str(), args.src_port);
  if (link == NULL) {
    fprintf(stderr, "ERROR: error connecting to src server: %s:%d!\n",
	    args.src_ip.c_str(), args.src_port);
    exit(1);
  }
  // connect to dst server
  dst = init_client(args.dst_ip, args.dst_port);
  if(dst == NULL){
    log_error("fail to connect to server!");
    return 0;
  }
  printf("after connect\n");
  // start transfer
  link->send("dump", "A", "", "-1");
  link->flush();
    
  int64_t dump_count = 0;
  while (1) {
    const std::vector<Bytes> *req = link->recv();
    if (req == NULL) {
      fprintf(stderr, "recv error\n");
      fprintf(stderr, "ERROR: failed to dump data!\n");
      exit(1);
    } else if(req->empty()) {
      int len = link->read();
      if (len <= 0) {
	fprintf(stderr, "read error: %s\n", strerror(errno));
	fprintf(stderr, "ERROR: failed to dump data!\n");
	exit(1);
      }
    } else {
      Bytes cmd = req->at(0);
      if (cmd == "begin") {
	printf("recv begin...\n");
      } else if(cmd == "end") {
	printf("received %" PRId64 " entry(s)\n", dump_count);
	printf("recv end\n\n");
	break;
      } else if(cmd == "set") {
	if (req->size() != 3) {
	  fprintf(stderr, "invalid set params!\n");
	  fprintf(stderr, "ERROR: failed to dump data!\n");
	  exit(1);
	}
	Bytes key = req->at(1);
	Bytes val = req->at(2);
	if (key.size() == 0 || key.data()[0] == DataType::SYNCLOG) {
	  continue;
	}

	if (args.src_port == args.dst_port) {
	    //if (dump_count > 0 && dump_count % 100 == 0) { // sample keys
	    //printf("key: %s\n", str_escape(key.data(), key.size()).c_str());
	    //}
	    printf("key: %s\nval: %s\n", str_escape(key.data(), key.size()).c_str(),
	    	   str_escape(val.data(), val.size()).c_str());
	} else {
	  if (key.data()[0] != 'h') { // only support hset right now
	    continue;
	  }
	  Bytes hkey(key.data() + 2, key.size() - 2); // skip 'h' & 'key length'
	  int index = -1;
	  for (int i = hkey.size() - 1; i >= 0; i--) {
	    if (hkey.data()[i] == '=') {
	      index = i;
	      break;
	    }
	  }
	  if (index < 0) {
	    fprintf(stderr, "invalid key\n");
	    continue;
	  }

	  std::string k(hkey.data(), index);
	  std::string f(hkey.data() + index + 1, hkey.size() - index - 1);
	  std::string v(val.data(), val.size());

	  if (dump_count > 0 && dump_count % 100 == 0) { // sample keys
	      printf("key: %s\n", str_escape(k.data(), k.size()).c_str());
	  }	  
	  /*printf("src %s %s %s\n", str_escape(k.data(), k.size()).c_str(),
	  	 str_escape(f.data(), f.size()).c_str(),
	  	 str_escape(v.data(), v.size()).c_str());*/

	  int ret = hashset(dst, k, f, v);
	  if (ret != 0) {
	    fprintf(stderr, "hset  error!\n");
	    fprintf(stderr, "ERROR: failed to dump data!\n");
	    exit(1);
	  }
	}

	dump_count ++;
	if (args.limit != -1 && dump_count >= args.limit) {
	  break;
	}
	//if ((int)log10(dump_count - 1) != (int)log10(dump_count) || (dump_count > 0 && dump_count % 100000 == 0)) {
	if (dump_count > 0 && dump_count % 100000 == 0) {
	  printf("received %" PRId64 " entry(s)\n", dump_count);
	}
      } else {
	fprintf(stderr, "error: unknown command %s\n", std::string(cmd.data(), cmd.size()).c_str());
	fprintf(stderr, "ERROR: failed to dump data!\n");
	exit(1);
      }
    }
  }
  printf("total dumped %" PRId64 " entry(s)\n", dump_count);

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
  delete link;
  delete dst;
  return 0;
}

