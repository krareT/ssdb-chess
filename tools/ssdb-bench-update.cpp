/*
  Copyright (c) 2012-2015 The SSDB Authors. All rights reserved.
  Use of this source code is governed by a BSD-style license that can be
  found in the LICENSE file.
*/
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <errno.h>
#include <algorithm>
#include <chrono>
#include <fstream>
#include <random>       // std::default_random_engine
#include <string>
#include <vector>
#include <map>
#include "net/link.h"
#include "net/fde.h"
#include "util/log.h"
#include "version.h"

#include "../src/include.h"

struct Data {
  std::string key;
  std::string val;
  std::string num;
};

std::map<std::string, Data *> *ds;
std::vector<std::string> chess_ds;
Fdevents *fdes;
std::vector<Link *> *free_links;


void welcome(){
  printf("ssdb-bench - SSDB benchmark tool, %s\n", SSDB_VERSION);
  printf("Copyright (c) 2013-2015 ssdb.io\n");
  printf("\n");
}

void usage(int argc, char **argv){
  printf("Usage:\n");
  printf("    %s [ip] [port] [requests] [clients]\n", argv[0]);
  printf("\n");
  printf("Options:\n");
  printf("    ip          server ip (default 127.0.0.1)\n");
  printf("    port        server port (default 8888)\n");
  printf("    requests    Total number of requests (default 10000)\n");
  printf("    clients     Number of parallel connections (default 50)\n");
  printf("\n");
}


void init_links(int num, const char *ip, int port){
  fdes = new Fdevents();
  free_links = new std::vector<Link *>();

  for(int i=0; i<num; i++){
    Link *link = Link::connect(ip, port);
    if(!link){
      fprintf(stderr, "connect error! %s\n", strerror(errno));
      exit(0);
    }
    fdes->set(link->fd(), FDEVENT_IN, 0, link);
    free_links->push_back(link);
  }
}

void send_req(Link *link, const std::string &cmd, const Data *d){
  if (cmd == "hgetall") {
    link->send(cmd, d->key);
  } else if(cmd == "set"){
    link->send(cmd, d->key, d->val);
  }else if(cmd == "get"){
    link->send(cmd, d->key);
  }else if(cmd == "del"){
    link->send(cmd, d->key);
  }else if(cmd == "hset"){
    link->send(cmd, "TEST", d->key, d->val);
  }else if(cmd == "hget"){
    link->send(cmd, "TEST", d->key);
  }else if(cmd == "hdel"){
    link->send(cmd, "TEST", d->key);
  }else if(cmd == "zset"){
    link->send(cmd, "TEST", d->key, d->num);
  }else if(cmd == "zget"){
    link->send(cmd, "TEST", d->key);
  }else if(cmd == "zdel"){
    link->send(cmd, "TEST", d->key);
  }else if(cmd == "qpush"){
    link->send(cmd, "TEST", d->key);
  }else if(cmd == "qpop"){
    link->send(cmd, "TEST");
  }else{
    log_error("bad command!");
    exit(0);
  }
  link->flush();
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

void bench(std::string cmd) {
  int total = (int)chess_ds.size();
  int finished = 0;
  int num_sent = 0;
	
  printf("========== %s ==========\n", cmd.c_str());
  //auto it = ds->begin();
  auto it = chess_ds.begin();
  double stime = millitime();
  Data data;
  while (1) {
    while (!free_links->empty()) {
      if(num_sent == total){
	break;
      }
      num_sent ++;

      Link *link = free_links->back();
      free_links->pop_back();
      
      data.key.clear();
      data.key.assign((*it).data(), (*it).size());
      //send_req(link, cmd, it->second);
      
      send_req(link, cmd, &data);
      it ++;
    }

    const Fdevents::events_t *events;
    events = fdes->wait(50);
    if(events == NULL){
      log_error("events.wait error: %s", strerror(errno));
      break;
    }

    for (int i=0; i<(int)events->size(); i++) {
      const Fdevent *fde = events->at(i);
      Link *link = (Link *)fde->data.ptr;

      int len = link->read();
      if(len <= 0){
	log_error("fd: %d, read: %d, delete link", link->fd(), len);
	exit(0);
      }

      const std::vector<Bytes> *resp = link->recv();
      if(resp == NULL){
	log_error("error");
	break;
      }else if(resp->empty()){
	continue;
      }else{
	if(resp->at(0) != "ok"){
	  log_error("bad response: %s", resp->at(0).String().c_str());
	  exit(0);
	} else {
	    //log_info("recv cnt: %d", resp->size());
	  //log_info("recv data: %s", str_escape(resp->at(0).data(),
	  //				       resp->at(0).size()).c_str());
	}
	free_links->push_back(link);
	finished ++;
	if(finished == total){
	  double etime = millitime();
	  double ts = (stime == etime)? 1 : (etime - stime);
	  double speed = total / ts;
	  printf("qps: %d, time: %.3f s\n", (int)speed, ts);
	  return;
	}
      }
    }
  }
}

int main(int argc, char **argv){
  const char *ip = "127.0.0.1";
  int port = 8888;
  int requests = 10000;
  int clients = 50;

  welcome();
  usage(argc, argv);
  for(int i=1; i<argc; i++){
    if(strcmp("-v", argv[i]) == 0){
      exit(0);
    }
  }
  if (argc < 4) {
    printf("\n");
    return 0;
  }
  if(argc > 1){
    ip = argv[1];
  }
  if(argc > 2){
    port = atoi(argv[2]);
  }
  if(argc > 3){
    requests = atoi(argv[3]);
  }
  if(argc > 4){
    clients = atoi(argv[4]);
  }

  //printf("preparing data...\n");
  init_data(requests);
  //printf("preparing links...\n");
  init_links(clients, ip, port);

  bench("hgetall");
  /*bench("set");
  bench("get");
  bench("del");

  bench("hset");
  bench("hget");
  bench("hdel");

  bench("zset");
  bench("zget");
  bench("zdel");

  bench("qpush");
  bench("qpop");*/
	
  printf("\n");

  return 0;
}

