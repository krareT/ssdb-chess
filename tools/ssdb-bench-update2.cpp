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
#include <iostream>
#include <mutex>
#include <random>       // std::default_random_engine
#include <string>
#include <thread>
#include <vector>
#include <map>
#include "net/link.h"
#include "net/fde.h"
#include "util/log.h"
#include "util/strings.h"
#include "version.h"

#include "../src/include.h"
#include "../src/client/SSDB_client.h"

struct Data {
    std::string key;
    std::string val;
    std::string num;
};

std::vector<std::string> chess_ds;
std::vector<ssdb::Client*> clients;
int total_reqs;
int total_clients;
std::mutex mu;

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

void init_clients(const char* ip, int port) {
    for (int i = 0; i < total_clients; i++) {
	ssdb::Client *client = ssdb::Client::connect(ip, port);
	if (client == NULL) {
	    log_error("fail to connect to server!");
	    return;
	}

	const std::vector<std::string>* resp;
	resp = client->request("ignore_key_range");
	if(!resp || resp->empty() || resp->at(0) != "ok"){
	    log_error("src server ignore_key_range error!");
	    delete client;
	    exit(1);
	}
	clients.push_back(client);
    }
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

void send_req(int tid) {
    int per = total_reqs / total_clients;
    int offset = per * tid;
    std::vector<std::string> results;
    for (int cnt = 0; cnt < per; cnt++) {
	int index = offset + cnt;
	results.clear();
	printf("key: %s\n", str_escape(chess_ds[index].data(), 
				       chess_ds[index].size()).c_str());
	if (hgetall(clients[tid], chess_ds[index], &results) == -1) {
	    printf("hgetall error: %s\n", chess_ds[index].c_str());
	    continue;
	}
	std::lock_guard<std::mutex> lock(mu);
	printf("result cnt: %lu\n", results.size());
	for (int i = 0; i + 1 < results.size(); i += 2) {
	    printf("field: %s, value: %s\n", results[i].c_str(), results[i + 1].c_str());
	}
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
	printf("data to convert: %s\n", src.c_str());
	std::string dst;
	if (to_binary(src.data(), src.size(), dst)) {
	    printf("after to_bin: %s\n", str_escape(dst.data(), dst.size()).c_str());
	    chess_ds.emplace_back(dst.data(), dst.size());
	    printf("in chess: %s\n", str_escape(chess_ds.back().data(), 
						chess_ds.back().size()).c_str());
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

void bench() {
    double stime = millitime();
    std::vector<std::thread> threads;
    std::cout << "Launched from the main\n";
    // Launch a group of threads
    for (int i = 0; i < total_clients; ++i) {
        threads.push_back(std::thread(send_req, i));
    }
    // Join the threads with the main thread
    for (int i = 0; i < total_clients; ++i) {
        threads[i].join();
    }
    double etime = millitime();
    double ts = (stime == etime)? 1 : (etime - stime);
    double speed = total_reqs / ts;
    printf("qps: %d, time: %.3f s\n", (int)speed, ts);
    return;
}

int main(int argc, char **argv){
    const char *ip = "127.0.0.1";
    int port = 8888;

    welcome();
    usage(argc, argv);
    for (int i = 1; i<argc; i++) {
	if (strcmp("-v", argv[i]) == 0) {
	    exit(0);
	}
    }
    if (argc < 4) {
	printf("\n");
	return 0;
    }
    ip = argv[1];
    port = atoi(argv[2]);
    total_reqs = atoi(argv[3]);
    total_clients = atoi(argv[4]);

    init_data(total_reqs);
    init_clients(ip, port);

    bench();

    printf("\n");

    return 0;
}

