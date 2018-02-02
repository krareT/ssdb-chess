/*
  Copyright (c) 2012-2014 The SSDB Authors. All rights reserved.
  Use of this source code is governed by a BSD-style license that can be
  found in the LICENSE file.
*/
#ifndef SSDB_OPTION_H_
#define SSDB_OPTION_H_

#include "../util/config.h"

class Options
{
 public:
    Options();
    ~Options(){}
	
    void load(const Config &conf);

    size_t cache_size = 0;
    size_t max_open_files = 0;
    size_t write_buffer_size = 0;
    size_t block_size = 0;
    int compaction_speed = 0;
    std::string compression;
    bool binlog = 0;
    size_t binlog_capacity = 0;
};

#endif
