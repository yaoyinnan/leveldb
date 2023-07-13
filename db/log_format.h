// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
//
// Log format information shared by reader and writer.
// See ../doc/log_format.md for more detail.

#ifndef STORAGE_LEVELDB_DB_LOG_FORMAT_H_
#define STORAGE_LEVELDB_DB_LOG_FORMAT_H_

namespace leveldb {
namespace log {

enum RecordType {
  // Zero is reserved for preallocated files
  kZeroType = 0,

  kFullType = 1,  // 完整的 user record

  // For fragments
  kFirstType = 2, // 切分的 user record 的第一个 record
  kMiddleType = 3, // user record 中间的 record
  kLastType = 4 // 最后一个 record
};
static const int kMaxRecordType = kLastType;

static const int kBlockSize = 32768; // block：32KB

// Header is checksum (4 bytes), length (2 bytes), type (1 byte).
static const int kHeaderSize = 4 + 2 + 1; // 7 bytes 的 head

}  // namespace log
}  // namespace leveldb

#endif  // STORAGE_LEVELDB_DB_LOG_FORMAT_H_
