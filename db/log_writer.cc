// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/log_writer.h"

#include <cstdint>

#include "leveldb/env.h"
#include "util/coding.h"
#include "util/crc32c.h"

namespace leveldb {
namespace log {

static void InitTypeCrc(uint32_t* type_crc) {
  for (int i = 0; i <= kMaxRecordType; i++) {
    char t = static_cast<char>(i);
    type_crc[i] = crc32c::Value(&t, 1);
  }
}

Writer::Writer(WritableFile* dest) : dest_(dest), block_offset_(0) {
  InitTypeCrc(type_crc_);
}

Writer::Writer(WritableFile* dest, uint64_t dest_length)
    : dest_(dest), block_offset_(dest_length % kBlockSize) {
  InitTypeCrc(type_crc_);
}

Writer::~Writer() = default;

// 该⽅法将记录写⼊⼀个Slice结构，调⽤AddRecord就会写⼊Log⽂件。AddRecord根据写⼊
// 记录的⼤⼩确定是否需要跨块，并据此得出相应的头部类型，然后将记录写⼊并且刷新到磁盘。
Status Writer::AddRecord(const Slice& slice) {
  //prt指向需要写⼊的记录内容
  const char* ptr = slice.data();
  //left代表需要写⼊的记录内容⻓度
  size_t left = slice.size();

  // Fragment the record if necessary and emit it.  Note that if slice
  // is empty, we still want to iterate once to emit a single
  // zero-length record
  Status s;
  // begin为true表明该条记录是第⼀次写⼊，即如果⼀个记录跨越多个块，只有写⼊第⼀个块时
  // begin为true，其他时候会置为false。通过该值可以确定头部类型字段是否为kFirstType
  bool begin = true;
  do {
    // kBlockSize为⼀个块的⼤⼩(32768字节)，block_offset_代表当前块的写⼊偏移量，
    // 因此leftover表明当前块还剩余多少字节可⽤
    const int leftover = kBlockSize - block_offset_;
    assert(leftover >= 0);
    // kHeaderSize为每个记录的头部⻓度(7字节)，如果当前块的剩余空间⼩于7个字节并且不
    // 等于0，则需要填充\x00
    // 如果leftover等于0，则说明正好写满⼀个块，将block_offset_置为0，表明开始
    // 写⼊⼀个新的块
    if (leftover < kHeaderSize) {
      // Switch to a new block
      if (leftover > 0) {
        // Fill the trailer (literal below relies on kHeaderSize being 7)
        static_assert(kHeaderSize == 7, "");
        dest_->Append(Slice("\x00\x00\x00\x00\x00\x00", leftover));
      }
      block_offset_ = 0;
    }

    // Invariant: we never leave < kHeaderSize bytes in a block.
    assert(kBlockSize - block_offset_ - kHeaderSize >= 0);

    // 计算块剩余空间
    const size_t avail = kBlockSize - block_offset_ - kHeaderSize;
    // 当前块能够写⼊的数据⼤⼩取决于记录剩余内容和块剩余空间之中⽐较⼩的值
    const size_t fragment_length = (left < avail) ? left : avail;

    RecordType type;
    // end字段表⽰该条记录是否已经完整地写⼊
    const bool end = (left == fragment_length);
    // 通过begin和end字段组合判断头部类型
    if (begin && end) {
      type = kFullType; // 开始写⼊时就完整地写⼊了⼀条记录，因此为kFullType
    } else if (begin) {
      type = kFirstType; // 开始写⼊但未完整写⼊，因此是kFirstType
    } else if (end) {
      type = kLastType; // 完整写⼊但已经不是第⼀次写⼊，因此是kLastType
    } else {
      type = kMiddleType; // 既不是第⼀次写⼊也不是完整写⼊，因此为kMiddleType
    }

    // 将数据写⼊并刷新到磁盘⽂件，然后更新block_offset_字段⻓度
    s = EmitPhysicalRecord(type, ptr, fragment_length);
    ptr += fragment_length; // 更新需要写⼊的记录指针
    left -= fragment_length; // 更新需要写⼊的记录内容⼤⼩
    begin = false; // 第⼀次写⼊之后将begin置为false
  } while (s.ok() && left > 0); // ⼀直到left不⼤于0或者某次写⼊失败时停⽌
  return s;
}

// EmitPhysicalRecord函数写⼊内容，该函数的作⽤为⽣成头部4字节校
// 验字段，并且按照记录结构写⼊⽂件并且刷新到磁盘，最后将block_offset_字段⻓度增加，
// 即更新写⼊偏移量。
Status Writer::EmitPhysicalRecord(RecordType t, const char* ptr,
                                  size_t length) {
  assert(length <= 0xffff);  // Must fit in two bytes
  assert(block_offset_ + kHeaderSize + length <= kBlockSize);

  // Format the header
  // Head, checksum (4 bytes), length (2 bytes), type (1 byte).
  char buf[kHeaderSize];
  // 写入checksum
  buf[4] = static_cast<char>(length & 0xff);
  // 写入长度
  buf[5] = static_cast<char>(length >> 8);
  // 写入Type
  buf[6] = static_cast<char>(t);

  // Compute the crc of the record type and the payload.
  // crc32c生成的checksum，替换前面写入的checksum
  uint32_t crc = crc32c::Extend(type_crc_[t], ptr, length);
  crc = crc32c::Mask(crc);  // Adjust for storage
  EncodeFixed32(buf, crc);

  // Write the header and the payload
  // 写入log数据内容
  Status s = dest_->Append(Slice(buf, kHeaderSize));
  if (s.ok()) {
    s = dest_->Append(Slice(ptr, length));
    if (s.ok()) {
      s = dest_->Flush();
    }
  }

  // 更新 block_offset 偏移量
  block_offset_ += kHeaderSize + length;
  return s;
}

}  // namespace log
}  // namespace leveldb
