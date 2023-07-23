#ifndef GRAPH_SYSTEMS_BITMAP_H
#define GRAPH_SYSTEMS_BITMAP_H

#include <cassert>
#include <cstring>
#include <stdint.h>

namespace sics::graph::core::util {

#define WORD_OFFSET(i) (i >> 6)
#define BIT_OFFSET(i) (i & 0x3f)

// @DESCRIPTION
//
// Bitmap is a mapping from one integer~(index) to bits. If the unit is
// occupied, the bit is 1 and if it is empty, the bit is zero.
class Bitmap {
 public:
  Bitmap(const size_t size) {
    Init(size);
    return;
  }

  ~Bitmap() {
    if (data_ != nullptr) delete data_;
    size_ = 0;
    return;
  }

  void Init(size_t size) {
    size_ = size;
    data_ = new uint64_t[WORD_OFFSET(size) + 1];
    return;
  }

  void Clear() {
    size_t bm_size = WORD_OFFSET(size_);
    for (size_t i = 0; i <= bm_size; i++) data_[i] = 0;
    return;
  }

  bool IsEmpty() {
    size_t bm_size = WORD_OFFSET(size_);
    for (size_t i = 0; i <= bm_size; i++)
      if (data_[i] != 0) return false;
    return true;
  }

  bool IsEqual(Bitmap& b) {
    if (size_ != b.size_) return false;
    size_t bm_size = WORD_OFFSET(size_);
    for (size_t i = 0; i <= bm_size; i++)
      if (data_[i] != b.data_[i]) return false;
    return true;
  }

  void Fill() {
    size_t bm_size = WORD_OFFSET(size_);
    for (size_t i = 0; i < bm_size; i++) {
      data_[i] = 0xffffffffffffffff;
    }
    data_[bm_size] = 0;
    for (size_t i = (bm_size << 6); i < size_; i++) {
      data_[bm_size] |= 1ul << BIT_OFFSET(i);
    }
    return;
  }

  uint64_t GetBit(size_t i) const {
    if (i > size_) return 0;
    return data_[WORD_OFFSET(i)] & (1ul << BIT_OFFSET(i));
  }

  void SetBit(size_t i) {
    if (i > size_) return;
    __sync_fetch_and_or(data_ + WORD_OFFSET(i), 1ul << BIT_OFFSET(i));
    return;
  }

  void RMBit(const size_t i) {
    if (i > size_) return;
    __sync_fetch_and_and(data_ + WORD_OFFSET(i), ~(1ul << BIT_OFFSET(i)));
    return;
  }

  size_t GetNumBit() const {
    size_t count = 0;
    for (size_t i = 0; i < size_; i++)
      if (GetBit(i)) count++;
    return count;
  }

  size_t size() const { return size_; }

 private:
  size_t size_ = 0;
  uint64_t* data_ = nullptr;
};

}  // namespace sics::graph::core::util

#endif  // GRAPH_SYSTEMS_BITMAP_H
