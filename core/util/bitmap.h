#ifndef CORE_UTIL_BITMAP_H_
#define CORE_UTIL_BITMAP_H_

#include <cassert>
#include <cstring>
#include <stdint.h>

namespace sics::graph::core::util {

#define WORD_OFFSET(i) (i >> 6)
#define BIT_OFFSET(i) (i & 0x3f)

// @DESCRIPTION
//
// Bitmap is a mapping from integers~(indexes) to bits. If the unit is
// occupied, the bit is a nonzero integer constant and if it is empty, the bit
// is zero.
class Bitmap {
 public:
  Bitmap(size_t size) {
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

  bool IsEmpty() const {
    size_t bm_size = WORD_OFFSET(size_);
    for (size_t i = 0; i <= bm_size; i++)
      if (data_[i] != 0) return false;
    return true;
  }

  bool IsEqual(Bitmap& b) const {
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

  bool GetBit(size_t i) const {
    if (i > size_) return 0;
    return data_[WORD_OFFSET(i)] & (1ul << BIT_OFFSET(i));
  }

  void SetBit(size_t i) {
    if (i > size_) return;
    __sync_fetch_and_or(data_ + WORD_OFFSET(i), 1ul << BIT_OFFSET(i));
    return;
  }

  void ClearBit(const size_t i) {
    if (i > size_) return;
    __sync_fetch_and_and(data_ + WORD_OFFSET(i), ~(1ul << BIT_OFFSET(i)));
    return;
  }

  size_t Count() const {
    auto count  = 0;
    for (size_t i = 0; i <= WORD_OFFSET(i); i++) {
      auto x = data_[i];
      x = (x & (0x5555555555555555)) + ((x >> 1) & (0x5555555555555555));
      x = (x & (0x3333333333333333)) + ((x >> 2) & (0x3333333333333333));
      x = (x & (0x0f0f0f0f0f0f0f0f)) + ((x >> 4) & (0x0f0f0f0f0f0f0f0f));
      x = (x & (0x00ff00ff00ff00ff)) + ((x >> 8) & (0x00ff00ff00ff00ff));
      x = (x & (0x0000ffff0000ffff)) + ((x >> 16) & (0x0000ffff0000ffff));
      x = (x & (0x00000000ffffffff)) + ((x >> 32) & (0x00000000ffffffff));
      count +=x;
    }
    return count;
  }

  size_t size() const { return size_; }

 private:
  size_t size_ = 0;
  uint64_t* data_ = nullptr;
};

}  // namespace sics::graph::core::util

#endif  // CORE_UTIL_BITMAP_H_
