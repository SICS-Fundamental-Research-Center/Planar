#ifndef MINICLEAN_COMPONENTS_PREPROCESSOR_STAR_BITMAP_H_
#define MINICLEAN_COMPONENTS_PREPROCESSOR_STAR_BITMAP_H_

#include <bitset>
#include <vector>

namespace xyz::graph::miniclean::components::preprocessor {

#define WORD_OFFSET(i) (i >> 6)
#define BIT_OFFSET(i) (i & 0x3f)

class StarBitmap {
 public:
  StarBitmap() = default;
  StarBitmap(size_t map_size) : map_size_(map_size) {
    size_t bitmap_size = WORD_OFFSET(map_size) + 1;
    star_bitmap_.resize(bitmap_size);
  }

  void Clear() {
    for (size_t i = 0; i < star_bitmap_.size(); ++i) {
      star_bitmap_[i].reset();
    }
  }

  void SetBit(size_t index) {
    star_bitmap_[WORD_OFFSET(index)].set(BIT_OFFSET(index));
  }

  void ResetBit(size_t index) {
    star_bitmap_[WORD_OFFSET(index)].reset(BIT_OFFSET(index));
  }

  bool TestBit(size_t index) {
    return star_bitmap_[WORD_OFFSET(index)].test(BIT_OFFSET(index));
  }

  size_t get_bitset_num() const { return star_bitmap_.size(); }

  size_t get_bitset_count(size_t i) const { return star_bitmap_.at(i).count(); }

  size_t get_map_size() const { return map_size_; }

  bool MergeWith(const StarBitmap& other) {
    if (other.get_bitset_num() != get_bitset_num()) {
      return false;
    }
    for (size_t i = 0; i < star_bitmap_.size(); ++i) {
      star_bitmap_[i] &= other.star_bitmap_[i];
    }
    return true;
  }

  size_t CountOneBits() {
    size_t num_one = 0;
    for (size_t i = 0; i < star_bitmap_.size(); ++i) {
      num_one += star_bitmap_[i].count();
    }
    return num_one;
  }

 private:
  std::vector<std::bitset<64>> star_bitmap_;
  size_t map_size_ = 0;
};

}  // namespace xyz::graph::miniclean::components::preprocessor

#endif  // MINICLEAN_COMPONENTS_PREPROCESSOR_STAR_BITMAP_H_