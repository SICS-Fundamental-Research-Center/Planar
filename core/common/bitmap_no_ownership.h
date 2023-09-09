#ifndef GRAPH_SYSTEMS_CORE_COMMON_BITMAP_NO_OWNERSHIP_H_
#define GRAPH_SYSTEMS_CORE_COMMON_BITMAP_NO_OWNERSHIP_H_

#include "common/bitmap.h"

namespace sics::graph::core::common {

class BitmapNoOwnerShip : public Bitmap {
 public:
  BitmapNoOwnerShip() = default;
  BitmapNoOwnerShip(size_t size) : Bitmap() {
    size_ = size;
  }
  BitmapNoOwnerShip(size_t size, uint64_t* data) : Bitmap() {
    Init(size, data);
  }
  ~BitmapNoOwnerShip() {
    data_ = nullptr;
    size_ = 0;
  };

  void Init(size_t size, uint64_t* data) {
    size_ = size;
    data_ = data;
  }
};

}  // namespace sics::graph::core::common

#endif  // GRAPH_SYSTEMS_CORE_COMMON_BITMAP_NO_OWNERSHIP_H_
