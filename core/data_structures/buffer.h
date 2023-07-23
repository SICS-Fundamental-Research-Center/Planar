#ifndef CORE_DATA_STRUCTURES_BUFFER_H_
#define CORE_DATA_STRUCTURES_BUFFER_H_

#include <cstdint>
#include <cstddef>
#include <cstdlib>


namespace sics::graph::core::data_structures {

class Buffer {
 public:
  explicit Buffer(uint8_t* p,  size_t s) : p_(p), s_(s) {}
  virtual ~Buffer() = default;

  uint8_t* Get(size_t offset = 0) {
    return p_ + offset;
  }

  size_t GetSize() const {
    return s_;
  }

  void SetPointer(uint8_t* p) {
    p_ = p;
  }

 protected:
  uint8_t* p_;
  size_t  s_;
};


class OwnedBuffer : public Buffer {
 public:
  explicit OwnedBuffer(size_t s) : Buffer(reinterpret_cast<uint8_t*>(malloc(s)), s) {}
  OwnedBuffer(const OwnedBuffer& r) = delete;
  OwnedBuffer(OwnedBuffer&& r) = default;
  ~OwnedBuffer() override {
    delete p_;
  }

  Buffer GetReference(size_t offset, size_t s);
};

}  // namespace sics::graph::core::data_structures

#endif  // CORE_DATA_STRUCTURES_BUFFER_H_

