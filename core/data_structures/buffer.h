#ifndef CORE_DATA_STRUCTURES_BUFFER_H_
#define CORE_DATA_STRUCTURES_BUFFER_H_

#include <cstddef>
#include <cstdint>
#include <cstdlib>

namespace sics::graph::core::data_structures {

class Buffer {
 public:
  explicit Buffer(uint8_t* p, size_t s) : p_(p), s_(s) {}
  virtual ~Buffer() = default;

  uint8_t* Get(size_t offset = 0) { return p_ + offset; }

  size_t GetSize() const { return s_; }

 private:
  uint8_t* p_;
  size_t s_;
};

class OwnedBuffer {
 public:
  explicit OwnedBuffer(size_t s) : p_((uint8_t*)(malloc(s))), s_(s) {}
  ~OwnedBuffer() { delete p_; }
  OwnedBuffer(const OwnedBuffer& r) = delete;
  OwnedBuffer(OwnedBuffer&& r) = default;
  OwnedBuffer& operator=(const OwnedBuffer& r) = delete;
  OwnedBuffer& operator=(OwnedBuffer&& r) = default;

  Buffer GetReference(size_t offset, size_t s) {
    return Buffer(p_ + offset, s);
  };

  size_t GetSize() const { return s_; }

 private:
  uint8_t* p_;
  size_t s_;
};

}  // namespace sics::graph::core::data_structures

#endif  // CORE_DATA_STRUCTURES_BUFFER_H_
