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
  // pointer p should be owned
  explicit OwnedBuffer(size_t s, std::unique_ptr<uint8_t> p)
      : p_(p.release()), s_(s) {}
  ~OwnedBuffer() { delete p_; }
  OwnedBuffer(const OwnedBuffer& r) = delete;
  OwnedBuffer(OwnedBuffer&& r) noexcept : p_(r.p_), s_(r.s_) {
    r.p_ = nullptr;
    r.s_ = 0;
  };
  OwnedBuffer& operator=(const OwnedBuffer& r) = delete;
  OwnedBuffer& operator=(OwnedBuffer&& r) noexcept {
    if (this != &r) {
      delete p_;
      p_ = r.p_;
      s_ = r.s_;
      r.p_ = nullptr;
      r.s_ = 0;
    }
    return *this;
  };

  Buffer GetReference(size_t offset, size_t s) {
    return Buffer(p_ + offset, s);
  }

  uint8_t* Get(size_t offset = 0) const { return p_ + offset; }

  size_t GetSize() const { return s_; }

  // used to release the ownership of the pointer
  uint8_t* Release() {
    uint8_t* p = p_;
    p_ = nullptr;
    s_ = 0;
    return p;
  }

 private:
  uint8_t* p_;
  size_t s_;
};

}  // namespace sics::graph::core::data_structures

#endif  // CORE_DATA_STRUCTURES_BUFFER_H_
