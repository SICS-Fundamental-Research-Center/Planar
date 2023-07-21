#ifndef GRAPH_SYSTEMS_BUFFER_H
#define GRAPH_SYSTEMS_BUFFER_H

#include <cstdint>
#include <cstddef>
#include <cstdlib>


namespace sics::graph::core::data_structures {

class Buffer {
 public:
  Buffer(uint8_t* p,  size_t s) : p_(p), s_(s) {}
  virtual ~Buffer() = default;

  uint8_t* Get(size_t offset = 0) {
    return p_ + offset;
  }

  size_t GetSize() const {
    return s_;
  }

 protected:
  uint8_t* p_;
  size_t  s_;
};


class OwnedBuffer : public Buffer {
 public:
  OwnedBuffer(size_t s) : Buffer((uint8_t*) malloc(s), s) {}
  OwnedBuffer(const OwnedBuffer& r) = delete;
  OwnedBuffer(OwnedBuffer&& r) = default;
  ~OwnedBuffer() override {
    delete p_;
  }

  Buffer GetReference(size_t offset, size s);
};

} // namespace sics::graph::core::data_structures

#endif  // GRAPH_SYSTEMS_BUFFER_H
