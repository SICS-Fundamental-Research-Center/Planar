#ifndef GRAPH_SYSTEMS_ARRAY_H
#define GRAPH_SYSTEMS_ARRAY_H

#include <cstddef>
#include <cstdio>

#include "core/common/types.h"
#include "core/util/atomic.h"

namespace sics::graph::nvme::apis {

using namespace core::util;

// TODO: Use mmap?
template <typename T>
class Array {
  using VertexID = sics::graph::core::common::VertexID;

 public:
  typedef T raw_value_type;
  typedef T value_type;
  typedef size_t size_type;
  typedef ptrdiff_t difference_type;
  typedef value_type& reference;
  typedef const value_type& const_reference;
  typedef value_type* pointer;
  typedef const value_type* const_pointer;
  typedef pointer iterator;
  typedef const_pointer const_iterator;
  const static bool has_value = true;

  void InitMemoryMmap(size_t size) {
    // TODO: use mmap to allocate vertex state
  }

  void InitMemory(size_t size) {
    size_ = size;
    data_ = new T[size];
  }

  Array() : data_(nullptr), size_(0) {}
  // move constructor
  Array(Array&& o) {
    if (&o != this) {
      data_ = o.data_;
      size_ = o.size_;
      o.data_ = nullptr;
      o.size_ = 0;
    }
  }
  // move assignment
  Array& operator=(Array&& o) {
    if (this != &o) {
      data_ = o.data_;
      size_ = o.size_;
      o.data_ = nullptr;
      o.size_ = 0;
    }
    return *this;
  }

  // delete copy constructor and copy assignment
  Array(const Array&) = delete;
  Array& operator=(const Array&) = delete;

  ~Array() { delete[] data_; }

  const_reference at(difference_type x) const { return data_[x]; }
  reference at(difference_type x) { return data_[x]; }
  const_reference operator[](size_type x) const { return data_[x]; }
  reference operator[](size_type x) { return data_[x]; }
  void set(difference_type x, const_reference v) { data_[x] = v; }
  size_type size() const { return size_; }
  iterator begin() { return data_; }
  const_iterator begin() const { return data_; }
  iterator end() { return data_ + size_; }
  const_iterator end() const { return data_ + size_; }

  // functions for read and write
  // The type T must support '=' operator function

  const_reference Read(VertexID id) const { return data_[id]; }
  reference Read(VertexID id) { return data_[id]; }

  void Write(VertexID id, value_type v) { data_[id] = v; }
  void WriteMin(VertexID id, value_type v) { atomic::WriteMin(data_ + id, v); }
  void WriteMax(VertexID id, value_type v) { atomic::WriteMax(data_ + id, v); }
  void WriteAdd(VertexID id, value_type v) { atomic::WriteAdd(data_ + id, v); }

  void Write(VertexID id, reference v) { data_[id] = v; }
  void WriteMin(VertexID id, reference v) { atomic::WriteMin(data_ + id, v); }
  void WriteMax(VertexID id, reference v) { atomic::WriteMax(data_ + id, v); }
  void WriteAdd(VertexID id, reference v) { atomic::WriteAdd(data_ + id, v); }

  void Sync() {}

  virtual void BlockWriteBack() {}

  void Log() {
    for (size_t i = 0; i < size_; i++) {
      LOGF_INFO("Vertex {}, data {}", i, data_[i]);
    }
  }

 private:
  T* data_;
  size_t size_;
};

}  // namespace sics::graph::nvme::apis

#endif  // GRAPH_SYSTEMS_ARRAY_H
