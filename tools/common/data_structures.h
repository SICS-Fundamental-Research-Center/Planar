#ifndef TOOLS_COMMON_TYPES_H_
#define TOOLS_COMMON_TYPES_H_

#include <cstddef>  // For std::ptrdiff_t
#include <cstdint>
#include <iterator>  // For std::forward_iterator_tag
#include <vector>

#include "core/common/types.h"

namespace sics::graph::tools::common {

struct Edge {
 private:
  using VertexID = sics::graph::core::common::VertexID;

 public:
  Edge(VertexID src, VertexID dst) : src(src), dst(dst) {}

  bool operator<(const Edge& e) const {
    if (src != e.src) {
      return src < e.src;
    } else {
      return dst < e.dst;
    }
  }

 public:
  VertexID src;
  VertexID dst;
};

class Edges {
 public:
  struct Iterator {
    // Iterator tags
    using iterator_category = std::forward_iterator_tag;
    using difference_type = std::ptrdiff_t;
    using value_type = Edge;
    using pointer = Edge*;
    using reference = Edge&;

    // Iterator constructors
    Iterator(pointer ptr) : base_ptr_(ptr) {}

    // Iterator operators
    reference operator*() const { return *base_ptr_; }
    pointer operator->() { return base_ptr_; }
    Iterator& operator++() {
      base_ptr_++;
      return *this;
    }
    // used for clang++ compiling
    Iterator& operator+=(int n) {
      base_ptr_--;
      return *this;
    }
    Iterator& operator--() {
      base_ptr_--;
      return *this;
    }
    Iterator& operator+(int n) {
      base_ptr_ += n;
      return *this;
    }
    Iterator& operator-(int n) {
      base_ptr_ -= n;
      return *this;
    }
    Iterator operator++(int) {
      Iterator tmp = *this;
      ++(*this);
      return tmp;
    }
    bool operator-(const Iterator& b) const {
      return (*base_ptr_).src - (*b).src;
    }
    bool operator<(const Iterator& b) const {
      return (*base_ptr_).src < (*b).src;
    }
    bool operator>(const Iterator& b) const {
      return (*base_ptr_).src > (*b).src;
    }
    // used for clang++ compiling
    bool operator<=(const Iterator& b) const {
      return (*base_ptr_).src <= (*b).src;
    }
    // used for clang++ compiling
    bool operator>=(const Iterator& b) const {
      return (*base_ptr_).src >= (*b).src;
    }
    friend bool operator==(const Iterator& a, const Iterator& b) {
      return a.base_ptr_ == b.base_ptr_;
    };
    friend bool operator!=(const Iterator& a, const Iterator& b) {
      return a.base_ptr_ != b.base_ptr_;
    };

   private:
    pointer base_ptr_;
  };

  Edges(size_t n_edges, Edge* edges_ptr)
      : n_edges_(n_edges), edges_ptr_(edges_ptr) {}

  ~Edges() { delete edges_ptr_; }

  Iterator begin() { return Iterator(&edges_ptr_[0]); }
  Iterator end() { return Iterator(&edges_ptr_[n_edges_ - 1]); }

  static void SortBySrc(Edges edges) {
    std::sort(std::begin(edges), std::end(edges),
              [](const Edge& l, const Edge& r) {
                if (l.src != r.src) {
                  return l.src < r.src;
                } else {
                  return l.dst < r.dst;
                }
              });
  }

  static void SortByDst(Edges edges) {
    // TODO (hsiaoko):
  }

 private:
  size_t n_edges_ = 0;
  Edge* edges_ptr_;
};

struct EdgelistMetadata {
 private:
  using VertexID = sics::graph::core::common::VertexID;

 public:
  size_t num_vertices;
  size_t num_edges;
  VertexID max_vid;
};

}  // namespace sics::graph::tools::common

#endif  // TOOLS_COMMON_TYPES_H_
