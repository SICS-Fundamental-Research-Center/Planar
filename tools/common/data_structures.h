#ifndef TOOLS_COMMON_TYPES_H_
#define TOOLS_COMMON_TYPES_H_

#include <cstring>

#include "core/common/types.h"

namespace sics::graph::tools::common {

struct EdgelistMetadata {
 private:
  using VertexID = sics::graph::core::common::VertexID;
  using EdgeIndex = sics::graph::core::common::EdgeIndex;

 public:
  VertexID num_vertices;
  EdgeIndex num_edges;
  VertexID max_vid;
};

struct Edge {
 private:
  using VertexID = sics::graph::core::common::VertexID;

 public:
  Edge(VertexID src, VertexID dst) : src(src), dst(dst) {}
  Edge() = default;

  bool operator<(const Edge& b) const { return src < b.src; }

 public:
  VertexID src;
  VertexID dst;
};

class Edges {
 private:
  using VertexID = sics::graph::core::common::VertexID;
  using EdgelistMetadata = sics::graph::tools::common::EdgelistMetadata;

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
      base_ptr_ += n;
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

    pointer get_base_ptr() const { return base_ptr_; };

   private:
    pointer base_ptr_;
  };

  Edges(const EdgelistMetadata& edgelist_metadata, Edge* edges_ptr)
      : edgelist_metadata_(edgelist_metadata), edges_ptr_(edges_ptr) {}

  Edges(const EdgelistMetadata& edgelist_metadata)
      : edgelist_metadata_(edgelist_metadata) {
    edges_ptr_ = new Edge[edgelist_metadata.num_edges]();
  }

  Edges(const Edges& edges) {
    edgelist_metadata_ = edges.get_metadata();
    edges_ptr_ = new Edge[edgelist_metadata_.num_edges]();
    memcpy(edges_ptr_, edges.get_base_ptr(),
           sizeof(Edge) * edgelist_metadata_.num_edges);
  }

  ~Edges() { delete[] edges_ptr_; }

  Iterator begin() { return Iterator(&edges_ptr_[0]); }
  Iterator end() {
    return Iterator(&edges_ptr_[edgelist_metadata_.num_edges - 1]);
  }

  // Sort edges by source.
  void SortBySrc();

  // Sort edges by destination.
  void SortByDst();

  // Count the number of vertices and find the vertex with maximum
  // outdegree.
  VertexID GetVertexWithMaximumDegree();

  // Find the index of given vid via binary search.
  Iterator SearchVertex(VertexID vid);

  Edge* get_base_ptr() const { return edges_ptr_; }
  EdgelistMetadata get_metadata() const { return edgelist_metadata_; }

  VertexID get_src_by_index(size_t i) const { return edges_ptr_[i].src; }
  VertexID get_dst_by_index(size_t i) const { return edges_ptr_[i].dst; }
  Edge get_edge_by_index(size_t i) const { return edges_ptr_[i]; }
  size_t get_index_by_iter(const Iterator& iter) {
    return (iter.get_base_ptr() - begin().get_base_ptr());
  };

 private:
  Edge* edges_ptr_;
  EdgelistMetadata edgelist_metadata_;
};

}  // namespace sics::graph::tools::common

#endif  // TOOLS_COMMON_TYPES_H_
