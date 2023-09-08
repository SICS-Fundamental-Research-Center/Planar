#ifndef TOOLS_COMMON_TYPES_H_
#define TOOLS_COMMON_TYPES_H_

#include "core/common/bitmap.h"
#include "core/common/multithreading/thread_pool.h"
#include "core/common/types.h"
#include "core/util/atomic.h"
#include "core/util/logging.h"
#include "tools/common/data_structures.h"
#include <cstddef>  // For std::ptrdiff_t
#include <cstdint>
#include <iterator>  // For std::forward_iterator_tag
#include <vector>

namespace sics::graph::tools::common {

struct EdgelistMetadata {
 private:
  using VertexID = sics::graph::core::common::VertexID;

 public:
  size_t num_vertices;
  size_t num_edges;
  VertexID max_vid;
};

struct Edge {
 private:
  using VertexID = sics::graph::core::common::VertexID;

 public:
  Edge(VertexID src, VertexID dst) : src(src), dst(dst) {}
  Edge() = default;

  bool operator<(const Edge& e) const {
    if (src != e.src) {
      return src < e.src;
    } else {
      return dst < e.dst;
    }
  }

  bool operator>(VertexID n) const { return src > n; }
  bool operator<(VertexID n) const { return src < n; }

  bool operator()(VertexID n, const Edge& e) const { return n > e.src; }

 public:
  VertexID src;
  VertexID dst;
};

class Edges {
 private:
  using VertexID = sics::graph::core::common::VertexID;
  using Bitmap = sics::graph::core::common::Bitmap;
  using TaskPackage = sics::graph::core::common::TaskPackage;
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

  ~Edges() { delete edges_ptr_; }

  Iterator begin() { return Iterator(&edges_ptr_[0]); }
  Iterator end() {
    return Iterator(&edges_ptr_[edgelist_metadata_.num_edges - 1]);
  }

  void SortBySrc() {
    std::sort(std::begin(*this), std::end(*this),
              [](const Edge& l, const Edge& r) {
                if (l.src != r.src) {
                  return l.src < r.src;
                }
                if (l.dst != r.dst) {
                  return l.dst < r.dst;
                }
              });
  }

  void SortByDst() {
    std::sort(std::begin(*this), std::end(*this),
              [](const Edge& l, const Edge& r) {
                if (l.dst != r.dst) {
                  return l.dst < r.dst;
                }
                if (l.src != r.src) {
                  return l.src < r.src;
                }
              });
  }

  // count the number of vertices and find the vertex with maximum
  // outdegree.
  VertexID GetVertexWithMaximumDegree() {
    auto parallelism = std::thread::hardware_concurrency();
    auto thread_pool = sics::graph::core::common::ThreadPool(parallelism);
    auto task_package = TaskPackage();
    std::mutex mtx;

    auto aligned_max_vid = ((edgelist_metadata_.max_vid >> 6) << 6) + 64;
    VertexID outdegree_by_vid[aligned_max_vid] = {0};
    VertexID max_outdegree = 0, vid_with_maximum_degree = 0;

    for (unsigned int i = 0; i < parallelism; i++) {
      auto task = std::bind([&, i]() {
        for (VertexID j = i; j < edgelist_metadata_.num_edges;
             j += parallelism) {
          auto src = get_src_by_index(j);
          sics::graph::core::util::atomic::WriteAdd(outdegree_by_vid + src,
                                                    (VertexID)1);
          std::lock_guard<std::mutex> lck(mtx);
          if (sics::graph::core::util::atomic::WriteMax(
                  &max_outdegree, outdegree_by_vid[src])) {
            vid_with_maximum_degree = src;
          }
        }
      });
      task_package.push_back(task);
    }
    thread_pool.SubmitSync(task_package);
    task_package.clear();

    return vid_with_maximum_degree;
  }

  Iterator SearchVertex(VertexID vid) {
    auto iter = std::lower_bound(this->begin(), this->end(), vid);
    return iter;
  }

  void AssignEdge(size_t pos, const Edge& e) { edges_ptr_[pos] = e; }

  Edge* get_base_ptr() { return edges_ptr_; }
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
