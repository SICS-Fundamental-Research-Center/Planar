#ifndef TOOLS_COMMON_TYPES_H_
#define TOOLS_COMMON_TYPES_H_

#include <cstddef>
#include <cstdint>
#include <vector>

#include "core/common/types.h"

namespace sics::graph::tools::common {

class Edge {
 private:
  using VertexID = sics::graph::core::common::VertexID;

 public:
  Edge(VertexID src, VertexID dst) : src_(src), dst_(dst) {}

  static void SortBySrc(std::vector<Edge>& edges) {
    sort(edges.begin(), edges.end(),
         [](const Edge& l, const Edge& r) { return l < r; });
  }

  static void SortByDst(std::vector<Edge>& edges) {
    sort(edges.begin(), edges.end(),
         [](const Edge& l, const Edge& r) { return l.dst_ < r.dst_; });
  }

  bool operator<(const Edge& e) const { return src_ < e.src_; }
  bool operator>(const Edge& e) const { return src_ > e.src_; }
  bool operator-(const Edge& e) const { return src_ - e.src_; }

 public:
  VertexID src_;
  VertexID dst_;
};

struct EdgelistMetadata {
 private:
  using VertexID = sics::graph::core::common::VertexID;

 public:
  size_t num_vertices;
  size_t num_edges;
  VertexID max_vid;
};

}  // namespace sics::graph::tools

#endif  // TOOLS_COMMON_TYPES_H_
