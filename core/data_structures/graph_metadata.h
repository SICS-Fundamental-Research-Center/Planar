#ifndef GRAPH_SYSTEMS_GRAPH_METADATA_H
#define GRAPH_SYSTEMS_GRAPH_METADATA_H

#include "common/types.h"
#include "util/logging.h"

#include <cstdio>
#include <string>
#include <vector>

#include <yaml-cpp/yaml.h>

namespace sics::graph::core::data_structures {

struct SubgraphMetadata {
  common::GraphID gid_;
  size_t num_vertices_;
  size_t num_edges_;
  size_t size_;  // need this??
};

class GraphMetadata {
 public:
  GraphMetadata() {}
  // maybe used later
  GraphMetadata(const std::string& root_path);

  void SetNumVertices(size_t num_vertices) { num_vertices_ = num_vertices; }
  void SetNumEdges(size_t num_edges) { num_edges_ = num_edges; }
  void SetNumSubgraphs(size_t num_subgraphs) { num_subgraphs_ = num_subgraphs; }
  size_t GetNumVertices() const { return num_vertices_; }
  size_t GetNumEdges() const { return num_edges_; }
  size_t GetNumSubgraphs() const { return num_subgraphs_; }

  void AddSubgraphMetadata(SubgraphMetadata& subgraphMetadata) {
    subgraph_metadata_.emplace_back(subgraphMetadata);
  }

  SubgraphMetadata& GetSubgraphMetadata(common::GraphID gid) {
    return subgraph_metadata_.at(gid);
  }

  bool IsSubgraphPendingCurrentRound(common::GraphID subgraph_gid) const {
    return current_round_pending_.at(subgraph_gid);
  }

  bool IsSubgraphPendingNextRound(common::GraphID subgraph_gid) {
    return next_round_pending_.at(subgraph_gid);
  }

 private:
  size_t num_vertices_;
  size_t num_edges_;
  size_t num_subgraphs_;
  std::vector<std::vector<int>> dependency_matrix_;
  std::string data_root_path_;
  std::vector<bool> current_round_pending_;
  std::vector<bool> next_round_pending_;
  std::vector<SubgraphMetadata> subgraph_metadata_;
};

}  // namespace sics::graph::core::data_structures

#endif  // GRAPH_SYSTEMS_GRAPH_METADATA_H
