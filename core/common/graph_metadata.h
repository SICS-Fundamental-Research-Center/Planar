//
// Created by Yang Liu on 2023/7/27.
//

#ifndef GRAPH_SYSTEMS_GRAPH_METADATA_H
#define GRAPH_SYSTEMS_GRAPH_METADATA_H

#include <cstdio>
#include <string>
#include <vector>

namespace sics::graph::core::common {

class SubgraphMetadata {};

class GraphMetadata {
 private:
  size_t num_vertices;
  size_t num_edges;
  size_t num_subgraphs;
  std::vector<std::vector<int>> dependency_matrix;
  const std::string data_root_path;
  std::vector<bool> current_round_pending;
  std::vector<bool> next_round_pending;
  std::vector<SubgraphMetadata> subgraph_metadata;

 public:
  SubgraphMetadata GetSubgraphMetadata(int gid);

  uint16_t GetSuperStep(int gid);
};

}  // namespace sics::graph::core::common

#endif  // GRAPH_SYSTEMS_GRAPH_METADATA_H
