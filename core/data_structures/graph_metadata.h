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

  void set_num_vertices(size_t num_vertices) { num_vertices_ = num_vertices; }
  void set_num_edges(size_t num_edges) { num_edges_ = num_edges; }
  void set_num_subgraphs(size_t num_subgraphs) { num_subgraphs_ = num_subgraphs; }
  size_t get_num_vertices() const { return num_vertices_; }
  size_t get_num_edges() const { return num_edges_; }
  size_t get_num_subgraphs() const { return num_subgraphs_; }

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

// used for read meta.yaml when serialized(encode) and deserialize(decode)
namespace YAML {

// template is needed for this function
template <>
struct convert<sics::graph::core::data_structures::SubgraphMetadata> {
  static Node encode(const sics::graph::core::data_structures::SubgraphMetadata&
                         subgraph_metadata) {
    Node node;
    node["gid"] = subgraph_metadata.gid_;
    node["num_vertices"] = subgraph_metadata.num_vertices_;
    node["num_edges"] = subgraph_metadata.num_edges_;
    node["size"] = subgraph_metadata.size_;
    return node;
  }
  static bool decode(
      const Node& node,
      sics::graph::core::data_structures::SubgraphMetadata& subgraph_metadata) {
    if (node.size() != 4) {
      return false;
    }
    subgraph_metadata.gid_ =
        node["gid"].as<sics::graph::core::common::GraphID>();
    subgraph_metadata.num_vertices_ = node["num_vertices"].as<size_t>();
    subgraph_metadata.num_edges_ = node["num_edges"].as<size_t>();
    subgraph_metadata.size_ = node["size"].as<size_t>();
    return true;
  }
};

// template is needed for this function
template<>
struct convert<sics::graph::core::data_structures::GraphMetadata> {
  static Node encode(
      const sics::graph::core::data_structures::GraphMetadata& metadata) {
    Node node;
    node["num_vertices"] = metadata.get_num_vertices();
    node["num_edges"] = metadata.get_num_edges();
    node["num_subgraphs"] = metadata.get_num_subgraphs();
    node["subgraphs"] =
        std::vector<sics::graph::core::data_structures::SubgraphMetadata>();
    for (int i = 0; i < metadata.get_num_subgraphs(); i++) {
    }
    return node;
  }

  static bool decode(
      const Node& node,
      sics::graph::core::data_structures::GraphMetadata& metadata) {
    if (node.size() != 4) {
      return false;
    }
    metadata.set_num_vertices(node["num_vertices"].as<size_t>());
    metadata.set_num_edges(node["num_edges"].as<size_t>());
    metadata.set_num_subgraphs(node["num_subgraphs"].as<size_t>());
    auto subgraphMetadatas =
        node["subgraphs"]
            .as<std::vector<
                sics::graph::core::data_structures::SubgraphMetadata>>();
    for (int i = 0; i < metadata.get_num_subgraphs(); i++) {
      metadata.AddSubgraphMetadata(subgraphMetadatas[i]);
    }
    return true;
  }
};
}  // namespace YAML

#endif  // GRAPH_SYSTEMS_GRAPH_METADATA_H
