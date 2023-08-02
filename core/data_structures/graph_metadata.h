#ifndef GRAPH_SYSTEMS_GRAPH_METADATA_H
#define GRAPH_SYSTEMS_GRAPH_METADATA_H

#include <cstdio>
#include <string>
#include <vector>

#include <yaml-cpp/yaml.h>

#include "common/types.h"

using sics::graph::core::common::GraphID;
using sics::graph::core::common::VertexID;

namespace sics::graph::core::data_structures {

struct SubgraphMetadata {
  GraphID gid;
  size_t num_vertices;
  size_t num_incoming_edges;
  size_t num_outgoing_edges;
  VertexID max_vid;
  VertexID min_vid;
};

class GraphMetadata {
 public:
  GraphMetadata() = default;
  // maybe used later
  GraphMetadata(const std::string& root_path);

  void set_num_vertices(size_t num_vertices) { num_vertices_ = num_vertices; }
  void set_num_incoming_edges(size_t num_incoming_edges) {
    num_incoming_edges_ = num_incoming_edges;
  }
  void set_num_outgoing_edges(size_t num_outgoing_edges) {
    num_outgoing_edges_ = num_outgoing_edges;
  }
  void set_max_vid(VertexID max_vid) { max_vid_ = max_vid; }
  void set_min_vid(VertexID min_vid) { min_vid_ = min_vid; }
  void set_num_subgraphs(size_t num_subgraphs) {
    num_subgraphs_ = num_subgraphs;
  }
  size_t get_num_vertices() const { return num_vertices_; }
  size_t get_num_outgoing_edges() const { return num_outgoing_edges_; }
  size_t get_num_incoming_edges() const { return num_incoming_edges_; }
  size_t get_num_edges() const {
    return num_outgoing_edges_ + num_incoming_edges_;
  }
  size_t get_num_subgraphs() const { return num_subgraphs_; }
  size_t get_min_vid() const { return min_vid_; }
  size_t get_max_vid() const { return max_vid_; }
  void set_subgraph_metadatas(
      const std::vector<SubgraphMetadata>& subgraph_metadatas) {
    this->subgraph_metadatas_ = subgraph_metadatas;
  }

  SubgraphMetadata GetSubgraphMetadata(common::GraphID gid) const {
    return subgraph_metadatas_.at(gid);
  }

  bool IsSubgraphPendingCurrentRound(common::GraphID subgraph_gid) const {
    return current_round_pending_.at(subgraph_gid);
  }

  bool IsSubgraphPendingNextRound(common::GraphID subgraph_gid) const {
    return next_round_pending_.at(subgraph_gid);
  }

 private:
  size_t num_vertices_;
  size_t num_incoming_edges_;
  size_t num_outgoing_edges_;
  VertexID max_vid_;
  VertexID min_vid_;
  size_t num_subgraphs_;
  std::vector<std::vector<int>> dependency_matrix_;
  std::string data_root_path_;
  std::vector<bool> current_round_pending_;
  std::vector<bool> next_round_pending_;
  std::vector<SubgraphMetadata> subgraph_metadatas_;
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
    node["gid"] = subgraph_metadata.gid;
    node["num_vertices"] = subgraph_metadata.num_vertices;
    node["num_incoming_edges"] = subgraph_metadata.num_incoming_edges;
    node["num_outgoing_edges"] = subgraph_metadata.num_outgoing_edges;
    node["max_vid"] = subgraph_metadata.max_vid;
    node["min_vid"] = subgraph_metadata.min_vid;
    return node;
  }
  static bool decode(
      const Node& node,
      sics::graph::core::data_structures::SubgraphMetadata& subgraph_metadata) {
    if (node.size() != 6) {
      return false;
    }
    subgraph_metadata.gid =
        node["gid"].as<sics::graph::core::common::GraphID>();
    subgraph_metadata.num_vertices = node["num_vertices"].as<size_t>();
    subgraph_metadata.num_incoming_edges =
        node["num_incoming_edges"].as<size_t>();
    subgraph_metadata.num_outgoing_edges =
        node["num_outgoing_edges"].as<size_t>();
    subgraph_metadata.max_vid = node["max_vid"].as<size_t>();
    subgraph_metadata.min_vid = node["min_vid"].as<size_t>();
    return true;
  }
};

// template is needed for this function
template <>
struct convert<sics::graph::core::data_structures::GraphMetadata> {
  static Node encode(
      const sics::graph::core::data_structures::GraphMetadata& metadata) {
    Node node;
    node["num_vertices"] = metadata.get_num_vertices();
    node["num_incoming_edges"] = metadata.get_num_incoming_edges();
    node["num_outgoing_edges"] = metadata.get_num_outgoing_edges();
    node["num_subgraphs"] = metadata.get_num_subgraphs();
    std::vector<sics::graph::core::data_structures::SubgraphMetadata> tmp;
    for (size_t i = 0; i < metadata.get_num_subgraphs(); i++) {
      tmp.push_back(metadata.GetSubgraphMetadata(0));
    }
    node["subgraphs"] = tmp;
    return node;
  }

  static bool decode(
      const Node& node,
      sics::graph::core::data_structures::GraphMetadata& metadata) {
    if (node.size() != 7) {
      return false;
    }
    metadata.set_num_vertices(node["num_vertices"].as<size_t>());
    metadata.set_num_incoming_edges(node["num_incoming_edges"].as<size_t>());
    metadata.set_num_outgoing_edges(node["num_outgoing_edges"].as<size_t>());
    metadata.set_max_vid(node["max_vid"].as<VertexID>());
    metadata.set_min_vid(node["min_vid"].as<VertexID>());
    metadata.set_num_subgraphs(node["num_subgraphs"].as<size_t>());
    auto subgraphMetadatas =
        node["subgraphs"]
            .as<std::vector<
                sics::graph::core::data_structures::SubgraphMetadata>>();
    metadata.set_subgraph_metadatas(subgraphMetadatas);
    return true;
  }
};
}  // namespace YAML

#endif  // GRAPH_SYSTEMS_GRAPH_METADATA_H
