#ifndef GRAPH_SYSTEMS_GRAPH_METADATA_H
#define GRAPH_SYSTEMS_GRAPH_METADATA_H

#include "common/types.h"
#include "util/logging.h"
#include <yaml-cpp/yaml.h>
#include <cstdio>
#include <string>
#include <vector>

namespace sics::graph::core::data_structures {

struct SubgraphMetadata {
  using GraphID = sics::graph::core::common::GraphID;
  using VertexID = sics::graph::core::common::VertexID;

  // Subgraph Metadata
  GraphID gid;
  size_t num_vertices;
  size_t num_incoming_edges;
  size_t num_outgoing_edges;
  VertexID max_vid;
  VertexID min_vid;
};

class GraphMetadata {
  using GraphID = sics::graph::core::common::GraphID;
  using VertexID = sics::graph::core::common::VertexID;

 public:
  GraphMetadata() = default;
  // TO DO: maybe used later
  explicit GraphMetadata(const std::string& root_path);

  void set_num_vertices(size_t num_vertices) { num_vertices_ = num_vertices; }
  void set_num_edges(size_t num_edges) { num_edges_ = num_edges; }
  void set_max_vid(VertexID max_vid) { max_vid_ = max_vid; }
  void set_min_vid(VertexID min_vid) { min_vid_ = min_vid; }
  void set_num_subgraphs(size_t num_subgraphs) {
    num_subgraphs_ = num_subgraphs;
  }
  size_t get_num_vertices() const { return num_vertices_; }
  size_t get_num_edges() const { return num_edges_; }
  size_t get_num_subgraphs() const { return num_subgraphs_; }
  size_t get_min_vid() const { return min_vid_; }
  size_t get_max_vid() const { return max_vid_; }

  void set_subgraph_metadata_vec(
      const std::vector<SubgraphMetadata>& subgraph_metadata_vec) {
    subgraph_metadata_vec_ = subgraph_metadata_vec;
  }

  SubgraphMetadata GetSubgraphMetadata(common::GraphID gid) const {
    return subgraph_metadata_vec_.at(gid);
  }

  void Init();

  bool IsSubgraphPendingCurrentRound(common::GraphID subgraph_gid) const {
    return !current_round_pending_.at(subgraph_gid);
  }

  //  bool IsSubgraphPendingNextRound(common::GraphID subgraph_gid) const {
  //    return next_round_pending_.at(subgraph_gid);
  //  }

  void SetSubgraphLoaded(common::GraphID gid) {
    current_round_pending_.at(gid) = false;
  }

  common::GraphID GetNextLoadGraph();

  void SyncNextRound();

 private:
  size_t num_vertices_;
  size_t num_edges_;
  VertexID max_vid_;
  VertexID min_vid_;
  size_t num_subgraphs_;
  std::vector<std::vector<int>> dependency_matrix_;
  std::string data_root_path_;
  std::vector<bool> current_round_pending_;
  std::vector<bool> next_round_pending_;
  std::vector<SubgraphMetadata> subgraph_metadata_vec_;
};

}  // namespace sics::graph::core::data_structures

// used for read meta.yaml when serialized(encode) and deserialize(decode)
namespace YAML {

using sics::graph::core::common::GraphID;
using sics::graph::core::common::VertexID;
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
    node["num_edges"] = metadata.get_num_edges();
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
    if (node.size() != 6) {
      return false;
    }
    metadata.set_num_vertices(node["num_vertices"].as<size_t>());
    metadata.set_num_edges(node["num_edges"].as<size_t>());
    metadata.set_max_vid(node["max_vid"].as<VertexID>());
    metadata.set_min_vid(node["min_vid"].as<VertexID>());
    metadata.set_num_subgraphs(node["num_subgraphs"].as<size_t>());
    auto subgraph_metadata_vec =
        node["subgraphs"]
            .as<std::vector<
                sics::graph::core::data_structures::SubgraphMetadata>>();
    metadata.set_subgraph_metadata_vec(subgraph_metadata_vec);
    return true;
  }
};
}  // namespace YAML

#endif  // GRAPH_SYSTEMS_GRAPH_METADATA_H
