#ifndef GRAPH_SYSTEMS_GRAPH_METADATA_H
#define GRAPH_SYSTEMS_GRAPH_METADATA_H

#include <yaml-cpp/yaml.h>

#include <cstdio>
#include <string>
#include <vector>

#include "common/types.h"
#include "util/logging.h"

namespace sics::graph::core::data_structures {

struct SubgraphMetadata {
  using GraphID = sics::graph::core::common::GraphID;
  using VertexID = sics::graph::core::common::VertexID;
  using EdgeIndex = sics::graph::core::common::EdgeIndex;

  // Subgraph Metadata
  GraphID gid;
  VertexID num_vertices;
  EdgeIndex num_incoming_edges;
  EdgeIndex num_outgoing_edges;
  VertexID max_vid;
  VertexID min_vid;
};

// TODO: change class to struct
class GraphMetadata {
  using GraphID = sics::graph::core::common::GraphID;
  using VertexID = sics::graph::core::common::VertexID;
  using EdgeIndex = sics::graph::core::common::EdgeIndex;

 public:
  GraphMetadata() = default;
  // TO DO: maybe used later
  explicit GraphMetadata(const std::string& root_path);

  void set_num_vertices(VertexID num_vertices) { num_vertices_ = num_vertices; }
  void set_num_edges(size_t num_edges) { num_edges_ = num_edges; }
  void set_max_vid(VertexID max_vid) { max_vid_ = max_vid; }
  void set_min_vid(VertexID min_vid) { min_vid_ = min_vid; }
  void set_count_border_vertices(VertexID count_border_vertices) {
    count_border_vertices_ = count_border_vertices;
  }
  void set_num_subgraphs(GraphID num_subgraphs) {
    num_subgraphs_ = num_subgraphs;
  }
  VertexID get_num_vertices() const { return num_vertices_; }
  EdgeIndex get_num_edges() const { return num_edges_; }
  GraphID get_num_subgraphs() const { return num_subgraphs_; }
  VertexID get_min_vid() const { return min_vid_; }
  VertexID get_max_vid() const { return max_vid_; }
  VertexID get_count_border_vertices() const { return count_border_vertices_; }

  void set_subgraph_metadata_vec(
      const std::vector<SubgraphMetadata>& subgraph_metadata_vec) {
    subgraph_metadata_vec_ = subgraph_metadata_vec;
  }

  SubgraphMetadata GetSubgraphMetadata(common::GraphID gid) const {
    return subgraph_metadata_vec_.at(gid);
  }

  SubgraphMetadata* GetSubgraphMetadataPtr(common::GraphID gid) {
    return &subgraph_metadata_vec_.at(gid);
  }

  common::VertexCount GetSubgraphNumVertices(common::GraphID gid) const {
    return subgraph_metadata_vec_.at(gid).num_vertices;
  }

  void UpdateNumOutgoingEdges(common::GraphID gid, size_t num_outgoing_edges) {
    subgraph_metadata_vec_.at(gid).num_outgoing_edges = num_outgoing_edges;
  }

 private:
  VertexID num_vertices_;
  EdgeIndex num_edges_;
  VertexID max_vid_;
  VertexID min_vid_;
  VertexID count_border_vertices_;
  GraphID num_subgraphs_;
  std::vector<std::vector<int>> dependency_matrix_;
  std::string data_root_path_;
  std::vector<SubgraphMetadata> subgraph_metadata_vec_;
};

}  // namespace sics::graph::core::data_structures

// used for read meta.yaml when serialized(encode) and deserialize(decode)
namespace YAML {

using GraphID = sics::graph::core::common::GraphID;
using VertexID = sics::graph::core::common::VertexID;
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
    node["max_vid"] = metadata.get_max_vid();
    node["min_vid"] = metadata.get_min_vid();
    node["count_border_vertices"] = metadata.get_count_border_vertices();
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
    if (node.size() != 7) return false;

    metadata.set_num_vertices(node["num_vertices"].as<size_t>());
    metadata.set_num_edges(node["num_edges"].as<size_t>());
    metadata.set_max_vid(node["max_vid"].as<VertexID>());
    metadata.set_min_vid(node["min_vid"].as<VertexID>());
    metadata.set_min_vid(node["count_border_vertices"].as<VertexID>());
    metadata.set_count_border_vertices(
        node["count_border_vertices"].as<VertexID>());
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
