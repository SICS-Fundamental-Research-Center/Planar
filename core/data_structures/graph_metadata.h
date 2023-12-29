#ifndef GRAPH_SYSTEMS_GRAPH_METADATA_H
#define GRAPH_SYSTEMS_GRAPH_METADATA_H

#include <yaml-cpp/yaml.h>

#include <cstdio>
#include <string>
#include <vector>

#include "common/config.h"
#include "common/types.h"
#include "util/logging.h"

namespace sics::graph::core::data_structures {

struct BlockMetadata {
  using VertexID = common::VertexID;
  using EdgeIndex = common::EdgeIndex;
  using BlockID = common::BlockID;
  // Block Metadata
  BlockID bid;
  VertexID begin_id;  // included
  VertexID end_id;    // excluded
  VertexID num_vertices;
  EdgeIndex num_outgoing_edges;
};

struct SubgraphMetadata {
  using GraphID = common::GraphID;
  using VertexID = common::VertexID;
  using EdgeIndex = common::EdgeIndex;
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
  using GraphID = common::GraphID;
  using BlockID = common::BlockID;
  using VertexID = common::VertexID;
  using VertexDegree = common::VertexDegree;
  using VertexIndex = common::VertexIndex;
  using EdgeIndex = common::EdgeIndex;

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

  size_t GetSubgraphSize(common::GraphID gid) const {
    return subgraph_size_.at(gid);
  }

  void UpdateSubgraphSize(common::GraphID gid) {
    auto& subgraph = subgraph_metadata_vec_.at(gid);
    auto size_vertex_id = (subgraph.num_vertices * sizeof(VertexID)) >> 20;
    auto size_out_degree = (subgraph.num_vertices * sizeof(VertexDegree)) >> 20;
    auto size_out_offset = (subgraph.num_vertices * sizeof(EdgeIndex)) >> 20;
    auto size_out_edges =
        (subgraph.num_outgoing_edges * sizeof(VertexID)) >> 20;
    auto size_vertex_data =
        (subgraph.num_vertices * vertex_data_size_ * 2) >> 20;
    auto size_index = (num_vertices_ * sizeof(VertexIndex)) >> 20;
    auto size_is_in_graph_bitmap = num_vertices_ >> 23;
    auto size_is_src_or_dst_bitmap = (subgraph.num_vertices) >> 23;

    auto size_total = size_vertex_id + size_out_degree + size_out_offset +
                      size_out_edges + size_vertex_data + size_index +
                      size_is_in_graph_bitmap + size_is_src_or_dst_bitmap;

    if (common::Configurations::Get()->edge_mutate &&
        subgraph.num_outgoing_edges != 0) {
      auto size_out_degree_new =
          (subgraph.num_vertices * sizeof(VertexDegree)) >> 20;
      auto size_out_offset_new =
          (subgraph.num_vertices * sizeof(EdgeIndex)) >> 20;
      auto size_out_edges_new =
          (subgraph.num_outgoing_edges * sizeof(VertexID)) >> 20;
      auto size_egdes_dellete_bitmap = subgraph.num_outgoing_edges >> 23;

      size_total += size_out_degree_new + size_out_offset_new +
                    size_out_edges_new + size_egdes_dellete_bitmap;
    }
    if ((size_total + 1) == subgraph_size_.at(gid)) return;
    LOGF_INFO("memory size changed for graph {}, from {}MB to {}MB", gid,
              subgraph_size_.at(gid), size_total + 1);
    subgraph_size_.at(gid) = size_total + 1;
  }

  // type: "subgraph" or "block"
  void set_type(const std::string& type) { type_ = type; }
  const std::string& get_type() const { return type_; }
  void set_block_metadata_vec(
      const std::vector<BlockMetadata>& block_metadata_vec) {
    block_metadata_vec_ = block_metadata_vec;
  }
  const BlockMetadata& GetBlockMetadata(BlockID bid) const {
    return block_metadata_vec_.at(bid);
  }

  BlockMetadata* GetBlockMetadataPtr(BlockID bid) {
    return &block_metadata_vec_.at(bid);
  }

 private:
  void InitSubgraphSize() {
    for (int gid = 0; gid < num_subgraphs_; ++gid) {
      auto& subgraph = subgraph_metadata_vec_.at(gid);
      auto size_vertex_id = (subgraph.num_vertices * sizeof(VertexID)) >> 20;
      auto size_out_degree =
          (subgraph.num_vertices * sizeof(VertexDegree)) >> 20;
      auto size_out_offset = (subgraph.num_vertices * sizeof(EdgeIndex)) >> 20;
      auto size_out_edges =
          (subgraph.num_outgoing_edges * sizeof(VertexID)) >> 20;
      auto size_vertex_data =
          (subgraph.num_vertices * vertex_data_size_ * 2) >> 20;
      auto size_index = (num_vertices_ * sizeof(VertexIndex)) >> 20;
      auto size_is_in_graph_bitmap = num_vertices_ >> 23;
      auto size_is_src_or_dst_bitmap = (subgraph.num_vertices) >> 23;

      auto size_total = size_vertex_id + size_out_degree + size_out_offset +
                        size_out_edges + size_vertex_data + size_index +
                        size_is_in_graph_bitmap + size_is_src_or_dst_bitmap;

      if (common::Configurations::Get()->edge_mutate) {
        auto size_out_degree_new =
            (subgraph.num_vertices * sizeof(VertexDegree)) >> 20;
        auto size_out_offset_new =
            (subgraph.num_vertices * sizeof(EdgeIndex)) >> 20;
        auto size_out_edges_new =
            (subgraph.num_outgoing_edges * sizeof(VertexID)) >> 20;
        auto size_egdes_dellete_bitmap = subgraph.num_outgoing_edges >> 23;
        size_total += size_out_degree_new + size_out_offset_new +
                      size_out_edges_new + size_egdes_dellete_bitmap;
      }
      subgraph_size_.push_back(size_total + 1);
    }
    for (int gid = 0; gid < num_subgraphs_; ++gid) {
      LOGF_INFO("subgraph {} size: {} MB", gid, subgraph_size_.at(gid));
    }
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
  // size of subgraph in MB
  std::vector<size_t> subgraph_size_;

  // type: subgraph or block
  std::string type_ = "subgraph";
  std::vector<BlockMetadata> block_metadata_vec_;
  // configs
  uint32_t vertex_data_size_ = 4;
};

}  // namespace sics::graph::core::data_structures

// used for read meta.yaml when serialized(encode) and deserialize(decode)
namespace YAML {

using GraphID = sics::graph::core::common::GraphID;
using BlockID = sics::graph::core::common::BlockID;
using VertexID = sics::graph::core::common::VertexID;
using EdgeIndex = sics::graph::core::common::EdgeIndex;

// template is needed for this function
template <>
struct convert<sics::graph::core::data_structures::BlockMetadata> {
  static Node encode(
      const sics::graph::core::data_structures::BlockMetadata& block_metadata) {
    Node node;
    node["bid"] = block_metadata.bid;
    node["begin_id"] = block_metadata.begin_id;
    node["end_id"] = block_metadata.end_id;
    node["num_vertices"] = block_metadata.num_vertices;
    node["num_outgoing_edges"] = block_metadata.num_outgoing_edges;
    return node;
  }
  static bool decode(
      const Node& node,
      sics::graph::core::data_structures::BlockMetadata& block_metadata) {
    if (node.size() != 5) {
      return false;
    }
    block_metadata.bid = node["bid"].as<BlockID>();
    block_metadata.begin_id = node["begin_id"].as<VertexID>();
    block_metadata.end_id = node["end_id"].as<VertexID>();
    block_metadata.num_vertices = node["num_vertices"].as<VertexID>();
    block_metadata.num_outgoing_edges =
        node["num_outgoing_edges"].as<EdgeIndex>();
    return true;
  }
};

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
    if (metadata.get_type() == "subgraph") {
      node["num_vertices"] = metadata.get_num_vertices();
      node["num_edges"] = metadata.get_num_edges();
      node["max_vid"] = metadata.get_max_vid();
      node["min_vid"] = metadata.get_min_vid();
      node["count_border_vertices"] = metadata.get_count_border_vertices();
      node["num_subgraphs"] = metadata.get_num_subgraphs();
      std::vector<sics::graph::core::data_structures::SubgraphMetadata> tmp;
      for (size_t i = 0; i < metadata.get_num_subgraphs(); i++) {
        tmp.push_back(metadata.GetSubgraphMetadata(i));
      }
      node["subgraphs"] = tmp;
    } else {
      node["type"] = metadata.get_type();
      node["num_vertices"] = metadata.get_num_vertices();
      node["num_edges"] = metadata.get_num_edges();
      node["max_vid"] = metadata.get_max_vid();
      node["min_vid"] = metadata.get_min_vid();
      node["num_blocks"] = metadata.get_num_subgraphs();
      std::vector<sics::graph::core::data_structures::BlockMetadata> tmp;
      for (size_t i = 0; i < metadata.get_num_subgraphs(); i++) {
        tmp.push_back(metadata.GetBlockMetadata(i));
      }
      node["blocks"] = tmp;
    };
    return node;
  }

  static bool decode(
      const Node& node,
      sics::graph::core::data_structures::GraphMetadata& metadata) {
    auto type = node["type"];
    auto aa = node["22"];
    if (node["type"]) {
      metadata.set_type(node["type"].as<std::string>());
      metadata.set_num_vertices(node["num_vertices"].as<size_t>());
      metadata.set_num_edges(node["num_edges"].as<size_t>());
      metadata.set_max_vid(node["max_vid"].as<VertexID>());
      metadata.set_min_vid(node["min_vid"].as<VertexID>());
      metadata.set_num_subgraphs(node["num_blocks"].as<size_t>());
      auto block_metadata_vec =
          node["blocks"]
              .as<std::vector<
                  sics::graph::core::data_structures::BlockMetadata>>();
      metadata.set_block_metadata_vec(block_metadata_vec);
    } else {
      if (node.size() != 7) return false;
      metadata.set_num_vertices(node["num_vertices"].as<size_t>());
      metadata.set_num_edges(node["num_edges"].as<size_t>());
      metadata.set_max_vid(node["max_vid"].as<VertexID>());
      metadata.set_min_vid(node["min_vid"].as<VertexID>());
      metadata.set_count_border_vertices(
          node["count_border_vertices"].as<VertexID>());
      metadata.set_count_border_vertices(
          node["count_border_vertices"].as<VertexID>());
      metadata.set_num_subgraphs(node["num_subgraphs"].as<size_t>());
      auto subgraph_metadata_vec =
          node["subgraphs"]
              .as<std::vector<
                  sics::graph::core::data_structures::SubgraphMetadata>>();
      metadata.set_subgraph_metadata_vec(subgraph_metadata_vec);
    }
    return true;
  }
};
}  // namespace YAML

#endif  // GRAPH_SYSTEMS_GRAPH_METADATA_H
