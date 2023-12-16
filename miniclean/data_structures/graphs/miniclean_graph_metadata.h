#ifndef MINICLEAN_DATA_STRUCTURES_GRAPHS_MINICLEAN_GRAPH_METADATA_H_
#define MINICLEAN_DATA_STRUCTURES_GRAPHS_MINICLEAN_GRAPH_METADATA_H_

#include <yaml-cpp/yaml.h>

#include <map>
#include <string>
#include <vector>

#include "core/util/logging.h"
#include "miniclean/common/types.h"

namespace sics::graph::miniclean::data_structures::graphs {

typedef enum : uint8_t {
  kUInt8 = 0,
  kUInt16,
  kUInt32,
  kUInt64,
  kString
} VertexAttributeType;

struct MiniCleanSubgraphMetadata {
 private:
  using GraphID = miniclean::common::GraphID;
  using VertexID = miniclean::common::VertexID;
  using EdgeIndex = miniclean::common::EdgeIndex;

 public:
  GraphID gid;
  VertexID num_vertices;
  EdgeIndex num_incoming_edges;
  EdgeIndex num_outgoing_edges;
  VertexID max_vidg;
  VertexID min_vidg;
  std::vector<std::pair<VertexID, VertexID>> vlabel_id_to_vidl_range;
  std::vector<std::string> vattr_id_to_file_path;
  std::vector<VertexAttributeType> vattr_id_to_vattr_type;
  std::vector<uint16_t> vattr_id_to_max_string_length;
};

struct MiniCleanGraphMetadata {
 private:
  using GraphID = miniclean::common::GraphID;
  using VertexID = miniclean::common::VertexID;
  using EdgeIndex = miniclean::common::EdgeIndex;

 public:
  VertexID num_vertices;
  EdgeIndex num_edges;
  VertexID max_vidg;
  VertexID min_vidg;
  VertexID num_border_vertices;
  GraphID num_subgraphs;
  std::vector<std::pair<VertexID, VertexID>> vlabel_id_to_vidl_range;
  std::vector<MiniCleanSubgraphMetadata> subgraphs;
};
}  // namespace sics::graph::miniclean::data_structures::graphs

namespace YAML {

template <>
struct convert<sics::graph::miniclean::data_structures::graphs::
                   MiniCleanSubgraphMetadata> {
  static Node encode(const sics::graph::miniclean::data_structures::graphs::
                         MiniCleanSubgraphMetadata& subgraph_metadata) {
    Node node;
    node["gid"] = (uint32_t)subgraph_metadata.gid;
    node["num_vertices"] = subgraph_metadata.num_vertices;
    node["num_incoming_edges"] = subgraph_metadata.num_incoming_edges;
    node["num_outgoing_edges"] = subgraph_metadata.num_outgoing_edges;
    node["max_vid"] = subgraph_metadata.max_vidg;
    node["min_vid"] = subgraph_metadata.min_vidg;
    for (const auto& pair : subgraph_metadata.vlabel_id_to_vidl_range) {
      node["vlabel_id_to_vidl_range"].push_back(pair);
    }
    for (const auto& path : subgraph_metadata.vattr_id_to_file_path) {
      node["vattr_id_to_file_path"].push_back(path);
    }
    for (const auto& max_string_length :
         subgraph_metadata.vattr_id_to_max_string_length) {
      node["vattr_id_to_max_string_length"].push_back(max_string_length);
    }
    for (const auto& type : subgraph_metadata.vattr_id_to_vattr_type) {
      switch (type) {
        case sics::graph::miniclean::data_structures::graphs::kUInt8:
          node["vattr_id_to_vattr_type"].push_back("uint8_t");
          break;
        case sics::graph::miniclean::data_structures::graphs::kUInt16:
          node["vattr_id_to_vattr_type"].push_back("uint16_t");
          break;
        case sics::graph::miniclean::data_structures::graphs::kUInt32:
          node["vattr_id_to_vattr_type"].push_back("uint32_t");
          break;
        case sics::graph::miniclean::data_structures::graphs::kUInt64:
          node["vattr_id_to_vattr_type"].push_back("uint64_t");
          break;
        case sics::graph::miniclean::data_structures::graphs::kString:
          node["vattr_id_to_vattr_type"].push_back("string");
          break;
        default:
          LOG_FATAL("Unknown vertex attribute type.");
      }
    }
    return node;
  }
  static bool decode(const Node& node,
                     sics::graph::miniclean::data_structures::graphs::
                         MiniCleanSubgraphMetadata& subgraph_metadata) {
    if (node.size() != 10) return false;
    subgraph_metadata.gid =
        node["gid"].as<sics::graph::miniclean::common::GraphID>();
    subgraph_metadata.num_vertices =
        node["num_vertices"].as<sics::graph::miniclean::common::VertexID>();
    subgraph_metadata.num_incoming_edges =
        node["num_incoming_edges"]
            .as<sics::graph::miniclean::common::EdgeIndex>();
    subgraph_metadata.num_outgoing_edges =
        node["num_outgoing_edges"]
            .as<sics::graph::miniclean::common::EdgeIndex>();
    subgraph_metadata.max_vidg =
        node["max_vid"].as<sics::graph::miniclean::common::VertexID>();
    subgraph_metadata.min_vidg =
        node["min_vid"].as<sics::graph::miniclean::common::VertexID>();
    subgraph_metadata.vlabel_id_to_vidl_range.reserve(
        node["vlabel_id_to_vidl_range"].size());
    for (const auto& pair : node["vlabel_id_to_vidl_range"]) {
      subgraph_metadata.vlabel_id_to_vidl_range.push_back(
          pair.as<std::pair<sics::graph::miniclean::common::VertexID,
                            sics::graph::miniclean::common::VertexID>>());
    }
    subgraph_metadata.vattr_id_to_file_path.reserve(
        node["vattr_id_to_file_path"].size());
    for (const auto& path : node["vattr_id_to_file_path"]) {
      subgraph_metadata.vattr_id_to_file_path.push_back(path.as<std::string>());
    }
    for (const auto& max_string_length : node["vattr_id_to_max_string_length"]) {
      subgraph_metadata.vattr_id_to_max_string_length.push_back(
          max_string_length.as<uint16_t>());
    }
    subgraph_metadata.vattr_id_to_vattr_type.reserve(
        node["vattr_id_to_vattr_type"].size());
    std::map<
        std::string,
        sics::graph::miniclean::data_structures::graphs::VertexAttributeType>
        str2type = {{"uint8_t",
                     sics::graph::miniclean::data_structures::graphs::kUInt8},
                    {"uint16_t",
                     sics::graph::miniclean::data_structures::graphs::kUInt16},
                    {"uint32_t",
                     sics::graph::miniclean::data_structures::graphs::kUInt32},
                    {"uint64_t",
                     sics::graph::miniclean::data_structures::graphs::kUInt64},
                    {"string",
                     sics::graph::miniclean::data_structures::graphs::kString}};
    for (const auto& type : node["vattr_id_to_vattr_type"]) {
      subgraph_metadata.vattr_id_to_vattr_type.push_back(
          str2type[type.as<std::string>()]);
    }
    return true;
  }
};

template <>
struct convert<
    sics::graph::miniclean::data_structures::graphs::MiniCleanGraphMetadata> {
  static Node encode(const sics::graph::miniclean::data_structures::graphs::
                         MiniCleanGraphMetadata& graph_metadata) {
    Node node;
    Node metadata;
    node["num_vertices"] = graph_metadata.num_vertices;
    node["num_edges"] = graph_metadata.num_edges;
    node["max_vidg"] = graph_metadata.max_vidg;
    node["min_vidg"] = graph_metadata.min_vidg;
    node["num_border_vertices"] = graph_metadata.num_border_vertices;
    node["num_subgraphs"] = (uint32_t)graph_metadata.num_subgraphs;
    for (const auto& pair : graph_metadata.vlabel_id_to_vidl_range) {
      node["vlabel_id_to_vidl_range"].push_back(pair);
    }
    for (const auto& subgraph_metadata : graph_metadata.subgraphs) {
      node["subgraphs"].push_back(subgraph_metadata);
    }
    metadata["GraphMetadata"] = node;
    return metadata;
  }
  static bool decode(
      const Node& node,
      sics::graph::miniclean::data_structures::graphs::MiniCleanGraphMetadata&
          graph_metadata) {
    Node metadata = node["GraphMetadata"];
    if (metadata.size() != 8) return false;
    graph_metadata.num_vertices =
        metadata["num_vertices"].as<sics::graph::miniclean::common::VertexID>();
    graph_metadata.num_edges =
        metadata["num_edges"].as<sics::graph::miniclean::common::EdgeIndex>();
    graph_metadata.max_vidg =
        metadata["max_vidg"].as<sics::graph::miniclean::common::VertexID>();
    graph_metadata.min_vidg =
        metadata["min_vidg"].as<sics::graph::miniclean::common::VertexID>();
    graph_metadata.num_border_vertices =
        metadata["num_border_vertices"]
            .as<sics::graph::miniclean::common::VertexID>();
    graph_metadata.num_subgraphs =
        metadata["num_subgraphs"].as<sics::graph::miniclean::common::GraphID>();
    graph_metadata.vlabel_id_to_vidl_range.reserve(
        metadata["vlabel_id_to_vidl_range"].size());
    for (const auto& pair : metadata["vlabel_id_to_vidl_range"]) {
      graph_metadata.vlabel_id_to_vidl_range.push_back(
          pair.as<std::pair<sics::graph::miniclean::common::VertexID,
                            sics::graph::miniclean::common::VertexID>>());
    }
    graph_metadata.subgraphs.reserve(metadata["subgraphs"].size());
    for (const auto& subgraph_metadata : metadata["subgraphs"]) {
      graph_metadata.subgraphs.push_back(
          subgraph_metadata.as<sics::graph::miniclean::data_structures::graphs::
                                   MiniCleanSubgraphMetadata>());
    }
    return true;
  }
};
}  // namespace YAML

#endif  // MINICLEAN_DATA_STRUCTURES_GRAPHS_MINICLEAN_GRAPH_METADATA_H_