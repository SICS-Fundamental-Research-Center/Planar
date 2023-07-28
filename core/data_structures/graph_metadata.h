//
// Created by Yang Liu on 2023/7/27.
//

#ifndef GRAPH_SYSTEMS_GRAPH_METADATA_H
#define GRAPH_SYSTEMS_GRAPH_METADATA_H

#include "common/types.h"
#include "util/logging.h"
#include "yaml-cpp/yaml.h"
#include <cstdio>
#include <string>
#include <vector>

namespace sics::graph::core::data_structures {

class SubgraphMetadata {
 public:
  common::GraphIDType getGid() const { return gid; }
  void setGid(common::GraphIDType gid) { SubgraphMetadata::gid = gid; }
  size_t getNumVertices() const { return num_vertices; }
  void setNumVertices(size_t numVertices) { num_vertices = numVertices; }
  size_t getNumEdges() const { return num_edges; }
  void setNumEdges(size_t numEdges) { num_edges = numEdges; }
  size_t getSize() const { return size; }
  void setSize(size_t size) { SubgraphMetadata::size = size; }

 private:
  common::GraphIDType gid;
  size_t num_vertices;
  size_t num_edges;
  size_t size;  // need this??
};

class GraphMetadata {
 private:
  size_t num_vertices;
  size_t num_edges;
  size_t num_subgraphs;
  std::vector<std::vector<int>> dependency_matrix;
  std::string data_root_path;
  std::vector<bool> current_round_pending;
  std::vector<bool> next_round_pending;
  std::vector<SubgraphMetadata> subgraph_metadata;

 public:
  GraphMetadata() {}
  GraphMetadata(const std::string& root_path) {
    YAML::Node metadata;
    try {
      metadata = YAML::LoadFile(root_path + "/meta.yaml");
    } catch (YAML::BadFile& e) {
      LOG_ERROR("meta.yaml file read failed! ", e.msg);
    }
    //    this->num_vertices = metadata["global"].as < struct {
  }

 public:
  void setNumVertices(size_t numVertices) { num_vertices = numVertices; }
  void setNumEdges(size_t numEdges) { num_edges = numEdges; }
  void setNumSubgraphs(size_t numSubgraphs) { num_subgraphs = numSubgraphs; }
  size_t getNumVertices() const { return num_vertices; }
  size_t getNumEdges() const { return num_edges; }
  size_t getNumSubgraphs() const { return num_subgraphs; }

  void AddSubgraphMetadata(SubgraphMetadata& subgraphMetadata) {
    subgraph_metadata.emplace_back(subgraphMetadata);
  }

  SubgraphMetadata& GetSubgraphMetadata(common::GraphIDType gid) {
    return subgraph_metadata.at(gid);
  }

  int GetSubgraphRound(common::GraphIDType gid);
};

}  // namespace sics::graph::core::data_structures

// used for read meta.yaml when serialized(encode) and deserialize(decode)
namespace YAML {

template <>
struct convert<sics::graph::core::data_structures::SubgraphMetadata> {
  static Node encode(const sics::graph::core::data_structures::SubgraphMetadata&
                         subgraph_metadata) {
    Node node;
    return node;
  }
  static bool decode(
      const Node& node,
      sics::graph::core::data_structures::SubgraphMetadata& subgraph_metadata) {
    if (node.size() != 4) {
      return false;
    }
    subgraph_metadata.setGid(
        node["gid"].as<sics::graph::core::common::GraphIDType>());
    subgraph_metadata.setNumVertices(node["num_vertices"].as<size_t>());
    subgraph_metadata.setNumEdges(node["num_edges"].as<size_t>());
    subgraph_metadata.setSize(node["size"].as<size_t>());
    return true;
  }
};

template <>
struct convert<sics::graph::core::data_structures::GraphMetadata> {
  static Node encode(
      const sics::graph::core::data_structures::GraphMetadata& metadata) {
    Node node;
    node.push_back(metadata.getNumVertices());
    node.push_back(metadata.getNumEdges());
    node.push_back((metadata.getNumSubgraphs()));
    return node;
  }

  static bool decode(
      const Node& node,
      sics::graph::core::data_structures::GraphMetadata& metadata) {
    if (node.size() != 4) {
      return false;
    }
    metadata.setNumVertices(node["num_vertices"].as<size_t>());
    metadata.setNumEdges(node["num_edges"].as<size_t>());
    metadata.setNumSubgraphs(node["num_subgraphs"].as<size_t>());
    auto subgraphMetadatas =
        node["subgraphs"]
            .as<std::vector<
                sics::graph::core::data_structures::SubgraphMetadata>>();
    for (int i = 0; i < metadata.getNumSubgraphs(); i++) {
      metadata.AddSubgraphMetadata(subgraphMetadatas[i]);
    }
    return true;
  }
};
}  // namespace YAML

#endif  // GRAPH_SYSTEMS_GRAPH_METADATA_H
