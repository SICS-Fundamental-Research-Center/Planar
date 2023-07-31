#include "graph_metadata.h"

namespace sics::graph::core::data_structures {

GraphMetadata::GraphMetadata(const std::string& root_path) {
  YAML::Node metadata;
  try {
    metadata = YAML::LoadFile(root_path + "/meta.yaml");
  } catch (YAML::BadFile& e) {
    LOG_ERROR("meta.yaml file read failed! ", e.msg);
  }
  //    this->num_vertices_ = metadata["global"].as < struct {
}

}  // namespace sics::graph::core::data_structuresÏ

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
    node["num_vertices"] = metadata.GetNumVertices();
    node["num_edges"] = metadata.GetNumEdges();
    node["num_subgraphs"] = metadata.GetNumSubgraphs();
    node["subgraphs"] =
        std::vector<sics::graph::core::data_structures::SubgraphMetadata>();
    for (int i = 0; i < metadata.GetNumSubgraphs(); i++) {
    }
    return node;
  }

  static bool decode(
      const Node& node,
      sics::graph::core::data_structures::GraphMetadata& metadata) {
    if (node.size() != 4) {
      return false;
    }
    metadata.SetNumVertices(node["num_vertices"].as<size_t>());
    metadata.SetNumEdges(node["num_edges"].as<size_t>());
    metadata.SetNumSubgraphs(node["num_subgraphs"].as<size_t>());
    auto subgraphMetadatas =
        node["subgraphs"]
            .as<std::vector<
                sics::graph::core::data_structures::SubgraphMetadata>>();
    for (int i = 0; i < metadata.GetNumSubgraphs(); i++) {
      metadata.AddSubgraphMetadata(subgraphMetadatas[i]);
    }
    return true;
  }
};
}  // namespace YAML
