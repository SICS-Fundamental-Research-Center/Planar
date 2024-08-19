#include "graph_metadata.h"

namespace sics::graph::core::data_structures {

GraphMetadata::GraphMetadata(const std::string& graph_metadata_path)
    : data_root_path_(graph_metadata_path) {
  YAML::Node metadata_node;
  try {
    metadata_node = YAML::LoadFile(graph_metadata_path + "meta.yaml");
    *this = metadata_node["GraphMetadata"].as<GraphMetadata>();
  } catch (YAML::BadFile& e) {
    LOG_ERROR("meta.yaml file read failed! ", e.msg);
  }
  vertex_data_size_ = common::Configurations::Get()->vertex_data_size;
  if (this->type_ != "block") {
    InitSubgraphSize();
  } else {
  }
}

}  // namespace sics::graph::core::data_structures