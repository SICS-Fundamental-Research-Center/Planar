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



}  // namespace sics::graph::core::data_structures