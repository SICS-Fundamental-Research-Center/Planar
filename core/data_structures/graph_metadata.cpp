#include "graph_metadata.h"

namespace sics::graph::core::data_structures {

// TODO: initialize GraphMetadata by item not copy construction
GraphMetadata::GraphMetadata(const std::string& graph_metadata_path) {
  YAML::Node metadata_node;
  try {
    metadata_node = YAML::LoadFile(graph_metadata_path);

  } catch (YAML::BadFile& e) {
    LOG_ERROR("meta.yaml file read failed! ", e.msg);
  }
}

}  // namespace sics::graph::core::data_structures