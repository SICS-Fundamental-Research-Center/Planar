#include "miniclean/components/rule_discovery/rule_discovery.h"

#include <fstream>

#include "core/data_structures/buffer.h"

namespace sics::graph::miniclean::components::rule_discovery {

using PathPattern = sics::graph::miniclean::common::PathPattern;
using VertexLabel = sics::graph::miniclean::common::VertexLabel;
using VertexID = sics::graph::miniclean::common::VertexID;
using EdgeLabel = sics::graph::miniclean::common::EdgeLabel;
using OwnedBuffer = sics::graph::core::data_structures::OwnedBuffer;

void RuleMiner::LoadPathPatterns(const std::string& path_patterns_path) {
  std::ifstream pattern_file(path_patterns_path);
  if (!pattern_file.is_open()) {
    LOG_FATAL("Failed to open path pattern file: ", path_patterns_path.c_str());
  }

  std::string line;
  while (std::getline(pattern_file, line)) {
    // if the line is empty or the first char is `#`, continue.
    if (line.empty() || line[0] == '#') continue;

    PathPattern pattern;
    std::stringstream ss(line);
    std::string label;

    std::vector<uint8_t> label_vec;
    while (std::getline(ss, label, ',')) {
      uint8_t label_id = static_cast<uint8_t>(std::stoi(label));
      label_vec.push_back(label_id);
    }

    for (size_t i = 0; i < label_vec.size() - 1; i += 2) {
      VertexLabel src_label = static_cast<VertexLabel>(label_vec[i]);
      EdgeLabel edge_label = static_cast<EdgeLabel>(label_vec[i + 1]);
      VertexLabel dst_label = static_cast<VertexLabel>(label_vec[i + 2]);

      pattern.emplace_back(src_label, edge_label, dst_label);
    }

    path_patterns_.push_back(pattern);
  }

  pattern_file.close();

  // Initialize path_instance vector.
  path_instances_.resize(path_patterns_.size());
}

void RuleMiner::LoadPathInstances(const std::string& path_instances_path) {
  // Load instances of all patterns.
  for (size_t i = 0; i < path_patterns_.size(); i++) {
    // Open the file.
    std::ifstream instance_file(
        path_instances_path + "/" + std::to_string(i) + ".bin",
        std::ios::binary);
    if (!instance_file) {
      LOG_FATAL(
          "Failed to open path instance file: ",
          (path_instances_path + "/" + std::to_string(i) + ".bin").c_str());
    }

    // Get the file size.
    instance_file.seekg(0, std::ios::end);
    size_t fileSize = instance_file.tellg();
    instance_file.seekg(0, std::ios::beg);

    // Create a buffer.
    OwnedBuffer buffer(fileSize);

    // Read the instances.
    instance_file.read(reinterpret_cast<char*>(buffer.Get()), fileSize);
    if (!instance_file) {
      LOG_FATAL(
          "Failed to read path instance file: ",
          (path_instances_path + "/" + std::to_string(i) + ".bin").c_str());
    }

    // Parse the buffer.
    size_t pattern_length = path_patterns_[i].size() + 1;
    size_t num_instances = fileSize / (pattern_length * sizeof(VertexID));
    VertexID* instance_buffer = reinterpret_cast<VertexID*>(buffer.Get());
    path_instances_[i].reserve(num_instances);

    for (size_t j = 0; j < num_instances; j++) {
      std::vector<VertexID> instance(pattern_length);
      for (size_t k = 0; k < pattern_length; k++) {
        instance[k] = instance_buffer[j * pattern_length + k];
      }

      path_instances_[i].push_back(instance);
    }

    // Close the file.
    instance_file.close();
  }
}

}  // namespace sics::graph::miniclean::components::rule_discovery