#include "miniclean/components/rule_discovery/refactor_rule_miner.h"

#include <yaml-cpp/yaml.h>

#include <fstream>

#include "core/common/multithreading/thread_pool.h"
#include "core/data_structures/buffer.h"
#include "core/data_structures/graph/serialized_immutable_csr_graph.h"
#include "core/util/logging.h"
#include "miniclean/io/miniclean_csr_reader.h"

namespace sics::graph::miniclean::components::rule_discovery::refactor {

using EdgeLabel = sics::graph::miniclean::common::EdgeLabel;
using VertexLabel = sics::graph::miniclean::common::VertexLabel;
using VertexAttributeID = sics::graph::miniclean::common::VertexAttributeID;
using MiniCleanCSRReader = sics::graph::miniclean::io::MiniCleanCSRReader;
using SerializedImmutableCSRGraph =
    sics::graph::core::data_structures::graph::SerializedImmutableCSRGraph;
using ReadMessage = sics::graph::core::scheduler::ReadMessage;
using ThreadPool = sics::graph::core::common::ThreadPool;
using OwnedBuffer = sics::graph::core::data_structures::OwnedBuffer;

void RuleMiner::LoadGraph(const std::string& graph_path) {
  // Prepare reader.
  MiniCleanCSRReader reader(graph_path);

  // Initialize Serialized object.
  std::unique_ptr<SerializedImmutableCSRGraph> serialized_graph =
      std::make_unique<SerializedImmutableCSRGraph>();

  // Initialize ReadMessage object.
  ReadMessage read_message;
  read_message.graph_id = graph_->get_metadata().gid;
  read_message.response_serialized = serialized_graph.get();

  // Read a subgraph.
  reader.Read(&read_message, nullptr);

  // Deserialize the subgraph.
  ThreadPool thread_pool(1);
  graph_->Deserialize(thread_pool, std::move(serialized_graph));
}

void RuleMiner::LoadPathInstances(const std::string& path_instances_path) {
  if (path_patterns_.empty()) {
    LOG_FATAL("Path patterns are not loaded yet.");
  }
  // Initialize path instance vector.
  path_instances_.resize(path_patterns_.size());
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
    size_t pattern_length = path_patterns_[i].size();
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

void RuleMiner::LoadPathRules(const std::string& workspace_dir) {
  // Load path patterns.
  std::string path_patterns_path = workspace_dir + "/path_patterns.txt";
  LoadPathPatterns(path_patterns_path);
  // Load predicates.
  std::string predicates_path = workspace_dir + "/refactor_predicates.yaml";
  LoadPredicates(predicates_path);
}

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

    // Added a dummy edge for the simplicity of path traversal.
    pattern.emplace_back(static_cast<VertexLabel>(label_vec.back()), 0, 0);

    path_patterns_.push_back(pattern);
  }

  pattern_file.close();
}

void RuleMiner::LoadPredicates(const std::string& predicates_path) {
  YAML::Node predicate_nodes;
  try {
    predicate_nodes = YAML::LoadFile(predicates_path);
  } catch (const YAML::BadFile& e) {
    LOG_FATAL("Failed to open predicates file: ", predicates_path.c_str());
  }

  auto constant_predicate_nodes = predicate_nodes["ConstantPredicates"];

  // Reserve space for constant predicates
  constant_predicates_.reserve(constant_predicate_nodes.size());
  for (const auto constant_predicate_node : constant_predicate_nodes) {
    VertexLabel vertex_label = static_cast<VertexLabel>(
        std::stoi(constant_predicate_node.first.as<std::string>()));
    // Check if this label `has existed.
    if (constant_predicates_.find(vertex_label) != constant_predicates_.end()) {
      LOG_WARN("Duplicated vertex label for constant predicate: ",
               vertex_label);
    } else {
      constant_predicates_[vertex_label].reserve(
          constant_predicate_node.second.size());
    }
    for (const auto node : constant_predicate_node.second) {
      VertexAttributeID attribute_id = static_cast<VertexAttributeID>(
          std::stoi(node.first.as<std::string>()));
      // Check if this attribute id has existed.
      if (constant_predicates_[vertex_label].find(attribute_id) !=
          constant_predicates_[vertex_label].end()) {
        LOG_WARN("Duplicated attribute id for constant predicate: ",
                 attribute_id);
      } else {
        constant_predicates_[vertex_label][attribute_id] =
            node.second.as<std::vector<ConstantPredicate>>();
      }
    }
  }
}

void RuleMiner::LoadIndexMetadata(const std::string& index_metadata_path) {
  YAML::Node index_metadata_node;
  try {
    index_metadata_node = YAML::LoadFile(index_metadata_path);
  } catch (const YAML::BadFile& e) {
    LOG_FATAL("Failed to open index metadata file: ",
              index_metadata_path.c_str());
  }

  index_metadata_ = index_metadata_node.as<IndexMetadata>();
}

}  // namespace sics::graph::miniclean::components::rule_discovery::refactor