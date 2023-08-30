#include "miniclean/components/rule_discovery/rule_miner.h"

#include <yaml-cpp/yaml.h>

#include <fstream>

#include "core/data_structures/buffer.h"
#include "core/util/logging.h"

namespace sics::graph::miniclean::components::rule_discovery {

using PathPattern = sics::graph::miniclean::common::PathPattern;
using VertexLabel = sics::graph::miniclean::common::VertexLabel;
using VertexID = sics::graph::miniclean::common::VertexID;
using EdgeLabel = sics::graph::miniclean::common::EdgeLabel;
using OwnedBuffer = sics::graph::core::data_structures::OwnedBuffer;
using VariablePredicate =
    sics::graph::miniclean::data_structures::gcr::VariablePredicate;
using ConstantPredicate =
    sics::graph::miniclean::data_structures::gcr::ConstantPredicate;

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

void RuleMiner::LoadPredicates(const std::string& predicates_path) {
  YAML::Node predicate_nodes;
  try {
    predicate_nodes = YAML::LoadFile(predicates_path);
  } catch (const YAML::BadFile& e) {
    LOG_FATAL("Failed to open predicates file: ", predicates_path.c_str());
  }

  auto variable_predicate_nodes = predicate_nodes["VariablePredicates"];
  auto constant_predicate_nodes = predicate_nodes["ConstantPredicates"];
  
  // Reserve space for `variable_predicates_`.
  variable_predicates_.reserve(variable_predicate_nodes.size());
  for (auto variable_predicate_node : variable_predicate_nodes) {
    VertexLabel lhs_label = static_cast<uint8_t>(
        std::stoi(variable_predicate_node.first.as<std::string>()));
    // Check if `lhs_label` is already in `variable_predicates_` and reserve space for
    // `variable_predicates_[lhs_label]`.
    if (variable_predicates_.find(lhs_label) != variable_predicates_.end()) {
      LOG_WARN("Duplicate variable predicate lhs vertex label: ", lhs_label);
    } else {
      variable_predicates_[lhs_label].reserve(
          variable_predicate_node.second.size());
    }
    for (auto node : variable_predicate_node.second) {
      VertexLabel rhs_label =
          static_cast<uint8_t>(std::stoi(node.first.as<std::string>()));
      // Check if `rhs_label` is already in `variable_predicates_[lhs_label]`.
      if (variable_predicates_[lhs_label].find(rhs_label) !=
          variable_predicates_[lhs_label].end()) {
        LOG_WARN("Duplicate variable predicate rhs vertex label: ", rhs_label);
      }
      // `variable_predicates_[lhs_label][rhs_label]` is a vector of predicates
      // with lhs vertex label `lhs_label` and rhs vertex label `rhs_label`.
      variable_predicates_[lhs_label][rhs_label] =
          node.second.as<std::vector<VariablePredicate>>();
    }
  }

  // Reserve space for `constant_predicates_`.
  constant_predicates_.reserve(constant_predicate_nodes.size());
  for (auto constant_predicate_node : constant_predicate_nodes) {
    VertexLabel lhs_label = static_cast<uint8_t>(
        std::stoi(constant_predicate_node.first.as<std::string>()));
    // Check if `lhs_label` is already in `constant_predicates_`.
    if (constant_predicates_.find(lhs_label) != constant_predicates_.end()) {
      LOG_WARN("Duplicate constant predicate lhs vertex label: ", lhs_label);
    }
    // `constant_predicates_[lhs_label]` is a vector of predicates with lhs
    // vertex label `lhs_label`.
    constant_predicates_[lhs_label] =
        constant_predicate_node.second.as<std::vector<ConstantPredicate>>();
  }
}

}  // namespace sics::graph::miniclean::components::rule_discovery