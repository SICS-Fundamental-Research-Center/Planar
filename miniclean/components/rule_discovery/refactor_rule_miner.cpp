#include "miniclean/components/rule_discovery/refactor_rule_miner.h"

#include <yaml-cpp/yaml.h>

#include <fstream>

#include "core/util/logging.h"

namespace sics::graph::miniclean::components::rule_discovery::refactor {

using EdgeLabel = sics::graph::miniclean::common::EdgeLabel;
using VertexLabel = sics::graph::miniclean::common::VertexLabel;
using VertexAttributeID = sics::graph::miniclean::common::VertexAttributeID;

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
  for (auto constant_predicate_node : constant_predicate_nodes) {
    VertexLabel vertex_label = static_cast<VertexLabel>(
        std::stoi(constant_predicate_node.first.as<std::string>()));
    // Check if this label has existed.
    if (constant_predicates_.find(vertex_label) != constant_predicates_.end()) {
      LOG_FATAL("Duplicated vertex label for constant predicate: ",
                vertex_label);
    } else {
      constant_predicates_[vertex_label] =
          constant_predicate_node.second.as<std::vector<ConstantPredicate>>();
    }
  }
}

}  // namespace sics::graph::miniclean::components::rule_discovery::refactor