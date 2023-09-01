#include "miniclean/components/rule_discovery/rule_miner.h"

#include <yaml-cpp/yaml.h>

#include <fstream>
#include <memory>

#include "core/common/multithreading/thread_pool.h"
#include "core/data_structures/buffer.h"
#include "core/data_structures/graph/serialized_immutable_csr_graph.h"
#include "core/scheduler/message.h"
#include "core/util/logging.h"
#include "miniclean/io/miniclean_csr_reader.h"

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
using MiniCleanCSRReader = sics::graph::miniclean::io::MiniCleanCSRReader;
using SerializedImmutableCSRGraph =
    sics::graph::core::data_structures::graph::SerializedImmutableCSRGraph;
using ReadMessage = sics::graph::core::scheduler::ReadMessage;
using ThreadPool = sics::graph::core::common::ThreadPool;
using DualPattern = sics::graph::miniclean::common::DualPattern;
using StarPattern = sics::graph::miniclean::common::StarPattern;

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
    // Check if `lhs_label` is already in `variable_predicates_` and reserve
    // space for `variable_predicates_[lhs_label]`.
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

void RuleMiner::InitGCRs() {
  // Each pair of path patterns will generate a group of GCR.
  predicate_pool_.resize(
      path_patterns_.size() * (path_patterns_.size() - 1) / 2);

  size_t current_pair_id = 0;
  for (size_t i = 0; i < path_patterns_.size() - 1; i++) {
    for (size_t j = i + 1; j < path_patterns_.size(); j++) {
      StarPattern left_star, right_star;
      left_star.emplace_back(i);
      right_star.emplace_back(j);
      DualPattern dual_pattern = std::make_tuple(left_star, right_star);

      // Initialize predicate pool.
      size_t predicate_pool_size = 0;
      for (size_t k = 0; k < path_patterns_[i].size(); k++) {
        predicate_pool_size +=
            constant_predicates_[std::get<0>(path_patterns_[i][k])].size();
      }
      for (size_t k = 0; k < path_patterns_[j].size(); k++) {
        predicate_pool_size +=
            constant_predicates_[std::get<0>(path_patterns_[j][k])].size();
      }
      for (size_t k = 0; k < path_patterns_[i].size(); k++) {
        for (size_t l = 0; l < path_patterns_[j].size(); l++) {
          predicate_pool_size +=
              variable_predicates_[std::get<0>(path_patterns_[i][k])]
                                  [std::get<0>(path_patterns_[j][l])]
                                      .size();
        }
      }

      predicate_pool_[current_pair_id].reserve(predicate_pool_size);

      // Constant predicate enumeration for left path.
      for (size_t k = 0; k < path_patterns_[i].size(); k++) {
        for (auto constant_predicate :
             constant_predicates_[std::get<0>(path_patterns_[i][k])]) {
          ConstantPredicate* new_constant_predicate =
              new ConstantPredicate(constant_predicate);
          new_constant_predicate->set_lhs_ppid(i);
          new_constant_predicate->set_lhs_edge_id(k);
          predicate_pool_[current_pair_id].push_back(new_constant_predicate);
        }
      }

      // Constant predicate enumeration for right path.
      for (size_t k = 0; k < path_patterns_[j].size(); k++) {
        for (auto constant_predicate :
             constant_predicates_[std::get<0>(path_patterns_[j][k])]) {
          ConstantPredicate* new_constant_predicate =
              new ConstantPredicate(constant_predicate);
          new_constant_predicate->set_lhs_ppid(j);
          new_constant_predicate->set_lhs_edge_id(k);
          predicate_pool_[current_pair_id].push_back(new_constant_predicate);
        }
      }

      // Variable predicate enumeration for <lvertex, rvertex> pair.
      for (size_t k = 0; k < path_patterns_[i].size(); k++) {
        for (size_t l = 0; l < path_patterns_[j].size(); l++) {
          for (auto variable_predicate : variable_predicates_[std::get<0>(
                   path_patterns_[i][k])][std::get<0>(path_patterns_[j][l])]) {
            VariablePredicate* new_variable_predicate =
                new VariablePredicate(variable_predicate);
            new_variable_predicate->set_lhs_ppid(i);
            new_variable_predicate->set_lhs_edge_id(k);
            new_variable_predicate->set_rhs_ppid(j);
            new_variable_predicate->set_rhs_edge_id(l);
            predicate_pool_[current_pair_id].push_back(new_variable_predicate);
          }
        }
      }

      // Initialize GCRs.
      GCR gcr = GCR(dual_pattern);
      for (size_t k = 0; k <= predicate_restriction; k++) {
        gcr.set_consequence(predicate_pool_[current_pair_id][k]);
        // TODO (bai-wenchao): Check support of this GCR.
        gcrs_.push_back(gcr);
        InitGCRsRecur(gcr, 1, {}, k, predicate_pool_[current_pair_id]);
      }
      current_pair_id++;
    }
  }
}

void RuleMiner::InitGCRsRecur(GCR gcr, size_t depth,
                              std::vector<size_t> precondition_ids,
                              size_t consequence_id,
                              std::vector<GCRPredicate*>& predicate_pool) {
  if (depth == predicate_restriction) {
    return;
  }

  for (size_t i = 0; i < predicate_pool.size(); i++) {
    // Duplication check.
    if (std::find(precondition_ids.begin(), precondition_ids.end(), i) !=
            precondition_ids.end() ||
        i == consequence_id) {
      continue;
    }

    gcr.set_precondition(predicate_pool[i]);
    // TODO (bai-wenchao): Check support of this GCR.
    gcrs_.push_back(gcr);
    precondition_ids.push_back(i);
    InitGCRsRecur(gcr, depth + 1, precondition_ids, consequence_id,
                  predicate_pool);
    precondition_ids.pop_back();
    gcr.pop_precondition();
  }
}
}  // namespace sics::graph::miniclean::components::rule_discovery