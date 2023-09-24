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
  for (const auto& constant_predicate_node : constant_predicate_nodes) {
    VertexLabel vertex_label = static_cast<VertexLabel>(
        std::stoi(constant_predicate_node.first.as<std::string>()));
    // Check if this label has existed.
    if (constant_predicates_.find(vertex_label) != constant_predicates_.end()) {
      LOG_WARN("Duplicated vertex label for constant predicate: ",
               vertex_label);
    } else {
      constant_predicates_[vertex_label].reserve(
          constant_predicate_node.second.size());
    }
    for (const auto& node : constant_predicate_node.second) {
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

  YAML::Node variable_predicate_nodes = predicate_nodes["VariablePredicates"];
  variable_predicates_ =
      variable_predicate_nodes.as<std::vector<VariablePredicate>>();

  YAML::Node consequence_predicate_nodes = predicate_nodes["Consequence"];
  consequence_predicates_ =
      consequence_predicate_nodes.as<std::vector<VariablePredicate>>();
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

void RuleMiner::InitPathRuleUnits() {
  size_t num_vertex = graph_->get_num_vertices();
  const auto& attribute_metadata = index_metadata_.get_attribute_metadata();
  // Initialize path rule unit container.
  //   1. Initialize the container.
  path_rule_unit_container_.resize(path_patterns_.size());
  for (size_t i = 0; i < path_rule_unit_container_.size(); i++) {
    // The extra one is for the case that the path rule has no constant
    // predicate.
    path_rule_unit_container_[i].resize(path_patterns_[i].size() + 1);
    path_rule_unit_container_[i][0].resize(1);
    path_rule_unit_container_[i][0][0].resize(1);
    path_rule_unit_container_[i][0][0][0].resize(1);
    for (size_t j = 1; j < path_rule_unit_container_[i].size(); j++) {
      VertexLabel label = std::get<0>(path_patterns_[i][j - 1]);
      if (attribute_metadata.find(label) != attribute_metadata.end()) {
        path_rule_unit_container_[i][j].resize(
            attribute_metadata.at(label).size());
        for (size_t k = 0; k < path_rule_unit_container_[i][j].size(); k++) {
          // TODO (bai-wenchao): this is a dangurous implementation, we need to
          // make sure whether attribute_metadata[label] range from 0 to n.
          path_rule_unit_container_[i][j][k].resize(
              attribute_metadata.at(label)[k].second);
          for (size_t l = 0; l < path_rule_unit_container_[i][j][k].size();
               l++) {
            // TODO (bai-wenchao): `2` is the number of operator types.
            path_rule_unit_container_[i][j][k][l].resize(2);
          }
        }
      }
    }
  }
  //   2. Initialize the rule unit.
  path_rules_.resize(path_patterns_.size());
  for (size_t i = 0; i < path_rule_unit_container_.size(); i++) {
    PathRule path_rule(path_patterns_[i], num_vertex);
    path_rule.InitBitmap(path_instances_[i], graph_);
    // TODO (bai-wenchao): check the support.
    if (path_rule.CountOneBits() <= 0) {
      continue;
    }
    LOG_INFO("Pattern ID: ", i, " count: ", path_rule.CountOneBits());
    path_rule_unit_container_[i][0][0][0][0] = path_rule;
    path_rules_[i].push_back(path_rule);
    for (size_t j = 1; j < path_rule_unit_container_[i].size(); j++) {
      VertexLabel label = std::get<0>(path_patterns_[i][j - 1]);
      for (const auto& predicate_pair : constant_predicates_[label]) {
        VertexAttributeID attribute_id = predicate_pair.first;
        for (const auto& predicate : predicate_pair.second) {
          path_rule.AddConstantPredicate(j - 1, predicate);
          path_rule.InitBitmap(path_instances_[i], graph_);
          // TODO (bai-wenchao): check support.
          if (path_rule.CountOneBits() <= 0) {
            bool pop_status = path_rule.PopConstantPredicate();
            if (!pop_status) {
              LOG_FATAL("Failed to pop constant predicate.");
            }
            continue;
          }
          LOG_INFO("Pattern ID: ", i, " count: ", path_rule.CountOneBits(),
                   " index: ", j, "");
          for (const auto& carried_predicate :
               path_rule.get_constant_predicates()) {
            LOG_INFO("Predicate: ", carried_predicate.first, " ",
                     carried_predicate.second.get_vertex_label(), " ",
                     carried_predicate.second.get_vertex_attribute_id(), " ",
                     carried_predicate.second.get_operator_type(), " ",
                     carried_predicate.second.get_constant_value());
          }
          path_rule_unit_container_[i][j][attribute_id]
                                   [predicate.get_constant_value()]
                                   [predicate.get_operator_type()] = path_rule;
          path_rules_[i].push_back(path_rule);
          bool pop_status = path_rule.PopConstantPredicate();
          if (!pop_status) {
            LOG_FATAL("Failed to pop constant predicate.");
          }
        }
      }
    }
    ExtendPathRules(i);
  }
}

void RuleMiner::ExtendPathRules(size_t pattern_id) {
  size_t num_unit = path_rules_[pattern_id].size();
  size_t begin_from = 0;
  size_t offset = num_unit;
  bool should_continue = true;
  while (should_continue) {
    should_continue = false;
    size_t inner_offset = 0;
    for (size_t j = 0; j < num_unit; j++) {
      for (size_t k = begin_from; k < offset; k++) {
        PathRule path_rule_cp(path_rules_[pattern_id][k]);
        bool should_compose = path_rule_cp.ComposeWith(
            path_rules_[pattern_id][j], max_predicate_num_);
        should_continue = should_continue || should_compose;
        LOG_INFO("Compose status: ", should_continue);
        if (should_compose) {
          // TODO (bai-wenchao): check support.
          if (path_rule_cp.CountOneBits() <= 0) {
            continue;
          }
          LOG_INFO("[compose stage] Pattern ID: ", pattern_id,
                   " count: ", path_rule_cp.CountOneBits(), " j: ", j,
                   " k: ", k);
          for (const auto& carried_predicate :
               path_rule_cp.get_constant_predicates()) {
            LOG_INFO("Predicate: ", carried_predicate.first, " ",
                     carried_predicate.second.get_vertex_label(), " ",
                     carried_predicate.second.get_vertex_attribute_id(), " ",
                     carried_predicate.second.get_operator_type(), " ",
                     carried_predicate.second.get_constant_value());
          }
          path_rules_[pattern_id].push_back(path_rule_cp);
          inner_offset += 1;
        }
      }
    }
    begin_from += offset;
    offset = inner_offset;
  }
}

void RuleMiner::MineGCRs() {
  gcr_factory_ = GCRFactory(consequence_predicates_, variable_predicates_,
                            max_predicate_num_);
  // Enumerate all star pattern pairs.
  for (size_t i = 0; i < path_rules_.size(); i++) {
    for (size_t j = i; j < path_rules_.size(); j++) {
      LOG_INFO("Path Pattern ID: ", i, " ", j);
      VertexLabel left_label = std::get<0>(path_patterns_[i][0]);
      VertexLabel right_label = std::get<0>(path_patterns_[j][0]);
      // Enumerate all GCRs
      for (size_t k = 0; k < path_rules_[i].size(); k++) {
        for (size_t l = k; l < path_rules_[j].size(); l++) {
          StarRule left_star = {&path_rules_[i][k]};
          StarRule right_star = {&path_rules_[j][l]};
          // Initialize the GCR: assign consequence predicate and variable
          // predicates.
          GCR gcr(left_star, right_star);
          std::vector<GCR> init_gcrs;
          LOG_INFO("Initialize GCR: ", i, " ", j, " ", k, " ", l);
          bool init_status = gcr_factory_.InitializeGCRs(gcr, true, &init_gcrs);
          LOG_INFO("Initialize size of: ", i, " ", j, " ", k, " ", l, ": ",
                   init_gcrs.size());
          if (!init_status) {
            LOG_INFO("Failed to initialize GCR.");
            continue;
          }
          for (const auto& init_gcr : init_gcrs) {
            // Check the support and match of `init_gcr`.
            std::pair<size_t, size_t> support_and_match =
                init_gcr.ComputeMatchAndSupport();
            // If the match is less than the threshold, continue.

            // If the support is higher than the threshold, add it to the
            // verified GCRs than continue.

            // Otherwise, extend the GCR.
            ExtendGCR(init_gcr, j, l, left_label, right_label);
          }
        }
      }
    }
  }
}

void RuleMiner::ExtendGCR(const GCR& gcr, size_t start_pattern_id,
                          size_t start_rule_id, VertexLabel left_center_label,
                          VertexLabel right_center_label) {
  if (gcr.CountPreconditions() >= max_predicate_num_) return;

  for (size_t i = start_pattern_id; i < path_rules_.size(); i++) {
    if (std::get<0>(path_patterns_[i][0]) != left_center_label ||
        std::get<0>(path_patterns_[i][0]) != right_center_label) {
      continue;
    }
    for (size_t j = start_rule_id; j < path_rules_[i].size(); j++) {
      std::vector<GCR> extended_gcrs;
      bool extend_status = gcr_factory_.MergeAndCompleteGCRs(
          gcr, &path_rules_[i][j], max_path_num_, &extended_gcrs);
      for (const auto& extended_gcr : extended_gcrs) {
        // Check the support and match of `extended_gcr`.
        std::pair<size_t, size_t> support_and_match =
            extended_gcr.ComputeMatchAndSupport();
        // If the match is less than the threshold, continue.

        // If the support is higher than the threshold, add it to the verified
        // GCRs than continue.

        // Otherwise, extend the GCR.
        ExtendGCR(extended_gcr, i, j, left_center_label, right_center_label);
      }
    }
  }
}

}  // namespace sics::graph::miniclean::components::rule_discovery::refactor
