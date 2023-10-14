#include "miniclean/components/rule_discovery/refactor_rule_miner.h"

#include <yaml-cpp/yaml.h>

#include <fstream>
#include <functional>
#include <set>

#include "core/common/multithreading/thread_pool.h"
#include "core/data_structures/buffer.h"
#include "core/data_structures/graph/serialized_immutable_csr_graph.h"
#include "core/util/logging.h"
#include "miniclean/components/matcher/path_matcher.h"
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

template void RuleMiner::ComposeUnits<RuleMiner::StarRule>(
    const std::vector<std::vector<StarRule>>& unit_container, size_t max_item,
    bool check_support, size_t start_idx,
    std::vector<StarRule>& intermediate_result,
    std::vector<StarRule>* composed_results);

template void RuleMiner::ComposeUnits<RuleMiner::PathRule>(
    const std::vector<std::vector<PathRule>>& unit_container, size_t max_item,
    bool check_support, size_t start_idx,
    std::vector<PathRule>& intermediate_result,
    std::vector<PathRule>* composed_results);

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

void RuleMiner::LoadIndexCollection(const std::string& workspace_path) {
  std::string vertex_attribute_file =
      workspace_path + "/attribute_index_config.yaml";
  std::string path_instance_file = workspace_path + "/matched_path_patterns";
  std::string graph_config_path = workspace_path + "/graph/meta.yaml";
  std::string path_pattern_path = workspace_path + "/path_patterns.yaml";
  index_collection_.LoadIndexCollection(vertex_attribute_file,
                                        path_instance_file, graph_config_path,
                                        path_pattern_path);
}

void RuleMiner::PrepareGCRComponents(const std::string& workspace_path) {
  // Load path patterns.
  std::string path_pattern_path = workspace_path + "/path_patterns.yaml";
  LoadPathPatterns(path_pattern_path);
  // Load predicates.
  std::string predicates_path = workspace_path + "/predicates.yaml";
  LoadPredicates(predicates_path);
  // Init star rules.
  InitStarRuleUnitContainer();
  // Init path patterns.
  InitPathRuleUnitContainer();

  // Compose star rule units.
  star_rules_.reserve(star_rule_unit_container_.size());
  for (size_t i = 0; i < star_rule_unit_container_.size(); i++) {
    // Add empty star rule.
    star_rules_.emplace_back();
    if (star_rule_unit_container_[i].empty()) continue;
    star_rules_.back().emplace_back(i, &index_collection_);
    // Add star rules with at least one predicate.
    std::vector<StarRule> empty_intermediate_result;
    ComposeUnits(star_rule_unit_container_[i],
                 Configurations::Get()->max_predicate_num_ - 1, true, 0,
                 empty_intermediate_result, &star_rules_.back());
  }

  // Compose path rule units.
  path_rules_.reserve(path_rule_unit_container_.size());
  // Dim. #1: path pattern id.
  // Dim. #2: vertex index in the path.
  // Dim. #3: path rules
  std::vector<std::vector<std::vector<PathRule>>> vertex_level_path_rules;
  for (size_t i = 0; i < path_rule_unit_container_.size(); i++) {
    // Add empty path rule
    path_rules_.emplace_back();
    path_rules_.back().emplace_back(i);
    vertex_level_path_rules.emplace_back();
    for (size_t j = 0; j < path_rule_unit_container_[i].size(); j++) {
      vertex_level_path_rules.back().emplace_back();
      // Add path rules with at least one predicate.
      std::vector<PathRule> empty_intermediate_result;
      ComposeUnits(path_rule_unit_container_[i][j],
                   Configurations::Get()->max_predicate_num_ - 1, false, 0,
                   empty_intermediate_result,
                   &vertex_level_path_rules.back().back());
    }
  }
  for (size_t i = 0; i < path_rule_unit_container_.size(); i++) {
    std::vector<PathRule> empty_intermediate_result;
    ComposeUnits(vertex_level_path_rules[i],
                 Configurations::Get()->max_predicate_num_ - 1, false, 0,
                 empty_intermediate_result, &path_rules_[i]);
  }
}

void RuleMiner::LoadPathPatterns(const std::string& path_pattern_path) {
  YAML::Node path_pattern_config = YAML::LoadFile(path_pattern_path);
  YAML::Node path_pattern_node = path_pattern_config["PathPatterns"];
  path_patterns_ = path_pattern_node.as<std::vector<PathPattern>>();
}

void RuleMiner::LoadPredicates(const std::string& predicates_path) {
  YAML::Node predicate_nodes = YAML::LoadFile(predicates_path);

  std::vector<ConstantPredicate> constant_predicates =
      predicate_nodes["ConstantPredicates"]
          .as<std::vector<ConstantPredicate>>();
  for (const auto& const_pred : constant_predicates) {
    VertexLabel vertex_label = const_pred.get_vertex_label();
    VertexAttributeID attribute_id = const_pred.get_vertex_attribute_id();
    // Assume all constant predicates' operator types are `kEq`.
    // If the operator type can be `kGt`, `predicates[vlabel][attr_id]` should
    // be a vector instead of a `ConstantPredicate`
    constant_predicate_container_[vertex_label][attribute_id] = const_pred;
  }
  variable_predicates_ = predicate_nodes["VariablePredicates"]
                             .as<std::vector<VariablePredicate>>();
  consequence_predicates_ =
      predicate_nodes["Consequences"].as<std::vector<VariablePredicate>>();
}

void RuleMiner::InitStarRuleUnitContainer() {
  // Collect center labels of patterns.
  std::set<VertexLabel> center_labels;
  VertexLabel max_label = 0;
  for (const auto& path_pattern : path_patterns_) {
    VertexLabel center_label = std::get<0>(path_pattern[0]);
    center_labels.insert(center_label);
    max_label = std::max(max_label, center_label);
  }

  // Initialize star rule units.
  star_rule_unit_container_.resize(max_label + 1);
  for (const auto& center_label : center_labels) {
    const auto& attr_bucket_by_vlabel =
        index_collection_.GetAttributeBucketByVertexLabel(center_label);
    const auto& constant_predicate_by_vlabel =
        constant_predicate_container_[center_label];
    // Compute the max attribute id.
    VertexAttributeID max_attr_id = 0;
    for (const auto& const_pred : constant_predicate_by_vlabel) {
      max_attr_id = std::max(max_attr_id, const_pred.first);
    }
    star_rule_unit_container_[center_label].resize(max_attr_id + 1);
    for (const auto& const_pred : constant_predicate_by_vlabel) {
      // key: attr id; val: const pred.
      VertexAttributeID attr_id = const_pred.first;
      ConstantPredicate pred = const_pred.second;
      const auto& value_bucket = attr_bucket_by_vlabel.at(attr_id);
      // TODO: we do not reserve space beforehand for star rule unit because not
      // every attribute value has enough support. Optimize it in the future if
      // possible.
      for (const auto& value_bucket_pair : value_bucket) {
        VertexAttributeValue value = value_bucket_pair.first;
        const auto& valid_vertices = value_bucket_pair.second;
        if (valid_vertices.size() <
            Configurations::Get()->star_support_threshold_) {
          continue;
        }
        star_rule_unit_container_[center_label][attr_id].emplace_back(
            center_label, pred, value, &index_collection_);
      }
    }
  }
}

void RuleMiner::InitPathRuleUnitContainer() {
  path_rule_unit_container_.resize(path_patterns_.size());
  for (size_t i = 0; i < path_patterns_.size(); i++) {
    const auto& path_pattern = path_patterns_[i];
    path_rule_unit_container_[i].resize(path_pattern.size());
    // Do not assign constant predicate to the center vertex.
    for (size_t j = 1; j < path_pattern.size(); j++) {
      VertexLabel label = std::get<0>(path_pattern[j]);
      if (constant_predicate_container_.find(label) ==
          constant_predicate_container_.end()) {
        continue;
      }
      const auto& attr_bucket_by_vlabel =
          index_collection_.GetAttributeBucketByVertexLabel(label);
      const auto& const_pred_by_attr_id =
          constant_predicate_container_.at(label);
      // Compute the max attribute id.
      VertexAttributeID max_attr_id = 0;
      for (const auto& const_pred : const_pred_by_attr_id) {
        max_attr_id = std::max(max_attr_id, const_pred.first);
      }
      path_rule_unit_container_[i][j].resize(max_attr_id + 1);
      for (const auto& const_pred_pair : const_pred_by_attr_id) {
        VertexAttributeID attr_id = const_pred_pair.first;
        auto const_pred = const_pred_pair.second;
        const auto& value_bucket = attr_bucket_by_vlabel.at(attr_id);
        // TODO: we do not reserve space beforehand for star rule unit because
        // not every attribute value has enough support. Optimize it in the
        // future if possible.
        for (const auto& value_bucket_pair : value_bucket) {
          VertexAttributeValue value = value_bucket_pair.first;
          const auto& valid_vertices = value_bucket_pair.second;
          if (valid_vertices.size() <
              Configurations::Get()->star_support_threshold_) {
            continue;
          }
          path_rule_unit_container_[i][j][attr_id].emplace_back(
              i, j, const_pred, value);
        }
      }
    }
  }
}

template <typename T>
void RuleMiner::ComposeUnits(const std::vector<std::vector<T>>& unit_container,
                             size_t max_item, bool check_support,
                             size_t start_idx,
                             std::vector<T>& intermediate_result,
                             std::vector<T>* composed_results) {
  // Check return condition.
  size_t predicate_count = 0;
  for (const auto& result : intermediate_result) {
    predicate_count += result.get_constant_predicates().size();
  }
  if (predicate_count >= max_item) return;

  for (size_t i = start_idx; i < unit_container.size(); i++) {
    // Compose intermediate result with unit_container[i].
    for (size_t j = 0; j < unit_container[i].size(); j++) {
      intermediate_result.emplace_back(unit_container[i][j]);
      // Construct the composed item.
      T composed_item = intermediate_result[0];
      for (size_t k = 1; k < intermediate_result.size(); k++) {
        composed_item.ComposeWith(intermediate_result[k]);
      }
      // Check support.
      if (check_support) {
        size_t support = composed_item.ComputeInitSupport();
        if (support < Configurations::Get()->star_support_threshold_) {
          intermediate_result.pop_back();
          continue;
        }
      }
      composed_results->emplace_back(composed_item);
      ComposeUnits(unit_container, max_item, check_support, i + 1,
                   intermediate_result, composed_results);
      intermediate_result.pop_back();
    }
  }
}

void RuleMiner::MineGCRs() {
  for (size_t l_label = 0; l_label < star_rules_.size(); l_label++) {
    if (star_rules_[l_label].empty()) continue;
    for (size_t r_label = l_label; r_label < star_rules_.size(); r_label++) {
      if (star_rules_[r_label].empty()) continue;
      VertexLabel ll = std::get<0>(path_patterns_[l_label][0]);
      VertexLabel rl = std::get<0>(path_patterns_[r_label][0]);
      for (size_t ls = 0; ls < star_rules_[ll].size(); ls++) {
        size_t j_start = (ll == rl) ? ls : 0;
        for (size_t rs = j_start; rs < star_rules_[rl].size(); rs++) {
          GCR gcr = GCR(star_rules_[ll][ls], star_rules_[rl][rs]);
          ExtendGCR(gcr);
        }
      }
    }
  }
}

void RuleMiner::ExtendGCR(GCR& gcr) {
  // Check whether the GCR should be extended.
  if (gcr.get_left_star().get_path_rules().size() +
          gcr.get_right_star().get_path_rules().size() >=
      Configurations::Get()->max_path_num_) {
    return;
  }
  // Compute vertical extensions.
  std::vector<GCRVerticalExtension> vertical_extensions;
  ComputeVerticalExtensions(gcr, &vertical_extensions);
  // Compute horizontal extensions for each vertical extension.
  for (const auto& vertical_extension : vertical_extensions) {
    // Vertical extension.
    gcr.Backup();
    gcr.VerticalExtend(vertical_extension);
    // Compute horizontal extensions.
    std::vector<GCRHorizontalExtension> horizontal_extensions;
    ComputeHorizontalExtensions(gcr, vertical_extension.first,
                                &horizontal_extensions);

    for (const auto& horizontal_extension : horizontal_extensions) {
      // Horizontal extension.
      gcr.Backup();
      gcr.HorizontalExtend(horizontal_extension);
      // Compute support of GCR

      // If support < threshold, continue.

      // If support >= threshold, confidence >= threshold, write back to disk.

      // If support >= threshold, confidence < threshold, go to next level.
      ExtendGCR(gcr);
      gcr.Recover();
    }
    gcr.Recover();
  }
}

void RuleMiner::ComputeVerticalExtensions(
    const GCR& gcr, std::vector<GCRVerticalExtension>* extensions) {
  // Check whether the number of path rules exceeds the limit.
  if (gcr.get_left_star().get_path_rules().size() +
          gcr.get_right_star().get_path_rules().size() >=
      Configurations::Get()->max_path_num_) {
    return;
  }
  StarRule left_star = gcr.get_left_star();
  StarRule right_star = gcr.get_right_star();
  std::vector<PathRule> left_paths = left_star.get_path_rules();
  std::vector<PathRule> right_paths = right_star.get_path_rules();
  VertexLabel llabel = left_star.get_center_label();
  VertexLabel rlabel = right_star.get_center_label();
  bool choose_left = true;
  size_t min_pattern_id = 0;
  if (left_paths.size() > right_paths.size()) choose_left = false;
  if (right_paths.size() != 0) {
    min_pattern_id = std::min(left_paths.back().get_path_pattern_id(),
                              right_paths.back().get_path_pattern_id());
  } else if (left_paths.size() != 0) {
    min_pattern_id = left_paths.back().get_path_pattern_id();
  }
  for (size_t i = min_pattern_id; i < path_rules_.size(); i++) {
    if (choose_left) {
      if (llabel != std::get<0>(path_patterns_[i][0])) continue;
    } else {
      if (rlabel != std::get<0>(path_patterns_[i][0])) continue;
    }
    for (size_t j = 0; j < path_rules_[i].size(); j++) {
      extensions->emplace_back(choose_left, path_rules_[i][j]);
    }
  }
}

void RuleMiner::ComputeHorizontalExtensions(
    const GCR& gcr, bool from_left,
    std::vector<GCRHorizontalExtension>* extensions) {
  // Check whether the number of predicates exceeds the limit.
  if (gcr.get_constant_predicate_count() +
          gcr.get_variable_predicates().size() >=
      Configurations::Get()->max_predicate_num_) {
    return;
  }

  // Compute left and right bias.
  auto left_path_rules = gcr.get_left_star().get_path_rules();
  auto right_path_rules = gcr.get_right_star().get_path_rules();
  size_t lhs_start_path_index = 0;
  size_t rhs_start_path_index = 0;
  size_t lhs_start_vertex_index = 0;
  size_t rhs_start_vertex_index = 0;
  if (from_left) {
    lhs_start_path_index = left_path_rules.size() - 1;
    lhs_start_vertex_index = 1;
  } else {
    rhs_start_path_index = right_path_rules.size() - 1;
    rhs_start_vertex_index = 1;
  }

  // Assign consequence.
  std::vector<ConcreteVariablePredicate> c_consequences;
  ExtendConsequences(gcr, lhs_start_path_index, rhs_start_path_index,
                     lhs_start_vertex_index, rhs_start_vertex_index,
                     &c_consequences);

  // Assign variable predicates.
  ExtendVariablePredicates(gcr, c_consequences, lhs_start_path_index,
                           rhs_start_path_index, lhs_start_vertex_index,
                           rhs_start_vertex_index, extensions);
}

void RuleMiner::ExtendConsequences(
    const GCR& gcr, size_t lhs_start_path_index, size_t rhs_start_path_index,
    size_t lhs_start_vertex_index, size_t rhs_start_vertex_index,
    std::vector<ConcreteVariablePredicate>* consequences) {
  auto left_path_rules = gcr.get_left_star().get_path_rules();
  auto right_path_rules = gcr.get_right_star().get_path_rules();
  for (const auto& consequence : consequence_predicates_) {
    auto target_left_label = consequence.get_lhs_label();
    auto target_right_label = consequence.get_rhs_label();
    // Check two center vertices.
    if (gcr.get_left_star().get_center_label() == target_left_label &&
        gcr.get_right_star().get_center_label() == target_right_label) {
      ConcreteVariablePredicate center_cvp(consequence, 0, 0, 0, 0);
      if (!gcr.IsCompatibleWith(center_cvp, false)) continue;
      consequences->emplace_back(center_cvp);
    }
    // Check other pairs.
    for (size_t i = lhs_start_path_index; i < left_path_rules.size(); i++) {
      auto left_pattern =
          path_patterns_[left_path_rules[i].get_path_pattern_id()];
      for (size_t j = rhs_start_path_index; j < right_path_rules.size(); j++) {
        auto right_pattern =
            path_patterns_[right_path_rules[j].get_path_pattern_id()];
        for (size_t k = lhs_start_vertex_index; k < left_pattern.size(); k++) {
          if (std::get<0>(left_pattern[k]) != target_left_label) continue;
          for (size_t l = lhs_start_vertex_index; l < right_pattern.size();
               l++) {
            if (std::get<0>(right_pattern[l]) != target_right_label) continue;
            ConcreteVariablePredicate cvp(consequence, i, k, j, l);
            if (!gcr.IsCompatibleWith(cvp, false)) continue;
            consequences->emplace_back(cvp);
          }
        }
      }
    }
  }
}

void RuleMiner::ExtendVariablePredicates(
    const GCR& gcr, const std::vector<ConcreteVariablePredicate>& consequences,
    size_t lhs_start_path_index, size_t rhs_start_path_index,
    size_t lhs_start_vertex_index, size_t rhs_start_vertex_index,
    std::vector<GCRHorizontalExtension>* extensions) {
  auto left_path_rules = gcr.get_left_star().get_path_rules();
  auto right_path_rules = gcr.get_right_star().get_path_rules();
  // Check the available number of variable predicates.
  size_t const_pred_num = gcr.get_constant_predicate_count();
  size_t var_pred_num = gcr.get_variable_predicates().size();
  size_t consequence_num = 1;
  size_t available_var_pred_num = Configurations::Get()->max_predicate_num_ -
                                  const_pred_num - consequence_num;
  if (gcr.get_variable_predicates().size() >=
      Configurations::Get()->max_variable_predicate_num_) {
    for (const auto& c_consequence : consequences) {
      std::vector<ConcreteVariablePredicate> empty_cvps;
      extensions->emplace_back(c_consequence, empty_cvps);
    }
  }

  for (const auto& variable_predicate : variable_predicates_) {
    auto target_left_label = variable_predicate.get_lhs_label();
    auto target_right_label = variable_predicate.get_rhs_label();
    // Check two center vertices.
    if (gcr.get_left_star().get_center_label() == target_left_label &&
        gcr.get_right_star().get_center_label() == target_right_label) {
      std::vector<ConcreteVariablePredicate> center_cvps;
      center_cvps.emplace_back(variable_predicate, 0, 0, 0, 0);
      if (!gcr.IsCompatibleWith(center_cvps.back(), true)) continue;
      for (const auto& c_consequence : consequences) {
        if (!gcr.TestCompatibility(c_consequence, center_cvps)) continue;
        extensions->emplace_back(c_consequence, center_cvps);
      }
    }
    // Check other pairs.
    for (size_t i = lhs_start_path_index; i < left_path_rules.size(); i++) {
      auto left_pattern =
          path_patterns_[left_path_rules[i].get_path_pattern_id()];
      for (size_t j = rhs_start_path_index; j < right_path_rules.size(); j++) {
        auto right_pattern =
            path_patterns_[right_path_rules[j].get_path_pattern_id()];
        std::vector<ConcreteVariablePredicate> variable_predicates;
        for (size_t k = lhs_start_vertex_index; k < left_pattern.size(); k++) {
          if (std::get<0>(left_pattern[k]) != target_left_label) continue;
          for (size_t l = rhs_start_path_index; l < right_pattern.size(); l++) {
            if (std::get<0>(right_pattern[l]) != target_right_label) continue;
            ConcreteVariablePredicate cvp(variable_predicate, i, k, j, l);
            if (!gcr.IsCompatibleWith(cvp, true)) continue;
            variable_predicates.emplace_back(cvp);
          }
        }
        // Select `available_var_pred_num` predicates from `variable_predicates`
        std::vector<std::vector<ConcreteVariablePredicate>>
            c_variable_predicates;
        std::vector<ConcreteVariablePredicate> empty_intermediate_result;
        EnumerateValidVariablePredicates(
            variable_predicates, 0, available_var_pred_num,
            empty_intermediate_result, &c_variable_predicates);
        for (const auto& c_consequence : consequences) {
          for (const auto& c_variable_predicate : c_variable_predicates) {
            if (!gcr.TestCompatibility(c_consequence, c_variable_predicate))
              continue;
            extensions->emplace_back(c_consequence, c_variable_predicate);
          }
        }
      }
    }
  }
}

void RuleMiner::EnumerateValidVariablePredicates(
    const std::vector<ConcreteVariablePredicate>& variable_predicates,
    size_t start_idx, size_t max_item_num,
    std::vector<ConcreteVariablePredicate>& intermediate_result,
    std::vector<std::vector<ConcreteVariablePredicate>>*
        valid_variable_predicates) {
  // Check return condition.
  if (intermediate_result.size() >= max_item_num) {
    return;
  }
  for (size_t i = start_idx; i < variable_predicates.size(); i++) {
    intermediate_result.emplace_back(variable_predicates[i]);
    valid_variable_predicates->emplace_back(intermediate_result);
    EnumerateValidVariablePredicates(variable_predicates, i + 1, max_item_num,
                                     intermediate_result,
                                     valid_variable_predicates);
    intermediate_result.pop_back();
  }
}

}  // namespace sics::graph::miniclean::components::rule_discovery::refactor
