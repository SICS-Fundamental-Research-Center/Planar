#include "miniclean/components/rule_discovery/rule_miner.h"

#include <yaml-cpp/yaml.h>

#include <set>

#include "core/common/multithreading/thread_pool.h"
#include "core/data_structures/buffer.h"
#include "core/data_structures/graph/serialized_immutable_csr_graph.h"
#include "core/util/logging.h"
#include "miniclean/components/matcher/path_matcher.h"
#include "miniclean/io/miniclean_csr_reader.h"

namespace sics::graph::miniclean::components::rule_discovery {

using EdgeLabel = sics::graph::miniclean::common::EdgeLabel;
using VertexLabel = sics::graph::miniclean::common::VertexLabel;
using VertexAttributeID = sics::graph::miniclean::common::VertexAttributeID;
using MiniCleanCSRReader = sics::graph::miniclean::io::MiniCleanCSRReader;
using SerializedImmutableCSRGraph =
    sics::graph::core::data_structures::graph::SerializedImmutableCSRGraph;
using ReadMessage = sics::graph::core::scheduler::ReadMessage;
using ThreadPool = sics::graph::core::common::ThreadPool;
using OwnedBuffer = sics::graph::core::data_structures::OwnedBuffer;
using PathRule = sics::graph::miniclean::data_structures::gcr::PathRule;
using PathPattern = sics::graph::miniclean::common::PathPattern;
using ConcreteVariablePredicate =
    sics::graph::miniclean::data_structures::gcr::ConcreteVariablePredicate;
using GCRVerticalExtension =
    sics::graph::miniclean::data_structures::gcr::GCRVerticalExtension;
using GCRHorizontalExtension =
    sics::graph::miniclean::data_structures::gcr::GCRHorizontalExtension;
using Task = sics::graph::core::common::Task;
using TaskPackage = sics::graph::core::common::TaskPackage;

void RuleMiner::LoadGraph(const std::string& graph_path) {
  // Prepare reader.
  MiniCleanCSRReader reader(graph_path);

  // Initialize Serialized object.
  std::unique_ptr<SerializedImmutableCSRGraph> serialized_graph =
      std::make_unique<SerializedImmutableCSRGraph>();

  // Initialize ReadMessage object.
  ReadMessage read_message;
  read_message.graph_id = graph_ptr_->get_metadata().gid;
  read_message.response_serialized = serialized_graph.get();

  // Read a subgraph.
  reader.Read(&read_message, nullptr);

  // Deserialize the subgraph.
  ThreadPool thread_pool(1);
  graph_ptr_->Deserialize(thread_pool, std::move(serialized_graph));
}

void RuleMiner::LoadIndexCollection(const std::string& workspace_path) {
  std::string vertex_attribute_file =
      workspace_path + "/attribute_index_config.yaml";
  std::string path_instance_file = workspace_path + "/matched_path_patterns";
  std::string graph_config_path = workspace_path + "/graph/meta.yaml";
  std::string path_pattern_path = workspace_path + "/path_patterns.yaml";
  std::string label_range_path = workspace_path + "/vlabel/vlabel_offset.yaml";
  index_collection_.LoadIndexCollection(vertex_attribute_file,
                                        path_instance_file, graph_config_path,
                                        path_pattern_path, label_range_path);
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
    star_rules_.back().emplace_back(i, index_collection_);
    // Add star rules with at least one predicate.
    std::vector<StarRule> empty_intermediate_result;
    ComposeUnits(star_rule_unit_container_[i],
                 Configurations::Get()->max_predicate_num_ - 1, true, 0,
                 &empty_intermediate_result, &star_rules_.back());
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
                   &empty_intermediate_result,
                   &vertex_level_path_rules.back().back());
    }
  }
  for (size_t i = 0; i < path_rule_unit_container_.size(); i++) {
    std::vector<PathRule> empty_intermediate_result;
    ComposeUnits(vertex_level_path_rules[i],
                 Configurations::Get()->max_predicate_num_ - 1, false, 0,
                 &empty_intermediate_result, &path_rules_[i]);
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
    auto constant_predicate_by_vlabel =
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
      auto value_bucket = attr_bucket_by_vlabel.at(attr_id);
      // TODO: we do not reserve space beforehand for star rule unit because not
      // every attribute value has enough support. Optimize it in the future if
      // possible.
      for (const auto& value_bucket_pair : value_bucket) {
        VertexAttributeValue value = value_bucket_pair.first;
        auto valid_vertices = value_bucket_pair.second;
        if (valid_vertices.size() <
            Configurations::Get()->star_support_threshold_) {
          continue;
        }
        star_rule_unit_container_[center_label][attr_id].emplace_back(
            center_label, pred, value, index_collection_);
      }
    }
  }
}

void RuleMiner::InitPathRuleUnitContainer() {
  path_rule_unit_container_.resize(path_patterns_.size());
  for (size_t i = 0; i < path_patterns_.size(); i++) {
    auto path_pattern = path_patterns_[i];
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
      auto const_pred_by_attr_id = constant_predicate_container_.at(label);
      // Compute the max attribute id.
      VertexAttributeID max_attr_id = 0;
      for (const auto& const_pred : const_pred_by_attr_id) {
        max_attr_id = std::max(max_attr_id, const_pred.first);
      }
      path_rule_unit_container_[i][j].resize(max_attr_id + 1);
      for (const auto& const_pred_pair : const_pred_by_attr_id) {
        VertexAttributeID attr_id = const_pred_pair.first;
        auto const_pred = const_pred_pair.second;
        auto value_bucket = attr_bucket_by_vlabel.at(attr_id);
        // TODO: we do not reserve space beforehand for star rule unit because
        // not every attribute value has enough support. Optimize it in the
        // future if possible.
        for (const auto& value_bucket_pair : value_bucket) {
          VertexAttributeValue value = value_bucket_pair.first;
          auto valid_vertices = value_bucket_pair.second;
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

void RuleMiner::MineGCRsPar(uint32_t parallelism) {
  ThreadPool thread_pool(parallelism);
  ThreadPool* thread_pool_ptr = &thread_pool;
  std::atomic_uint32_t pending_tasks_num{0};
  std::atomic_uint32_t* pending_tasks_num_ptr = &pending_tasks_num;
  std::atomic_uint32_t total_tasks_num{0};
  std::atomic_uint32_t* total_tasks_num_ptr = &total_tasks_num;

  for (size_t l_label = 0; l_label < star_rules_.size(); l_label++) {
    if (star_rules_[l_label].empty()) continue;
    for (size_t r_label = l_label; r_label < star_rules_.size(); r_label++) {
      if (star_rules_[r_label].empty()) continue;
      for (size_t ls = 0; ls < star_rules_[l_label].size(); ls++) {
        size_t j_start = (l_label == r_label) ? ls : 0;
        for (size_t rs = j_start; rs < star_rules_[r_label].size(); rs++) {
          std::shared_ptr<GCR> gcr_ptr = std::make_shared<GCR>(
              star_rules_[l_label][ls], star_rules_[r_label][rs]);
          // GCR initialization (vertical extension).
          gcr_ptr->Init();
          Task task = std::bind(
              [this, pending_tasks_num_ptr, total_tasks_num_ptr,
               thread_pool_ptr](std::shared_ptr<GCR> parent_gcr_ptr) {
                MineGCRHorizontally(parent_gcr_ptr, pending_tasks_num_ptr,
                                    total_tasks_num_ptr, thread_pool_ptr);
                std::lock_guard<std::mutex> lck(rule_discovery_mtx_);
                (*pending_tasks_num_ptr)--;
                LOGF_INFO(
                    "Unfinished tasks: {}. Total tasks: {}, Pending tasks: {}",
                    (*pending_tasks_num_ptr).load(),
                    (*total_tasks_num_ptr).load(),
                    thread_pool_ptr->GetPendingTaskCount());
                if ((*pending_tasks_num_ptr).load() == 0) cv_.notify_all();
              },
              gcr_ptr);
          pending_tasks_num++;
          total_tasks_num++;
          thread_pool.SubmitAsync(std::move(task));
        }
      }
    }
  }

  std::unique_lock<std::mutex> lck(rule_discovery_mtx_);
  cv_.wait(lck, [&] { return pending_tasks_num == 0; });
}

void RuleMiner::MineGCRHorizontally(std::shared_ptr<GCR> parent_gcr_ptr,
                                    std::atomic_uint32_t* pending_tasks_num_ptr,
                                    std::atomic_uint32_t* total_tasks_num_ptr,
                                    ThreadPool* thread_pool) {
  // Compute Horizontal extensions
  std::vector<GCRHorizontalExtension> horizontal_extensions =
      ComputeHorizontalExtensions(*parent_gcr_ptr, true);
  // Define the subtask.
  auto verify_and_extend = [this, pending_tasks_num_ptr, total_tasks_num_ptr,
                            thread_pool](std::shared_ptr<GCR> gcr_ptr,
                                         bool is_subtask) {
    // Verify the GCR.
    std::pair<size_t, size_t> match_result =
        gcr_ptr->ComputeMatchAndSupport(*graph_ptr_);
    size_t match = match_result.first;
    size_t support = match_result.second;
    float confidence = 0;
    if (match != 0) confidence = static_cast<float>(support) / match;
    if (support >= Configurations::Get()->support_threshold_ &&
        confidence >= Configurations::Get()->confidence_threshold_) {
      // If support, confidenc >= threshold, write back to disk.
      std::string gcr_info =
          gcr_ptr->GetInfoString(path_patterns_, match, support, confidence);
      LOG_INFO(gcr_info);
    }
    if (support >= Configurations::Get()->support_threshold_ &&
        confidence < Configurations::Get()->confidence_threshold_ &&
        gcr_ptr->get_left_star().get_path_rules().size() +
                gcr_ptr->get_right_star().get_path_rules().size() <
            Configurations::Get()->max_path_num_) {
      // If support >= threshold but confidence < threshold and the GCR has not
      // hit the depth limit, extend the GCR vertically.
      MineGCRVertically(gcr_ptr, pending_tasks_num_ptr, total_tasks_num_ptr,
                        thread_pool);
    }
    if (is_subtask) {
      std::lock_guard<std::mutex> lck(rule_discovery_mtx_);
      (*pending_tasks_num_ptr)--;
      LOGF_INFO("Unfinished tasks: {}. Total tasks: {}. Pending task count: {}",
                (*pending_tasks_num_ptr).load(), (*total_tasks_num_ptr).load(),
                thread_pool->GetPendingTaskCount());
      if ((*pending_tasks_num_ptr).load() == 0) cv_.notify_all();
    }
  };
  // Pending flag.
  bool pending_flag = thread_pool->GetPendingTaskCount() <=
                      Configurations::Get()->min_pending_tasks_;
  // Perform horizontal extension.
  TaskPackage task_package;
  for (size_t i = 0; i < horizontal_extensions.size(); i++) {
    // Copy the parent node.
    std::shared_ptr<GCR> child_gcr_ptr = std::make_shared<GCR>(*parent_gcr_ptr);
    // Extend horizontally.
    child_gcr_ptr->ExtendHorizontally(horizontal_extensions[i], *graph_ptr_, i,
                                      horizontal_extensions.size());
    if (pending_flag) {
      Task task = std::bind(verify_and_extend, child_gcr_ptr, true);
      task_package.push_back(task);
      continue;
    }
    verify_and_extend(child_gcr_ptr, false);
  }
  // If task package is not empty, submit tasks.
  if (!task_package.empty()) {
    *pending_tasks_num_ptr += task_package.size();
    *total_tasks_num_ptr += task_package.size();
    thread_pool->SubmitAsync(task_package);
    LOGF_INFO("Submit {} tasks.", task_package.size());
  }
}

void RuleMiner::MineGCRVertically(std::shared_ptr<GCR> gcr_ptr,
                                  std::atomic_uint32_t* pending_tasks_num_ptr,
                                  std::atomic_uint32_t* total_tasks_num_ptr,
                                  ThreadPool* thread_pool) {
  // Check whether the GCR should be extended.
  if (gcr_ptr->get_left_star().get_path_rules().size() +
          gcr_ptr->get_right_star().get_path_rules().size() >=
      Configurations::Get()->max_path_num_) {
    LOG_FATAL("GCR hits the depth limit.");
  }
  // Compute vertical extensions.
  std::vector<GCRVerticalExtension> vertical_extensions =
      ComputeVerticalExtensions(*gcr_ptr);
  // Compute horizontal extensions for each vertical extension.
  for (size_t i = 0; i < vertical_extensions.size(); i++) {
    // Copy the parent node.
    std::shared_ptr<GCR> child_gcr_ptr = std::make_shared<GCR>(*gcr_ptr);
    // Extend the GCR vertically.
    child_gcr_ptr->ExtendVertically(vertical_extensions[i], *graph_ptr_, i,
                                    vertical_extensions.size());
    MineGCRHorizontally(child_gcr_ptr, pending_tasks_num_ptr,
                        total_tasks_num_ptr, thread_pool);
  }
}

std::vector<GCRVerticalExtension> RuleMiner::ComputeVerticalExtensions(
    const GCR& gcr) const {
  std::vector<GCRVerticalExtension> extensions;
  // Check whether the number of path rules exceeds the limit.
  if (gcr.get_left_star().get_path_rules().size() +
          gcr.get_right_star().get_path_rules().size() >=
      Configurations::Get()->max_path_num_) {
    return extensions;
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
    // Both paths are not empty: choose the minimum pattern id.
    min_pattern_id = std::min(left_paths.back().get_path_pattern_id(),
                              right_paths.back().get_path_pattern_id());
  } else if (left_paths.size() != 0) {
    // Only left path is not empty: choose the minimum pattern id.
    min_pattern_id = left_paths.back().get_path_pattern_id();
  }
  for (size_t i = min_pattern_id; i < path_rules_.size(); i++) {
    if (choose_left) {
      if (llabel != std::get<0>(path_patterns_[i][0])) continue;
    } else {
      if (rlabel != std::get<0>(path_patterns_[i][0])) continue;
    }
    for (size_t j = 0; j < path_rules_[i].size(); j++) {
      extensions.emplace_back(choose_left, path_rules_[i][j]);
    }
  }
  return extensions;
}

std::vector<GCRHorizontalExtension> RuleMiner::ComputeHorizontalExtensions(
    const GCR& gcr, bool from_left) const {
  // Result to return.
  // TODO: Can we reserve space beforehand?
  std::vector<GCRHorizontalExtension> extensions;

  // Assign consequence.
  std::vector<ConcreteVariablePredicate> c_consequences =
      InstantiateVariablePredicates(gcr, consequence_predicates_);

  // Assign variable predicates.
  //   Check whether can extend.
  size_t const_pred_num = gcr.get_constant_predicate_count();
  size_t var_pred_num = gcr.get_variable_predicate_count();
  size_t consequence_num = 1;
  if (Configurations::Get()->max_predicate_num_ <
          const_pred_num + var_pred_num + consequence_num ||
      Configurations::Get()->max_variable_predicate_num_ < var_pred_num) {
    return extensions;
  }
  extensions = ExtendVariablePredicates(gcr, c_consequences);
  return extensions;
}

std::vector<ConcreteVariablePredicate> RuleMiner::InstantiateVariablePredicates(
    const GCR& gcr,
    const std::vector<VariablePredicate>& variable_predicates) const {
  std::vector<ConcreteVariablePredicate> results;
  const auto& left_path_rules = gcr.get_left_star().get_path_rules();
  const auto& right_path_rules = gcr.get_right_star().get_path_rules();
  for (const auto& predicate : variable_predicates) {
    auto target_left_label = predicate.get_lhs_label();
    auto target_right_label = predicate.get_rhs_label();
    // The reason of `+ 1` is that we need to consider the case where the star
    // do not have any path.
    for (size_t i = 0; i < left_path_rules.size() + 1; i++) {
      if (i > 0 && i == left_path_rules.size()) break;
      PathPattern left_pattern;
      if (left_path_rules.size() == 0) {
        left_pattern.reserve(1);
        left_pattern.emplace_back(
            std::make_tuple(gcr.get_left_star().get_center_label(),
                            MAX_EDGE_LABEL, MAX_EDGE_LABEL));
      } else {
        left_pattern = path_patterns_[left_path_rules[i].get_path_pattern_id()];
      }
      for (size_t j = 0; j < right_path_rules.size() + 1; j++) {
        if (j > 0 && j == right_path_rules.size()) break;
        PathPattern right_pattern;
        if (right_path_rules.size() == 0) {
          right_pattern.reserve(1);
          right_pattern.emplace_back(
              std::make_tuple(gcr.get_right_star().get_center_label(),
                              MAX_EDGE_LABEL, MAX_EDGE_LABEL));
        } else {
          right_pattern =
              path_patterns_[right_path_rules[j].get_path_pattern_id()];
        }
        for (size_t k = 0; k < left_pattern.size(); k++) {
          // Skip the center node if it's not the first path.
          if (i > 0 && k == 0) continue;
          if (std::get<0>(left_pattern[k]) != target_left_label) continue;
          for (size_t l = 0; l < right_pattern.size(); l++) {
            if (j > 0 && l == 0) continue;
            if (std::get<0>(right_pattern[l]) != target_right_label) continue;
            ConcreteVariablePredicate cvp(predicate, i, k, j, l);
            if (!gcr.IsCompatibleWith(cvp, false)) continue;
            results.emplace_back(cvp);
          }
        }
      }
    }
  }
  return results;
}

std::vector<GCRHorizontalExtension> RuleMiner::ExtendVariablePredicates(
    const GCR& gcr,
    const std::vector<ConcreteVariablePredicate>& consequences) const {
  std::vector<GCRHorizontalExtension> extensions;
  // Check the available number of variable predicates.
  size_t const_pred_num = gcr.get_constant_predicate_count();
  size_t var_pred_num = gcr.get_variable_predicate_count();
  size_t consequence_num = 1;
  if (Configurations::Get()->max_predicate_num_ <
          const_pred_num + var_pred_num + consequence_num ||
      Configurations::Get()->max_variable_predicate_num_ < var_pred_num) {
    LOG_FATAL("Available variable predicate number < 0");
  }
  size_t available_var_pred_num = std::min(
      Configurations::Get()->max_predicate_num_ - const_pred_num -
          var_pred_num - consequence_num,
      Configurations::Get()->max_variable_predicate_num_ - var_pred_num);
  if (available_var_pred_num == 0) {
    for (const auto& c_consequence : consequences) {
      // We have tested the compability of consequences with the GCR.
      std::vector<ConcreteVariablePredicate> empty_cvps;
      extensions.emplace_back(c_consequence, empty_cvps);
    }
    return extensions;
  }

  std::vector<ConcreteVariablePredicate> variable_predicates =
      InstantiateVariablePredicates(gcr, variable_predicates_);
  std::vector<std::vector<ConcreteVariablePredicate>> c_variable_predicates;
  c_variable_predicates.reserve(ComputeCombinationNum(
      variable_predicates.size(), available_var_pred_num));
  std::vector<ConcreteVariablePredicate> empty_intermediate_result;
  empty_intermediate_result.reserve(available_var_pred_num);
  EnumerateValidVariablePredicates(
      variable_predicates, 0, available_var_pred_num,
      &empty_intermediate_result, &c_variable_predicates);
  extensions = MergeHorizontalExtensions(
      gcr, consequences, c_variable_predicates, available_var_pred_num);
  return extensions;
}

std::vector<GCRHorizontalExtension> RuleMiner::MergeHorizontalExtensions(
    const GCR& gcr,
    const std::vector<ConcreteVariablePredicate>& c_consequences,
    std::vector<std::vector<ConcreteVariablePredicate>> c_variable_predicates,
    size_t available_var_pred_num) const {
  std::vector<GCRHorizontalExtension> extensions;
  if (c_consequences.size() == 0) {
    LOG_FATAL("Consequences is empty");
  }
  if (c_variable_predicates.size() == 0) {
    for (const auto& c_consequence : c_consequences) {
      std::vector<ConcreteVariablePredicate> empty_cvps;
      extensions.emplace_back(c_consequence, empty_cvps);
    }
    return extensions;
  }
  for (const auto& c_consequence : c_consequences) {
    std::vector<ConcreteVariablePredicate> empty_cvps;
    extensions.emplace_back(c_consequence, empty_cvps);
    for (const auto& c_variable_predicate : c_variable_predicates) {
      std::vector<ConcreteVariablePredicate> c_consequence_vec{c_consequence};
      if (!ConcreteVariablePredicate::TestCompatibility(c_consequence_vec,
                                                        c_variable_predicate))
        continue;
      extensions.emplace_back(c_consequence, c_variable_predicate);
    }
  }
  return extensions;
}

void RuleMiner::EnumerateValidVariablePredicates(
    const std::vector<ConcreteVariablePredicate>& variable_predicates,
    size_t start_idx, size_t max_item_num,
    std::vector<ConcreteVariablePredicate>* intermediate_results,
    std::vector<std::vector<ConcreteVariablePredicate>>*
        valid_variable_predicates) const {
  // Check return condition.
  if (intermediate_results->size() >= max_item_num) return;
  for (size_t i = start_idx; i < variable_predicates.size(); i++) {
    intermediate_results->emplace_back(variable_predicates[i]);
    valid_variable_predicates->emplace_back(*intermediate_results);
    EnumerateValidVariablePredicates(variable_predicates, i + 1, max_item_num,
                                     intermediate_results,
                                     valid_variable_predicates);
    intermediate_results->pop_back();
  }
}

// Compute the number of combinations of k elements from a set of n elements.
// TODO: this function should not be bonded to this class, make it free.
size_t RuleMiner::ComputeCombinationNum(size_t n, size_t k) const {
  if (k > n || n <= 0) return 0;
  size_t result = 1;
  for (size_t i = 1; i <= k; ++i) {
    result = result * (n - i + 1) / i;
  }
  return result;
}

}  // namespace sics::graph::miniclean::components::rule_discovery
