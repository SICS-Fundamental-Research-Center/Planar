#include "miniclean/data_structures/gcr/gcr.h"

#include <mutex>

#include "core/util/logging.h"

namespace xyz::graph::miniclean::data_structures::gcr {

using VertexAttributeID = xyz::graph::miniclean::common::VertexAttributeID;
using VertexAttributeValue =
    xyz::graph::miniclean::common::VertexAttributeValue;
using PathPatternID = xyz::graph::miniclean::common::PathPatternID;

void GCR::Init(
    MiniCleanCSRGraph* graph,
    std::vector<std::vector<std::vector<VertexID>>>& path_instances) {
  ThreadPool thread_pool(20);

  size_t num_segments = 5;
  TaskPackage task_package;
  task_package.reserve(num_segments * num_segments);

  for (size_t i = 0; i < num_segments * num_segments; i++) {
    task_package.emplace_back(std::bind(&GCR::InitTask, this, graph,
                                        &path_instances, num_segments, i));
  }

  thread_pool.SubmitSync(task_package);
  task_package.clear();
}

void GCR::InitTask(
    MiniCleanCSRGraph* graph,
    std::vector<std::vector<std::vector<VertexID>>>* path_instances,
    size_t num_segments, size_t task_id) {
  // Retrieve path instances of left and right path pattern.
  std::vector<std::vector<VertexID>>* left_path_instances =
      &(*path_instances)[dual_pattern_.first.front()];
  std::vector<std::vector<VertexID>>* right_path_instances =
      &(*path_instances)[dual_pattern_.second.front()];

  std::list<std::pair<std::list<PathInstanceID>, std::list<PathInstanceID>>>
      local_gcr_instances;

  size_t left_segment_size = left_path_instances->size() / num_segments + 1;
  size_t right_segment_size = right_path_instances->size() / num_segments + 1;

  // TODO: A unique pair of center can at most contribute 1
  // support. Fix this mistake when instance tree is built.
  size_t local_support = 0;
  size_t local_match = 0;

  size_t left_segment_id = task_id / num_segments;
  size_t right_segment_id = task_id % num_segments;

  for (PathInstanceID i = left_segment_id * left_segment_size;
       i < (left_segment_id + 1) * left_segment_size; i++) {
    for (PathInstanceID j = right_segment_id * right_segment_size;
         j < (right_segment_id + 1) * right_segment_size; j++) {
      if (i >= left_path_instances->size() ||
          j >= right_path_instances->size()) {
        break;
      }
      bool is_precondition_satisfied = true;
      bool is_consequence_satisfied = true;
      // Check preconditions.
      for (const auto& precondition : preconditions_) {
        if (precondition->get_predicate_type() == kConstantPredicate) {
          ConstantPredicate* constant_precondition =
              dynamic_cast<ConstantPredicate*>(precondition);
          if (constant_precondition->is_in_lhs_pattern()) {
            is_precondition_satisfied = constant_precondition->ConstantCompare(
                graph, (*left_path_instances)[i]);
          } else {
            is_precondition_satisfied = constant_precondition->ConstantCompare(
                graph, (*right_path_instances)[j]);
          }
          if (!is_precondition_satisfied) {
            break;
          }
        } else if (precondition->get_predicate_type() == kVariablePredicate) {
          VariablePredicate* variable_precondition =
              dynamic_cast<VariablePredicate*>(precondition);

          is_precondition_satisfied = variable_precondition->VariableCompare(
              graph, (*left_path_instances)[i], (*right_path_instances)[j]);

          if (!is_precondition_satisfied) {
            break;
          }
        } else {
          LOG_FATAL("Unsupported predicate type.");
        }
      }
      if (!is_precondition_satisfied) {
        continue;
      }
      local_match++;
      // Check consequences.
      if (consequence_->get_predicate_type() == kConstantPredicate) {
        ConstantPredicate* constant_consequence =
            dynamic_cast<ConstantPredicate*>(consequence_);
        if (constant_consequence->is_in_lhs_pattern()) {
          is_consequence_satisfied = constant_consequence->ConstantCompare(
              graph, (*left_path_instances)[i]);
        } else {
          is_consequence_satisfied = constant_consequence->ConstantCompare(
              graph, (*right_path_instances)[j]);
        }
      } else if (consequence_->get_predicate_type() == kVariablePredicate) {
        VariablePredicate* variable_consequence =
            dynamic_cast<VariablePredicate*>(consequence_);
        is_consequence_satisfied = variable_consequence->VariableCompare(
            graph, (*left_path_instances)[i], (*right_path_instances)[j]);
      } else {
        LOG_FATAL("Unsupported predicate type.");
      }

      if (is_consequence_satisfied) {
        local_support++;
        local_gcr_instances.push_back(std::make_pair(
            std::list<PathInstanceID>{i}, std::list<PathInstanceID>{j}));
      }

      std::lock_guard<std::mutex> lock(mutex_);
      set_local_support(local_support);
      set_local_match(local_match);
      AddGCRInstancesToBack(local_gcr_instances);
    }
  }
}

}  // namespace xyz::graph::miniclean::data_structures::gcr