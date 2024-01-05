#include "miniclean/components/error_detector/error_detector.h"

#include <yaml-cpp/yaml.h>

#include <vector>

namespace sics::graph::miniclean::components::error_detector {

void ErrorDetector::InitGCRSet() {
  const auto& gcrs = io_manager_->GetGCRs();
  gcr_index_.resize(gcrs.size());
  for (size_t i = 0; i < gcrs.size(); i++) {
    const auto& left_pattern_constraints =
        gcrs[i].get_left_pattern_constraints();
    const auto& right_pattern_constraints =
        gcrs[i].get_right_pattern_constraints();
    // Decompose the left pattern.
    for (size_t j = 0; j < left_pattern_constraints.size(); j++) {
      size_t left_path_id = GetAttributedPathID(left_pattern_constraints[j]);
      if (left_path_id == attributed_paths_.size()) {
        attributed_paths_.push_back(left_pattern_constraints[j]);
      }
      gcr_index_[i].left_path_ids.push_back(left_path_id);
    }
    // Decompose the right pattern.
    for (size_t j = 0; j < right_pattern_constraints.size(); j++) {
      size_t right_path_id = GetAttributedPathID(right_pattern_constraints[j]);
      if (right_path_id == attributed_paths_.size()) {
        attributed_paths_.push_back(right_pattern_constraints[j]);
      }
      gcr_index_[i].right_path_ids.push_back(right_path_id);
    }
  }
}

void ErrorDetector::LoadBasicComponents(const GraphID gid) {
  // TODO (bai-wenchao): load the avtive vertices and vertex index when they are
  // available.
  graph_ = io_manager_->NewSubgraph(gid);
}

void ErrorDetector::DischargePartialResults(
    const std::vector<ConstrainedStarInstance>& partial_results) {
  // TODO (bai-wenchao): discharge the partial results when they are available.
  io_manager_->ReleaseSubgraph(graph_->GetMetadata().gid);
}

size_t ErrorDetector::GetAttributedPathID(
    std::vector<AttributedVertex> attributed_path) {
  bool has_matched = false;
  for (size_t i = 0; i < attributed_paths_.size(); i++) {
    has_matched = true;
    // Check the path length.
    if (attributed_paths_[i].size() != attributed_path.size()) continue;
    for (size_t j = 0; j < attributed_paths_[i].size(); j++) {
      // Check the label ID.
      if (attributed_paths_[i][j].label_id != attributed_path[j].label_id) {
        has_matched = false;
        break;
      }
      // Check the attribute IDs.
      if (attributed_paths_[i][j].attribute_ids.size() !=
          attributed_path[j].attribute_ids.size()) {
        has_matched = false;
        break;
      }
      for (size_t k = 0; k < attributed_paths_[i][j].attribute_ids.size();
           k++) {
        if (attributed_paths_[i][j].attribute_ids[k] !=
            attributed_path[j].attribute_ids[k]) {
          has_matched = false;
          break;
        }
      }
      if (!has_matched) break;
      // Check the attribute values.
      for (size_t k = 0; k < attributed_paths_[i][j].attribute_values.size();
           k++) {
        if (attributed_paths_[i][j].attribute_values[k] !=
            attributed_path[j].attribute_values[k]) {
          has_matched = false;
          break;
        }
      }
      if (!has_matched) break;
      // Check the op types.
      for (size_t k = 0; k < attributed_paths_[i][j].op_types.size(); k++) {
        if (attributed_paths_[i][j].op_types[k] !=
            attributed_path[j].op_types[k]) {
          has_matched = false;
          break;
        }
      }
      if (!has_matched) break;
    }
    if (has_matched) return i;
  }
  // If no match is found, then add the path to the attributed paths.
  // The new ID of the path is the size of the attributed paths.
  return attributed_paths_.size();
}

}  // namespace sics::graph::miniclean::components::error_detector