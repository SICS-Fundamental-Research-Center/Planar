#include "miniclean/components/error_detector/error_detector.h"

#include <yaml-cpp/yaml.h>

namespace sics::graph::miniclean::components::error_detector {

void ErrorDetector::InitGCRSet() {
  YAML::Node gcrs_node;
  try {
    gcrs_node = YAML::LoadFile(gcr_path_);
  } catch (YAML::BadFile& e) {
    LOG_FATAL("gcrs.yaml file read failed! ", e.msg);
  }
  gcrs_ = gcrs_node["GCRs"].as<std::vector<GCR>>();
  gcr_path_pattern_collections_.resize(gcrs_.size());
  for (size_t i = 0; i < gcrs_.size(); i++) {
    const auto& left_pattern_constraints =
        gcrs_[i].get_left_pattern_constraints();
    const auto& right_pattern_constraints =
        gcrs_[i].get_right_pattern_constraints();
    // Decompose the left pattern.
    for (size_t j = 0; j < left_pattern_constraints.size(); j++) {
      size_t left_path_id = GetAttributedPathID(left_pattern_constraints[j]);
      if (left_path_id == attributed_path_patterns_.size()) {
        attributed_path_patterns_.push_back(left_pattern_constraints[j]);
      }
      gcr_path_pattern_collections_[i].left_path_pattern_ids.push_back(
          left_path_id);
    }
    // Decompose the right pattern.
    for (size_t j = 0; j < right_pattern_constraints.size(); j++) {
      size_t right_path_id = GetAttributedPathID(right_pattern_constraints[j]);
      if (right_path_id == attributed_path_patterns_.size()) {
        attributed_path_patterns_.push_back(right_pattern_constraints[j]);
      }
      gcr_path_pattern_collections_[i].right_path_pattern_ids.push_back(
          right_path_id);
    }
  }
}

void ErrorDetector::BuildPathIndexForVertices() {
  TaskPackage task_package;
  task_package.reserve(parallelism_);
  vid_to_path_pattern_id_.resize(graph_->GetNumVertices());
  for (unsigned int i = 0; i < parallelism_; i++) {
    auto task = [this, i]() {
      for (VertexID j = i; j < graph_->GetNumVertices(); j += parallelism_) {
        std::vector<size_t> path_pattern_ids;
        for (size_t k = 0; k < attributed_path_patterns_.size(); k++) {
          if (MatchPathForVertex(j, 0, attributed_path_patterns_[k])) {
            path_pattern_ids.push_back(k);
          }
        }
        {
          std::lock_guard<std::mutex> lock(path_indexing_mtx_);
          vid_to_path_pattern_id_[j] = path_pattern_ids;
        }
      }
    };
    task_package.push_back(task);
  }
  thread_pool_.SubmitSync(task_package);
  task_package.clear();
}

bool ErrorDetector::MatchPathForVertex(
    VertexID local_vid, size_t match_position,
    const std::vector<AttributedVertex>& attributed_path) {
  if (match_position == attributed_path.size()) return true;
  // Check the labels.
  if (graph_->GetVertexLabel(local_vid) !=
      attributed_path.at(match_position).label_id)
    return false;
  // Check the attributes.
  for (size_t i = 0;
       i < attributed_path.at(match_position).attribute_ids.size(); i++) {
    std::string current_str_val((char*)graph_->GetVertexAttributePtr(
        local_vid, attributed_path.at(match_position).attribute_ids[i]));
    std::string pattern_str_val =
        attributed_path.at(match_position).attribute_values[i];
    VertexAttributeType vattr_type =
        graph_->GetVertexAttributeTypeByAttributeID(
            attributed_path.at(match_position).attribute_ids[i]);
    OpType op_type = attributed_path.at(match_position).op_types[i];
    if (!IsPredicateSatisfied(vattr_type, op_type, current_str_val,
                              pattern_str_val)) {
      return false;
    }
  }
  // Check the next vertex.
  VertexID* next_local_vids = graph_->GetOutgoingLocalVIDsByLocalID(local_vid);
  for (size_t i = 0; i < graph_->GetOutDegreeByLocalID(local_vid); i++) {
    if (MatchPathForVertex(next_local_vids[i], match_position + 1,
                           attributed_path)) {
      return true;
    }
  }
  return false;
}

bool ErrorDetector::IsPredicateSatisfied(VertexAttributeType vattr_type,
                                         OpType op_type, const std::string& rhs,
                                         const std::string& lhs) {
  switch (vattr_type) {
    case sics::graph::miniclean::data_structures::graphs::kUInt8: {
      switch (op_type) {
        case sics::graph::miniclean::data_structures::gcr::kEq:
          return std::stoi(lhs) == std::stoi(rhs);
        case sics::graph::miniclean::data_structures::gcr::kGt:
          return std::stoi(lhs) > std::stoi(rhs);
        default:
          LOGF_FATAL("Unsupported op type: {}", int(op_type));
          break;
      }
    }
    case sics::graph::miniclean::data_structures::graphs::kUInt16: {
      switch (op_type) {
        case sics::graph::miniclean::data_structures::gcr::kEq:
          return std::stoi(lhs) == std::stoi(rhs);
        case sics::graph::miniclean::data_structures::gcr::kGt:
          return std::stoi(lhs) > std::stoi(rhs);
        default:
          LOGF_FATAL("Unsupported op type: {}", int(op_type));
          break;
      }
    }
    case sics::graph::miniclean::data_structures::graphs::kUInt32: {
      switch (op_type) {
        case sics::graph::miniclean::data_structures::gcr::kEq:
          return std::stol(lhs) == std::stol(rhs);
        case sics::graph::miniclean::data_structures::gcr::kGt:
          return std::stol(lhs) > std::stol(rhs);
        default:
          LOGF_FATAL("Unsupported op type: {}", int(op_type));
          break;
      }
    }
    case sics::graph::miniclean::data_structures::graphs::kUInt64: {
      switch (op_type) {
        case sics::graph::miniclean::data_structures::gcr::kEq:
          return std::stoull(lhs) == std::stoull(rhs);
        case sics::graph::miniclean::data_structures::gcr::kGt:
          return std::stoull(lhs) > std::stoull(rhs);
        default:
          LOGF_FATAL("Unsupported op type: {}", int(op_type));
          break;
      }
    }
    case sics::graph::miniclean::data_structures::graphs::kString: {
      switch (op_type) {
        case sics::graph::miniclean::data_structures::gcr::kEq:
          return lhs == rhs;
        default:
          LOGF_FATAL("Unsupported op type: {}", int(op_type));
          break;
      }
    }
    default:
      LOGF_FATAL("Unsupported vertex attribute type: {}", int(vattr_type));
      break;
  }
}

size_t ErrorDetector::GetAttributedPathID(
    std::vector<AttributedVertex> attributed_path) {
  bool has_matched = false;
  for (size_t i = 0; i < attributed_path_patterns_.size(); i++) {
    has_matched = true;
    // Check the path length.
    if (attributed_path_patterns_[i].size() != attributed_path.size()) continue;
    for (size_t j = 0; j < attributed_path_patterns_[i].size(); j++) {
      // Check the label ID.
      if (attributed_path_patterns_[i][j].label_id !=
          attributed_path[j].label_id) {
        has_matched = false;
        break;
      }
      // Check the attribute IDs.
      if (attributed_path_patterns_[i][j].attribute_ids.size() !=
          attributed_path[j].attribute_ids.size()) {
        has_matched = false;
        break;
      }
      for (size_t k = 0;
           k < attributed_path_patterns_[i][j].attribute_ids.size(); k++) {
        if (attributed_path_patterns_[i][j].attribute_ids[k] !=
            attributed_path[j].attribute_ids[k]) {
          has_matched = false;
          break;
        }
      }
      if (!has_matched) break;
      // Check the attribute values.
      for (size_t k = 0;
           k < attributed_path_patterns_[i][j].attribute_values.size(); k++) {
        if (attributed_path_patterns_[i][j].attribute_values[k] !=
            attributed_path[j].attribute_values[k]) {
          has_matched = false;
          break;
        }
      }
      if (!has_matched) break;
      // Check the op types.
      for (size_t k = 0; k < attributed_path_patterns_[i][j].op_types.size();
           k++) {
        if (attributed_path_patterns_[i][j].op_types[k] !=
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
  return attributed_path_patterns_.size();
}

}  // namespace sics::graph::miniclean::components::error_detector