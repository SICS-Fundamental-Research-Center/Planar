#ifndef MINICLEAN_COMMON_ERROR_DETECTION_CONFIG_H_
#define MINICLEAN_COMMON_ERROR_DETECTION_CONFIG_H_

#include <map>
#include <vector>

#include "core/util/logging.h"
#include "miniclean/common/types.h"
#include "miniclean/data_structures/graphs/miniclean_graph_metadata.h"

namespace sics::graph::miniclean::common {

class ErrorDetectionConfig {
 private:
  using MiniCleanGraphMetadata = sics::graph::miniclean::data_structures::graphs::
      MiniCleanGraphMetadata;
      
 public:
  ErrorDetectionConfig(const ErrorDetectionConfig&) = delete;
  ErrorDetectionConfig& operator=(const ErrorDetectionConfig&) = delete;

  ~ErrorDetectionConfig() {
    if (instance_ != nullptr) {
      delete instance_;
      instance_ = nullptr;
    }
  }

  static void Init(const MiniCleanGraphMetadata& metadata) {
    if (instance_ == nullptr) {
      instance_ = new ErrorDetectionConfig();
    }
    label_id_to_label_name_ = metadata.label_id_to_label_name;
    label_name_to_label_id_ = metadata.label_name_to_label_id;
    attr_id_to_attr_name_ = metadata.attr_id_to_attr_name;
    attr_name_to_attr_id_ = metadata.attr_name_to_attr_id;
  }

  static const ErrorDetectionConfig* Get() {
    if (instance_ == nullptr) {
      LOG_FATAL("ErrorDetectionConfig not initialized.");
    }
    return instance_;
  }

  static ErrorDetectionConfig* GetMutable() {
    if (instance_ == nullptr) {
      LOG_FATAL("ErrorDetectionConfig not initialized.");
    }
    return instance_;
  }

  const std::string& GetLabelNameByID(uint8_t label_id) const {
    if (label_id < 0 || label_id >= label_id_to_label_name_.size()) {
      LOGF_FATAL("Label id {} out of bound: [{}, {})", label_id, 0,
                 label_id_to_label_name_.size());
    }
    return label_id_to_label_name_[label_id];
  }

  VertexLabel GetLabelIDByName(const std::string& label_name) const {
    if (label_name_to_label_id_.find(label_name) ==
        label_name_to_label_id_.end()) {
      LOGF_FATAL("Label name {} not found.", label_name);
    }
    return label_name_to_label_id_.at(label_name);
  }

  const std::string& GetAttrNameByID(uint8_t attr_id) const {
    if (attr_id < 0 || attr_id >= attr_id_to_attr_name_.size()) {
      LOGF_FATAL("Attribute ID {} out of bound: [{}, {})", attr_id, 0,
                 attr_id_to_attr_name_.size());
    }
    return attr_id_to_attr_name_[attr_id];
  }

  VertexAttributeID GetAttrIDByName(const std::string& attr_name) const {
    if (attr_name_to_attr_id_.find(attr_name) == attr_name_to_attr_id_.end()) {
      LOGF_FATAL("Attribute name {} not found.", attr_name);
    }
    return attr_name_to_attr_id_.at(attr_name);
  }

 private:
  ErrorDetectionConfig() = default;
  inline static ErrorDetectionConfig* instance_ = nullptr;
  inline static std::vector<std::string> label_id_to_label_name_;
  inline static std::map<std::string, VertexLabel> label_name_to_label_id_;
  inline static std::vector<std::string> attr_id_to_attr_name_;
  inline static std::map<std::string, VertexAttributeID> attr_name_to_attr_id_;
};

}  // namespace sics::graph::miniclean::common

#endif  // MINICLEAN_COMMON_ERROR_DETECTION_CONFIG_H_