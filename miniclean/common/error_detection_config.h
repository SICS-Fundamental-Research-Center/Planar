#ifndef MINICLEAN_COMMON_ERROR_DETECTION_CONFIG_H_
#define MINICLEAN_COMMON_ERROR_DETECTION_CONFIG_H_

#include <map>
#include <vector>

#include "miniclean/common/types.h"

namespace sics::graph::miniclean::common {

class ErrorDetectionConfig {
 public:
  ErrorDetectionConfig(const ErrorDetectionConfig&) = delete;
  ErrorDetectionConfig& operator=(const ErrorDetectionConfig&) = delete;

  ~ErrorDetectionConfig() {
    if (instance_ != nullptr) {
      delete instance_;
      instance_ = nullptr;
    }
  }

  static const ErrorDetectionConfig* Get() {
    if (instance_ == nullptr) {
      instance_ = new ErrorDetectionConfig();
    }
    return instance_;
  }

  static ErrorDetectionConfig* GetMutable() {
    if (instance_ == nullptr) {
      instance_ = new ErrorDetectionConfig();
    }
    return instance_;
  }

  const std::string& GetAttrNameByID(uint8_t attr_id) const {
    return attr_id_to_attr_name_[attr_id];
  }

  VertexAttributeID GetAttrIDByName(const std::string& attr_name) const {
    return attr_name_to_attr_id_.at(attr_name);
  }

 private:
  ErrorDetectionConfig() = default;
  inline static ErrorDetectionConfig* instance_ = nullptr;

  std::vector<std::string> attr_id_to_attr_name_ = {
      "movie_vid",  "movie_rating", "movie_year",         "movie_genre",
      "movie_name", "movie_title",  "movie_episode_name", "movie_episode_id",
      "cast_vid",   "cast_name",    "director_vid",       "director_name"};

  std::map<std::string, VertexAttributeID> attr_name_to_attr_id_ = {
      {"movie_vid", 0},          {"movie_rating", 1},
      {"movie_year", 2},         {"movie_genre", 3},
      {"movie_name", 4},         {"movie_title", 5},
      {"movie_episode_name", 6}, {"movie_episode_id", 7},
      {"cast_vid", 8},           {"cast_name", 9},
      {"director_vid", 10},      {"director_name", 11}};
};

}  // namespace sics::graph::miniclean::common

#endif  // MINICLEAN_COMMON_ERROR_DETECTION_CONFIG_H_