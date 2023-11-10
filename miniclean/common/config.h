#ifndef GRAPH_SYSTEMS_MINICLEAN_COMMON_CONFIG_H_
#define GRAPH_SYSTEMS_MINICLEAN_COMMON_CONFIG_H_

#include <cstddef>
#include <string>

namespace sics::graph::miniclean::common {

class Configurations {
 public:
  static const Configurations* Get() {
    if (instance_ == nullptr) {
      instance_ = new Configurations();
    }
    return instance_;
  }

  static Configurations* GetMutable() {
    if (instance_ == nullptr) {
      instance_ = new Configurations();
    }
    return instance_;
  }

  size_t max_predicate_num_ = 3;
  size_t max_path_num_ = 4;
  size_t star_support_threshold_ = 700;
  size_t support_threshold_ = 5000;
  float confidence_threshold_ = 0.90;
  float min_confidence_ = 0.1;

  size_t max_variable_predicate_num_ = 1;
  // If we cannot mine a gcr in 600 seconds, we will give up.
  size_t max_freeze_time = 600;

  std::string rule_discovery_log_path = "data/imdb/log/rule_discovery";

 private:
  Configurations() = default;
  inline static Configurations* instance_ = nullptr;
};
}  // namespace sics::graph::miniclean::common

#endif  // GRAPH_SYSTEMS_MINICLEAN_COMMON_CONFIG_H_
