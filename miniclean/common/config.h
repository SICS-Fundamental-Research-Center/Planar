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
  size_t max_path_num_ = 2;
  size_t star_support_threshold_ = 1000;
  size_t support_threshold_ = 20000;
  float confidence_threshold_ = 0.90;
  float min_confidence_ = 0.1;

  size_t max_variable_predicate_num_ = 1;
  // If the number of the pending tasks is less than `min_pending_tasks`, we
  // will package the subtasks and submit the task package to the thread pool.
  size_t min_pending_tasks_ = 20;

  std::string rule_discovery_log_path = "data/small_imdb/log/rule_discovery";

 private:
  Configurations() = default;
  inline static Configurations* instance_ = nullptr;
};
}  // namespace sics::graph::miniclean::common

#endif  // GRAPH_SYSTEMS_MINICLEAN_COMMON_CONFIG_H_
