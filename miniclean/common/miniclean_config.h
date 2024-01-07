#ifndef MINICLEAN_COMMON_MINICLEAN_CONFIG_H_
#define MINICLEAN_COMMON_MINICLEAN_CONFIG_H_

#include <cstddef>

namespace sics::graph::miniclean::common {

class MiniCleanConfig {
 public:
  MiniCleanConfig(const MiniCleanConfig&) = delete;
  MiniCleanConfig& operator=(const MiniCleanConfig&) = delete;

  ~MiniCleanConfig() {
    if (instance_ != nullptr) {
      delete instance_;
      instance_ = nullptr;
    }
  }

  static const MiniCleanConfig* Get() {
    if (instance_ == nullptr) {
      instance_ = new MiniCleanConfig();
    }
    return instance_;
  }

  static MiniCleanConfig* GetMutable() {
    if (instance_ == nullptr) {
      instance_ = new MiniCleanConfig();
    }
    return instance_;
  }

 public:
  size_t memory_size_ = 64 * 1024;  // 64 * 1024 MB (i.e., 64 GB)

 private:
  MiniCleanConfig() = default;
  inline static MiniCleanConfig* instance_ = nullptr;
};
}  // namespace sics::graph::miniclean::common

#endif  // MINICLEAN_COMMON_MINICLEAN_CONFIG_H_