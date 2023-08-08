#ifndef GRAPH_SYSTEMS_PIE_APP_FACTORY_H
#define GRAPH_SYSTEMS_PIE_APP_FACTORY_H

#include <map>
#include <memory>

#include "apis/pie.h"
#include "util/logging.h"

namespace sics::graph::core::apis {

// An automatic factory that creates an app instance by its name.
template <typename GraphType>
class PIEAppFactory {
  static_assert(
      std::is_base_of<data_structures::Serializable, GraphType>::value,
      "GraphType must be a subclass of Serializable");

 public:
  using CreationMethod = std::unique_ptr<PIE<GraphType>> (*)();

 public:
  PIEAppFactory() = delete;

  static bool Register(const std::string& name, CreationMethod factory) {
        if (factories_.find(name) != factories_.end()) {
      return false;
    }
    factories_[name] = factory;
    return true;
  }

  static std::unique_ptr<PIE<GraphType>> Create(const std::string& name) {
    if (factories_.find(name) == factories_.end()) {
      LOGF_ERROR("AppFactory: app {} is not registered", name);
      return nullptr;
    }
    return factories_[name]();
  }

 private:
  static std::map<std::string, CreationMethod> factories_;
};

template <typename GraphType>
std::map<std::string, std::unique_ptr<PIE<GraphType>> (*)()>
    PIEAppFactory<GraphType>::factories_ = {};

}  // namespace sics::graph::core::apis

#endif  // GRAPH_SYSTEMS_PIE_APP_FACTORY_H
