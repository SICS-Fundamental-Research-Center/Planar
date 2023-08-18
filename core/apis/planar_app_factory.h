#ifndef GRAPH_SYSTEMS_PLANAR_APP_FACTORY_H
#define GRAPH_SYSTEMS_PLANAR_APP_FACTORY_H

#include <map>
#include <memory>

#include "apis/planar_app_base.h"
#include "data_structures/serializable.h"
#include "update_stores/bsp_update_store.h"
#include "util/logging.h"

namespace sics::graph::core::apis {

// An automatic factory that creates an app instance by its name.
template <typename GraphType>
class PlanarAppFactory {
  static_assert(
      std::is_base_of<data_structures::Serializable, GraphType>::value,
      "GraphType must be a subclass of Serializable");

 public:
  using VertexData = typename GraphType::VertexData;
  using EdgeData = typename GraphType::EdgeData;
  // TODO: add UpdateStore as a parameter.
  using CreationMethod = std::unique_ptr<PlanarAppBase<GraphType>> (*)(
      common::TaskRunner* runner,
      update_stores::BspUpdateStore<VertexData, EdgeData>* update_store,
      data_structures::Serializable* graph);

 public:
  // TODO: add UpdateStore as a parameter.
  explicit PlanarAppFactory(common::TaskRunner* runner) : runner_(runner) {}

  static bool Register(const std::string& name, CreationMethod factory) {
    if (factories_.find(name) != factories_.end()) {
      return false;
    }
    factories_[name] = factory;
    return true;
  }

  std::unique_ptr<PlanarAppBase<GraphType>> Create(
      const std::string& name,
      update_stores::BspUpdateStore<VertexData, EdgeData>* update_store,
      data_structures::Serializable* graph) {
    if (PlanarAppFactory<GraphType>::factories_.find(name) ==
        PlanarAppFactory<GraphType>::factories_.end()) {
      LOGF_ERROR("PlanarAppFactory: app {} is not registered", name);
      return nullptr;
    }
    return PlanarAppFactory<GraphType>::factories_[name](runner_, update_store,
                                                         graph);
  }

 private:
  static std::map<std::string, CreationMethod> factories_;

  common::TaskRunner* runner_;

  // TODO: add UpdateStore as a member here.
};

template <typename GraphType>
std::map<std::string, typename PlanarAppFactory<GraphType>::CreationMethod>
    PlanarAppFactory<GraphType>::factories_ = {};

}  // namespace sics::graph::core::apis

#endif  // GRAPH_SYSTEMS_PLANAR_APP_FACTORY_H
