#ifndef GRAPH_SYSTEMS_MESSAGE_STORE_BASE_H
#define GRAPH_SYSTEMS_MESSAGE_STORE_BASE_H

namespace sics::graph::core::update_stores {

class UpdateStoreBase {
 public:
  virtual void Clear() = 0;
};

}  // namespace sics::graph::core::update_stores

#endif  // GRAPH_SYSTEMS_MESSAGE_STORE_BASE_H
