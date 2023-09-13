#ifndef GRAPH_SYSTEMS_MESSAGE_STORE_BASE_H
#define GRAPH_SYSTEMS_MESSAGE_STORE_BASE_H

namespace sics::graph::core::update_stores {

class UpdateStoreBase {
 public:
  virtual ~UpdateStoreBase() = default;
  virtual void Clear(){};

  virtual bool IsActive() { return false; };
};

}  // namespace sics::graph::core::update_stores

#endif  // GRAPH_SYSTEMS_MESSAGE_STORE_BASE_H
