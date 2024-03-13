#ifndef GRAPH_SYSTEMS_MESSAGE_STORE_BASE_H
#define GRAPH_SYSTEMS_MESSAGE_STORE_BASE_H

namespace sics::graph::core::update_stores {

class UpdateStoreBase {
 public:
  virtual ~UpdateStoreBase() = default;

  virtual bool IsActive() { return false; };

  // synchronize the data of global read and global write
  virtual void Sync(bool sync = false){};

  virtual size_t GetActiveCount() const { return 0; };

  virtual size_t GetMemorySize() const { return 0; }
};

}  // namespace sics::graph::core::update_stores

#endif  // GRAPH_SYSTEMS_MESSAGE_STORE_BASE_H
