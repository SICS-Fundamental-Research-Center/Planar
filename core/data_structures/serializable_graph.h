#ifndef GRAPH_SYSTEMS_SERIALIZABLE_GRAPH_H
#define GRAPH_SYSTEMS_SERIALIZABLE_GRAPH_H

#include "data_structures/serializable.h"
#include <memory>

namespace sics::graph::core::data_structures {

template <typename GID_T, typename VID_T>
class SerializableGraph : public Serializable {
 public:
  explicit Graph(GID_T gid) : gid_(gid) {}
  explicit Graph() = default;
  virtual ~Graph() = default;

  virtual std::unique_ptr<Serialized> Serialize(common::TaskRunner& runner) = 0;
  virtual void Deserialize(common::TaskRunner& runner, const Metadata& metadata,
                           Serialized&& serialized) override = 0;

  inline GID_T get_gid() const { return gid_; }
  inline void set_gid(GID_T gid) { gid_ = gid; }

 protected:
  GID_T gid_ = -1;
  VID_T* buf_graph_ = nullptr;
};

}  // namespace sics::graph::core::data_structures

#endif  // GRAPH_SYSTEMS_SERIALIABLE_GRAPH_H