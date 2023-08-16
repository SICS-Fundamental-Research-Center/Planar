#ifndef GRAPH_SYSTEMS_CORE_MESSAGE_STORES_BSP_MESSGAE_STORE_H_
#define GRAPH_SYSTEMS_CORE_MESSAGE_STORES_BSP_MESSGAE_STORE_H_

#include "common/types.h"
#include "message_stores/message_store_base.h"

namespace sics::graph::core::message_stores {

template <typename VertexData, typename EdgeData>
class BspMessageStore : public MessageStoreBase {
  VertexData* Read(common::GraphID gid) {
    if (gid >= message_count_) {
      return nullptr;
    }
    return read_data_ + gid;
  }

  bool Write(common::GraphID gid, const VertexData& vdata_new) {
    if (gid >= message_count_) {
      return false;
    }
    write_data_[gid] = vdata_new;
    return true;
  }

  // TODO
  void Clear() override {}

 private:
  VertexData* read_data_;
  VertexData* write_data_;
  common::VertexCount message_count_;
};

}  // namespace sics::graph::core::message_stores

#endif  // GRAPH_SYSTEMS_CORE_MESSAGE_STORES_BSP_MESSGAE_STORE_H_
