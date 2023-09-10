#ifndef GRAPH_SYSTEMS_CORE_MESSAGE_STORES_BSP_UPDATE_STORE_H_
#define GRAPH_SYSTEMS_CORE_MESSAGE_STORES_BSP_UPDATE_STORE_H_

#include <fstream>

#include "common/bitmap.h"
#include "common/bitmap_no_ownership.h"
#include "common/types.h"
#include "update_stores/update_store_base.h"
#include "util/atomic.h"
#include "util/logging.h"

namespace sics::graph::core::update_stores {

template <typename VertexData, typename EdgeData>
class BspUpdateStore : public UpdateStoreBase {
 public:
  BspUpdateStore() = default;
  explicit BspUpdateStore(const std::string& root_path,
                          common::VertexCount vertex_num)
      : message_count_(vertex_num) {
    read_data_ = new VertexData[message_count_];
    write_data_ = new VertexData[message_count_];
    ReadActiveVertexBitmap(root_path);
  }

  ~BspUpdateStore() {
    delete[] read_data_;
    delete[] write_data_;
  }

  // used for basic unsigned type
  VertexData Read(common::GraphID gid) {
    if (gid >= message_count_) {
      LOG_FATAL("Read out of bound");
    }
    return read_data_[gid];
  }

  bool Write(common::GraphID gid, VertexData vdata_new) {
    if (gid >= message_count_) {
      return false;
    }
    util::atomic::WriteMin(write_data_ + gid, vdata_new);
    return true;
  }

  // TODO
  void Clear() override {}

 private:
  void ReadActiveVertexBitmap(const std::string& root_path) {
    std::ifstream file(root_path + "bitmap/global_is_border_vertices.bin",
                       std::ios::binary);
    if (!file.is_open()) {
      LOG_FATAL("Cannot open file: %s",
                (root_path + "bitmap/global_is_border_vertices.bin").c_str());
    }
    file.seekg(0, std::ios::end);
    size_t file_size = file.tellg();
    size_t size = file_size / sizeof(uint64_t);
    file.seekg(0, std::ios::beg);
    uint64_t* buffer = new uint64_t[size];
    // TODO: bitmap assignment
    active_vertex_bitmap_.Init(size, buffer);
    file.close();
  }

 private:
  VertexData* read_data_;
  VertexData* write_data_;
  common::VertexCount message_count_;

  common::BitmapNoOwnerShip active_vertex_bitmap_;
};

}  // namespace sics::graph::core::update_stores

#endif  // GRAPH_SYSTEMS_CORE_MESSAGE_STORES_BSP_UPDATE_STORE_H_
