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
  using GraphID = common::GraphID;
  using VertexID = common::VertexID;

 public:
  BspUpdateStore() = default;
  explicit BspUpdateStore(const std::string& root_path,
                          common::VertexCount vertex_num)
      : message_count_(vertex_num) {
    read_data_ = new VertexData[message_count_];
    write_data_ = new VertexData[message_count_];
    for (int i = 0; i < vertex_num; i++) {
      read_data_[i] = i;
      write_data_[i] = i;
    }
    ReadBorderVertexBitmap(root_path);
  }

  ~BspUpdateStore() {
    delete[] read_data_;
    delete[] write_data_;
  }

  // used for basic unsigned type
  VertexData Read(VertexID vid) {
    if (vid >= message_count_) {
      LOG_FATAL("Read out of bound");
    }
    return read_data_[vid];
  }

  bool Write(VertexID vid, VertexData vdata_new) {
    if (vid >= message_count_) {
      return false;
    }
    if (border_vertex_bitmap_.GetBit(vid)) {
      write_data_[vid] = vdata_new;
      active++;
    }
    return true;
  }

  bool WriteMin(VertexID vid, VertexData vdata_new) {
    if (vid >= message_count_) {
      return false;
    }
    if (border_vertex_bitmap_.GetBit(vid)) {
      if (util::atomic::WriteMin(write_data_ + vid, vdata_new)) {
        active++;
        return true;
      }
    }
    return false;
  }

  bool IsActive() override { return active != 0; }

  // Now is useless.
  void Clear() override {}

 private:
  void ReadBorderVertexBitmap(const std::string& root_path) {
    std::ifstream file(root_path + "bitmap/border_vertices.bin",
                       std::ios::binary);
    if (!file.is_open()) {
      LOG_FATAL("Cannot open file: %s",
                (root_path + "bitmap/border_vertices.bin").c_str());
    }
    file.seekg(0, std::ios::end);
    size_t file_size = file.tellg();
    file.seekg(0, std::ios::beg);

    size_t buffer_size = (message_count_ >> 6) + 1;
    uint64_t* buffer = new uint64_t[buffer_size];
    file.read((char*)(buffer), file_size);

    border_vertex_bitmap_.Init(message_count_, buffer);
    file.close();
  }

 private:
  VertexData* read_data_;
  VertexData* write_data_;
  common::VertexCount message_count_;

  common::Bitmap border_vertex_bitmap_;

  int active = 0;
};

typedef BspUpdateStore<common::Uint32VertexDataType,
                       common::DefaultEdgeDataType>
    BspUpdateStoreUInt32;
typedef BspUpdateStore<common::Uint16VertexDataType,
                       common::DefaultEdgeDataType>
    BspUpdateStoreUInt16;

}  // namespace sics::graph::core::update_stores

#endif  // GRAPH_SYSTEMS_CORE_MESSAGE_STORES_BSP_UPDATE_STORE_H_
