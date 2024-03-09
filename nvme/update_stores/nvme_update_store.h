#ifndef GRAPH_SYSTEMS_NVME_UPDATE_STORES_NVME_UPDATE_STORE_H_
#define GRAPH_SYSTEMS_NVME_UPDATE_STORES_NVME_UPDATE_STORE_H_

#include <fstream>

#include "core/common/bitmap.h"
#include "core/common/bitmap_no_ownership.h"
#include "core/common/config.h"
#include "core/common/types.h"
#include "core/data_structures/graph_metadata.h"
#include "core/update_stores/update_store_base.h"
#include "core/util/atomic.h"
#include "core/util/logging.h"

namespace sics::graph::nvme::update_stores {

template <typename VertexData, typename EdgeData>
class PramNvmeUpdateStore : public core::update_stores::UpdateStoreBase {
  using GraphID = core::common::GraphID;
  using VertexID = core::common::VertexID;

 public:
  PramNvmeUpdateStore()
      : read_data_(nullptr), write_data_(nullptr), vertex_count_(0) {}
  explicit PramNvmeUpdateStore(
      const core::data_structures::GraphMetadata& graph_metadata)
      : graph_metadata_(graph_metadata),
        vertex_count_(graph_metadata.get_num_vertices()),
        edges_count_(graph_metadata.get_num_edges()) {
    application_type_ = core::common::Configurations::Get()->application;
    no_data_need_ = core::common::Configurations::Get()->no_data_need;
    is_block_mode_ = core::common::Configurations::Get()->is_block_mode;

    if (!no_data_need_) {
      read_data_ = new VertexData[vertex_count_];
      write_data_ = new VertexData[vertex_count_];
      switch (application_type_) {
        case core::common::ApplicationType::WCC:
        case core::common::ApplicationType::MST: {
          for (uint32_t i = 0; i < vertex_count_; i++) {
            read_data_[i] = i;
            write_data_[i] = i;
          }
          edge_delete_map_.Init(edges_count_);
          break;
        }
        case core::common::ApplicationType::Coloring: {
          for (uint32_t i = 0; i < vertex_count_; i++) {
            read_data_[i] = 0;
            write_data_[i] = 0;
          }
          break;
        }
        case core::common::ApplicationType::Sssp: {
          for (uint32_t i = 0; i < vertex_count_; i++) {
            read_data_[i] = std::numeric_limits<VertexData>::max();
            write_data_[i] = std::numeric_limits<VertexData>::max();
          }
          break;
        }
        default: {
          LOG_FATAL("Application type not supported");
          break;
        }
      }
    }

    // Init del bitmaps
    for (uint32_t i = 0; i < graph_metadata_.get_num_blocks(); i++) {
      auto block = graph_metadata_.GetBlockMetadata(i);
      edge_delete_map_.Init(block.num_outgoing_edges);
    }

    InitMemorySizeOfBlock();
    //    InitDeleteBitmaps();
  }

  ~PramNvmeUpdateStore() {
    if (read_data_) {
      delete[] read_data_;
    }
    if (write_data_) {
      delete[] write_data_;
    }
  }

  // used for basic unsigned type
  VertexData Read(VertexID vid) { return read_data_[vid]; }

  bool Write(VertexID vid, VertexData vdata_new) {
    write_data_[vid] = vdata_new;
    return true;
  }

  bool WriteMin(VertexID id, VertexData new_data) {
    return core::util::atomic::WriteMin(write_data_ + id, new_data);
  }

  bool WriteMax(VertexID vid, VertexData new_data) {
    return core::util::atomic::WriteMax(write_data_ + vid, new_data);
  }

  bool DeleteEdge(core::common::EdgeIndex eid) {
    if (eid > edges_count_) {
      return false;
    }
    edge_delete_map_.SetBit(eid);
    return true;
  }

  bool IsActive() override { return active_count_ != 0; }
  void SetActive() { active_count_ = 1; }
  void UnsetActive() { active_count_ = 0; }

  void Sync() override {
    if (!no_data_need_) {
      memcpy(read_data_, write_data_, vertex_count_ * sizeof(VertexData));
      active_count_ = 0;
    }
  }

  uint32_t GetMessageCount() { return vertex_count_; }

  void SetMessageCount(uint32_t message_count) {
    vertex_count_ = message_count;
    InitMemorySizeOfBlock();
  }

  void LogVertexData() {
    LOG_INFO("VertexData info:");
    for (size_t i = 0; i < vertex_count_; i++) {
      LOGF_INFO("VertexData: id({}) -> read: {} write: {}", i, read_data_[i],
                write_data_[i]);
    }
  }

  void LogEdgeDelInfo() {
    LOG_INFO("Delete edges info:");
    std::stringstream edges_del;
    auto index = 0;
    for (size_t i = 0; i < graph_metadata_.get_num_blocks(); i++) {
      auto block = graph_metadata_.GetBlockMetadata(i);
      for (size_t j = 0; j < block.num_outgoing_edges; j++) {
        if (!edge_delete_map_.GetBit(index)) {
          edges_del << index << " ";
        } else {
          edges_del << "x ";
        }
        index++;
      }
      edges_del << std::endl;
    }
    LOGF_INFO("{}", edges_del.str());
  }

  size_t GetActiveCount() const override { return active_count_; }

  size_t GetMemorySize() const override { return memory_size_; }

  core::common::EdgeIndex GetLeftEdges() const {
    //    return edges_count_ - edge_delete_map_.Count();
    return graph_metadata_.get_num_edges();
  }

  //  const core::common::Bitmap* GetDeleteBitmap(size_t block_id) const {
  //    return &delete_bitmaps_[block_id];
  //  }

  const core::common::Bitmap* GetDeleteBitmap() const {
    return &edge_delete_map_;
  }

 private:
  void InitMemorySizeOfBlock() {
    auto global_messgeage_size = (sizeof(VertexData) * vertex_count_ * 2) >> 20;
    auto delete_map_size = edge_delete_map_.size() >> 23;
    memory_size_ = global_messgeage_size + delete_map_size + 1;
  }

  //  void InitDeleteBitmaps() {
  //    edge_del_bitmaps_.resize(graph_metadata_.get_num_blocks());
  //    for (size_t i = 0; i < graph_metadata_.get_num_blocks(); i++) {
  //      edge_del_bitmaps_[i].Init(graph_metadata_.GetBlockNumEdges(i));
  //    }
  //  }

 private:
  const core::data_structures::GraphMetadata& graph_metadata_;

  VertexData* read_data_;
  VertexData* write_data_;
  core::common::VertexCount vertex_count_;
  core::common::EdgeIndex edges_count_;

  core::common::Bitmap edge_delete_map_;
  //  std::vector<core::common::Bitmap> edge_del_bitmaps_;

  size_t active_count_ = 0;

  size_t memory_size_ = 0;

  // configs
  core::common::ApplicationType application_type_;
  bool no_data_need_;
  bool is_block_mode_;

  //  std::vector<core::common::Bitmap> delete_bitmaps_;
};

typedef PramNvmeUpdateStore<core::common::Uint32VertexDataType,
                            core::common::DefaultEdgeDataType>
    PramUpdateStoreUInt32;
typedef PramNvmeUpdateStore<core::common::Uint16VertexDataType,
                            core::common::DefaultEdgeDataType>
    PramUpdateStoreUint16;

}  // namespace sics::graph::nvme::update_stores

#endif  // GRAPH_SYSTEMS_NVME_UPDATE_STORES_NVME_UPDATE_STORE_H_
