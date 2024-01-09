#ifndef GRAPH_SYSTEMS_NVME_UPDATE_STORES_NVME_UPDATE_STORE_H_
#define GRAPH_SYSTEMS_NVME_UPDATE_STORES_NVME_UPDATE_STORE_H_

#include <fstream>

#include "core/common/bitmap.h"
#include "core/common/bitmap_no_ownership.h"
#include "core/common/config.h"
#include "core/common/types.h"
#include "core/update_stores/update_store_base.h"
#include "core/util/atomic.h"
#include "core/util/logging.h"

namespace sics::graph::nvme::update_stores {

using namespace sics::graph::core;

template <typename VertexData, typename EdgeData>
class PramNvmeUpdateStore : public core::update_stores::UpdateStoreBase {
  using GraphID = common::GraphID;
  using VertexID = common::VertexID;

 public:
  PramNvmeUpdateStore()
      : read_data_(nullptr), write_data_(nullptr), message_count_(0) {}
  explicit PramNvmeUpdateStore(common::VertexCount vertex_num)
      : message_count_(vertex_num) {
    application_type_ = common::Configurations::Get()->application;
    no_data_need_ = common::Configurations::Get()->no_data_need;
    is_block_mode_ = common::Configurations::Get()->is_block_mode;

    if (!no_data_need_) {
      read_data_ = new VertexData[message_count_];
      write_data_ = new VertexData[message_count_];
      switch (application_type_) {
        case common::ApplicationType::WCC:
        case common::ApplicationType::MST: {
          for (uint32_t i = 0; i < vertex_num; i++) {
            read_data_[i] = i;
            write_data_[i] = i;
          }
          break;
        }
        case common::ApplicationType::Coloring: {
          for (uint32_t i = 0; i < vertex_num; i++) {
            read_data_[i] = 0;
            write_data_[i] = 0;
          }
          break;
        }
        case common::ApplicationType::Sssp: {
          for (uint32_t i = 0; i < vertex_num; i++) {
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

    InitMemorySizeOfBlock();
  }

  ~PramNvmeUpdateStore() {
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
    write_data_[vid] = vdata_new;
    return true;
  }

  bool WriteMin(VertexID id, VertexData new_data) {
    if (id >= message_count_) {
      return false;
    }
    return util::atomic::WriteMin(write_data_ + id, new_data);
  }

  bool WriteMax(VertexID vid, VertexData new_data) {
    if (vid >= message_count_) {
      return false;
    }
    return util::atomic::WriteMax(write_data_ + vid, new_data);
  }

  bool IsActive() override { return active_count_ != 0; }
  void SetActive() { active_count_ = 1; }
  void UnsetActive() { active_count_ = 0; }

  void Sync() override {
    if (!no_data_need_) {
      memcpy(read_data_, write_data_, message_count_ * sizeof(VertexData));
      active_count_ = 0;
    }
  }

  uint32_t GetMessageCount() { return message_count_; }

  void SetMessageCount(uint32_t message_count) {
    message_count_ = message_count;
    InitMemorySizeOfBlock();
  }

  void LogGlobalMessage() {
    LOG_INFO("Global message info:");
    for (size_t i = 0; i < message_count_; i++) {
      LOGF_INFO("global message: id({}) -> read: {} write: {}", i,
                read_data_[i], write_data_[i]);
    }
  }

  void LogAllMessage() {
    LOG_INFO("All Global message info:");
    for (size_t i = 0; i < message_count_; i++) {
      LOGF_INFO("global message: id({}) -> read: {} write: {}", i,
                read_data_[i], write_data_[i]);
    }
  }

  size_t GetActiveCount() const override { return active_count_; }

  size_t GetMemorySize() const override { return memory_size_; }

 private:
  void InitMemorySizeOfBlock() {
    auto global_messgeage_size =
        (sizeof(VertexData) * message_count_ * 2) >> 20;
    memory_size_ = global_messgeage_size + 1;
  }

 private:
  VertexData* read_data_;
  VertexData* write_data_;
  common::VertexCount message_count_;

  size_t active_count_ = 0;

  size_t memory_size_ = 0;

  // configs
  common::ApplicationType application_type_;
  bool no_data_need_;
  bool is_block_mode_;
};

typedef PramNvmeUpdateStore<common::Uint32VertexDataType,
                            common::DefaultEdgeDataType>
    BspUpdateStoreUInt32;
typedef PramNvmeUpdateStore<common::Uint16VertexDataType,
                            common::DefaultEdgeDataType>
    BspUpdateStoreUInt16;

}  // namespace sics::graph::nvme::update_stores

#endif  // GRAPH_SYSTEMS_NVME_UPDATE_STORES_NVME_UPDATE_STORE_H_
