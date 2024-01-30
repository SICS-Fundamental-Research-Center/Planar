#ifndef GRAPH_SYSTEMS_NVME_DATA_STRUCTURES_GRAPH_PRAM_BLOCK_H_
#define GRAPH_SYSTEMS_NVME_DATA_STRUCTURES_GRAPH_PRAM_BLOCK_H_

#include <memory>

#include "core/common/bitmap.h"
#include "core/common/bitmap_no_ownership.h"
#include "core/common/config.h"
#include "core/common/types.h"
#include "core/data_structures/graph_metadata.h"
#include "core/data_structures/serializable.h"
#include "core/data_structures/serialized.h"
#include "core/util/atomic.h"
#include "core/util/pointer_cast.h"
#include "nvme/data_structures/graph/serialized_pram_block_csr.h"
#include "nvme/update_stores/nvme_update_store.h"

namespace sics::graph::nvme::data_structures::graph {

using BlockMetadata = core::data_structures::BlockMetadata;
using Serialized = core::data_structures::Serialized;
using Serializable = core::data_structures::Serializable;

template <typename TV, typename TE>
class PramBlock : public core::data_structures::Serializable {
  using GraphID = core::common::GraphID;
  using BlockID = core::common::BlockID;
  using VertexID = core::common::VertexID;
  using VertexIndex = core::common::VertexIndex;
  using EdgeIndex = core::common::EdgeIndex;
  using VertexDegree = core::common::VertexDegree;
  using SerializedPramBlockCSRGraph =
      data_structures::graph::SerializedPramBlockCSRGraph;

 public:
  using VertexData = TV;
  using EdgeData = TE;
  PramBlock() = default;
  explicit PramBlock(BlockMetadata* blockmetadata)
      : block_metadata_(blockmetadata),
        block_buf_base_(nullptr),
        out_degree_base_(nullptr),
        out_offset_base_(nullptr),
        out_edges_base_(nullptr),
        out_degree_base_new_(nullptr),
        out_offset_base_new_(nullptr),
        out_edges_base_new_(nullptr) {}

  ~PramBlock() override {
    if (block_buf_base_ != nullptr) {
      delete[] block_buf_base_;
    }
  }

  std::unique_ptr<Serialized> Serialize(
      const core::common::TaskRunner& runner) override {
    // ptr clear work
    block_buf_base_ = nullptr;
    out_degree_base_ = nullptr;
    out_offset_base_ = nullptr;
    out_edges_base_ = nullptr;

    if (core::common::Configurations::Get()->edge_mutate &&
        block_metadata_->num_outgoing_edges != 0) {
      if (out_degree_base_new_) {
        delete[] out_degree_base_new_;
        out_degree_base_new_ = nullptr;
      }
      if (out_offset_base_new_) {
        delete[] out_offset_base_new_;
        out_offset_base_new_ = nullptr;
      }
      if (out_edges_base_new_) {
        delete[] out_edges_base_new_;
        out_edges_base_new_ = nullptr;
      }
    }
    return core::util::pointer_downcast<Serialized,
                                        SerializedPramBlockCSRGraph>(
        std::move(graph_serialized_));
  }

  void Deserialize(const core::common::TaskRunner& runner,
                   std::unique_ptr<Serialized>&& serialized) override {
    graph_serialized_ = std::move(
        core::util::pointer_upcast<Serialized, SerializedPramBlockCSRGraph>(
            std::move(serialized)));

    // degree and offset
    block_buf_base_ = graph_serialized_->GetCSRBuffer()->at(0).Get();
    size_t offset = 0;
    out_degree_base_ = (VertexID*)(block_buf_base_ + offset);
    offset += sizeof(VertexID) * block_metadata_->num_vertices;
    out_offset_base_ = (EdgeIndex*)(block_buf_base_ + offset);
    // edges
    out_edges_base_ =
        (VertexID*)(graph_serialized_->GetCSRBuffer()->at(1).Get());
    // mutate
    if (core::common::Configurations::Get()->edge_mutate &&
        block_metadata_->num_outgoing_edges != 0) {
      out_degree_base_new_ = new VertexDegree[block_metadata_->num_vertices];
      memcpy(out_degree_base_new_, out_degree_base_,
             sizeof(VertexDegree) * block_metadata_->num_vertices);
      out_offset_base_new_ = new EdgeIndex[block_metadata_->num_vertices];
      edge_delete_bitmap_.Init(block_metadata_->num_outgoing_edges);
      // edges_new buffer is malloc when used. and release in the call function.
      out_edges_base_new_ = nullptr;
    } else {
      out_edges_base_new_ = nullptr;
      out_degree_base_new_ = nullptr;
      out_offset_base_new_ = nullptr;
    }
  }

  // TODO: add block methods like sub-graph
  size_t GetVertexNums() const { return block_metadata_->num_vertices; }

  size_t GetOutEdgeNums() const { return block_metadata_->num_outgoing_edges; }

  VertexID GetVertexID(VertexIndex index) {
    return block_metadata_->begin_id + index;
  }

  VertexDegree GetOutDegreeByIndex(VertexIndex index) {
    return out_degree_base_[index];
  }

  EdgeIndex GetOutOffsetByIndex(VertexIndex index) {
    return out_offset_base_[index];
  }

  VertexID* GetOutEdgesBaseByIndex(VertexIndex index) {
    return out_edges_base_ + out_offset_base_[index];
  }

  void MutateGraphEdge(core::common::TaskRunner* runner) {}

  // log functions for lookup block info
  void LogBlockVertices() const {
    LOGF_INFO("block {} begin {} end {}: ==== ", block_metadata_->bid,
              block_metadata_->begin_id, block_metadata_->end_id);
    for (size_t i = block_metadata_->begin_id; i < block_metadata_->end_id;
         ++i) {
      auto index = GetIndex(i);
      LOGF_INFO("vertex: {} degree: {} offset: {}", i, out_degree_base_[index],
                out_offset_base_[index]);
    }
  }

  void LogBlockEdges() const {
    LOG_INFO("block edges info: ");
    for (size_t i = block_metadata_->begin_id; i < block_metadata_->end_id;
         ++i) {
      auto index = GetIndex(i);
      std::stringstream edges;
      for (EdgeIndex j = out_offset_base_[index];
           j < out_offset_base_[index] + out_degree_base_[index]; ++j) {
        edges << std::to_string(out_edges_base_[j]) + " ";
      }
      LOGF_INFO("edge: {} ", edges.str());
    }
  }

  void SetGlobalVertexData(update_stores::PramUpdateStoreUInt32* update_store) {
    update_store_ = update_store;
  }

  const core::common::Bitmap* GetEdgeDeleteBitmap() {
    return update_store_->GetDeleteBitmap();
  }

 private:
  VertexID GetIndex(VertexID id) const {
    return id - block_metadata_->begin_id;
  }

 private:
  BlockMetadata* block_metadata_;

  std::unique_ptr<SerializedPramBlockCSRGraph> graph_serialized_;

  uint8_t* block_buf_base_;
  VertexDegree* out_degree_base_;
  EdgeIndex* out_offset_base_;
  VertexID* out_edges_base_;

  // no data here, use global data only

  // used for PRAM algorithm, which needs to delete edge
  VertexDegree* out_degree_base_new_;
  EdgeIndex* out_offset_base_new_;
  VertexID* out_edges_base_new_;
  core::common::Bitmap edge_delete_bitmap_;

  update_stores::PramUpdateStoreUInt32* update_store_;
};

typedef PramBlock<core::common::Uint32VertexDataType,
                  core::common::DefaultEdgeDataType>
    BlockCSRGraphUInt32;

typedef PramBlock<core::common::Uint16VertexDataType,
                  core::common::DefaultEdgeDataType>
    BlockCSRGraphUInt16;

}  // namespace sics::graph::nvme::data_structures::graph

#endif  // GRAPH_SYSTEMS_NVME_DATA_STRUCTURES_GRAPH_PRAM_BLOCK_H_
