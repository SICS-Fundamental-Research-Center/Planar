#ifndef GRAPH_SYSTEMS_CORE_DATA_STRUCTURES_GRAPH_PRAM_BLOCK_H_
#define GRAPH_SYSTEMS_CORE_DATA_STRUCTURES_GRAPH_PRAM_BLOCK_H_

#include <memory>

#include "common/bitmap.h"
#include "common/bitmap_no_ownership.h"
#include "common/config.h"
#include "common/types.h"
#include "data_structures/graph/serialized_pram_block_csr.h"
#include "data_structures/graph_metadata.h"
#include "data_structures/serializable.h"
#include "data_structures/serialized.h"
#include "util/atomic.h"
#include "util/pointer_cast.h"

namespace sics::graph::core::data_structures::graph {

template <typename TV, typename TE>
class PramBlock : public Serializable {
  using GraphID = common::GraphID;
  using BlockID = common::BlockID;
  using VertexID = common::VertexID;
  using VertexIndex = common::VertexIndex;
  using EdgeIndex = common::EdgeIndex;
  using VertexDegree = common::VertexDegree;
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
      const common::TaskRunner& runner) override {
    // ptr clear work
    block_buf_base_ = nullptr;
    out_degree_base_ = nullptr;
    out_offset_base_ = nullptr;
    out_edges_base_ = nullptr;

    if (common::Configurations::Get()->edge_mutate &&
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
  }

  void Deserialize(const common::TaskRunner& runner,
                   std::unique_ptr<Serialized>&& serialized) override {
    graph_serialized_ =
        std::move(util::pointer_upcast<Serialized, SerializedPramBlockCSRGraph>(
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
    if (common::Configurations::Get()->edge_mutate &&
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

 private:
  VertexID GetIndex(VertexID id) { return id - block_metadata_->begin_id; }

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
  common::Bitmap edge_delete_bitmap_;
};

typedef PramBlock<common::Uint32VertexDataType, common::DefaultEdgeDataType>
    BlockCSRGraphUInt32;

typedef PramBlock<common::Uint16VertexDataType, common::DefaultEdgeDataType>
    BlockCSRGraphUInt16;

}  // namespace sics::graph::core::data_structures::graph

#endif  // GRAPH_SYSTEMS_CORE_DATA_STRUCTURES_GRAPH_PRAM_BLOCK_H_
