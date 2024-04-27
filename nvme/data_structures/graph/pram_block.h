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

namespace sics::graph::nvme::data_structures::graph {

using BlockMetadata = core::data_structures::BlockMetadata;
using Serialized = core::data_structures::Serialized;
using Serializable = core::data_structures::Serializable;

template <typename TV, typename TE>
class PramBlock : public core::data_structures::Serializable {
  using GraphID = core::common::GraphID;
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
        out_edges_base_new_(nullptr),
        parallelism_(core::common::Configurations::Get()->parallelism),
        task_package_factor_(
            core::common::Configurations::Get()->task_package_factor) {}

  ~PramBlock() override {}

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
      // edges_new buffer is malloc when used. and release in the call function.
      out_edges_base_new_ = nullptr;
      out_degree_base_new_ = new VertexDegree[block_metadata_->num_vertices];
      memcpy(out_degree_base_new_, out_degree_base_,
             sizeof(VertexDegree) * block_metadata_->num_vertices);
      out_offset_base_new_ = new EdgeIndex[block_metadata_->num_vertices];
      edge_delete_bitmap_.Init(block_metadata_->num_outgoing_edges);
    } else {
      out_edges_base_new_ = nullptr;
      out_degree_base_new_ = nullptr;
      out_offset_base_new_ = nullptr;
      edge_delete_bitmap_.Init(0);
    }
  }

  void UpdateOutOffsetBaseNew(core::common::TaskRunner* runner) {
    //    LOG_INFO("out_offset_base_new update begin!");
    // TODO: change simple logic
    uint32_t step = ceil((double)block_metadata_->num_vertices / parallelism_);
    VertexIndex b1 = 0, e1 = 0;
    {
      core::common::TaskPackage pre_tasks;
      pre_tasks.reserve(parallelism_);
      for (uint32_t i = 0; i < parallelism_; i++) {
        b1 = i * step;
        if (b1 + step > block_metadata_->num_vertices) {
          e1 = block_metadata_->num_vertices;
        } else {
          e1 = b1 + step;
        }
        auto task = [this, b1, e1]() {
          out_offset_base_new_[b1] = 0;
          for (uint32_t i = b1 + 1; i < e1; i++) {
            out_offset_base_new_[i] =
                out_offset_base_new_[i - 1] + out_degree_base_new_[i - 1];
          }
        };
        pre_tasks.push_back(task);
      }
      runner->SubmitSync(pre_tasks);
    }
    // compute the base offset of each range
    EdgeIndex accumulate_base = 0;
    for (uint32_t i = 0; i < parallelism_; i++) {
      VertexIndex b = i * step;
      VertexIndex e = (i + 1) * step;
      if (e > block_metadata_->num_vertices) {
        e = block_metadata_->num_vertices;
      }
      out_offset_base_new_[b] += accumulate_base;
      accumulate_base += out_offset_base_new_[e - 1];
      accumulate_base += out_degree_base_new_[e - 1];
    }
    {
      core::common::TaskPackage fix_tasks;
      fix_tasks.reserve(parallelism_);
      b1 = 0;
      e1 = 0;
      for (uint32_t i = 0; i < parallelism_; i++) {
        b1 = i * step;
        if (b1 + step > block_metadata_->num_vertices) {
          e1 = block_metadata_->num_vertices;
        } else {
          e1 = b1 + step;
        }
        auto task = [this, b1, e1]() {
          for (uint32_t i = b1 + 1; i < e1; i++) {
            out_offset_base_new_[i] += out_offset_base_new_[b1];
          }
        };
        fix_tasks.push_back(task);
        b1 += step;
      }
      runner->SubmitSync(fix_tasks);
    }
    //    LOG_INFO("out_offset_base_new update finish!");
  }

  void MutateGraphEdge(core::common::TaskRunner* runner) {
    if (block_metadata_->num_outgoing_edges == 0) {
      return;
    }
    auto del_edges = edge_delete_bitmap_.Count();
    size_t num_outgoing_edges_new =
        block_metadata_->num_outgoing_edges - del_edges;
    if (block_metadata_->num_outgoing_edges < del_edges) {
      //      num_outgoing_edges_new = 0;
      LOG_FATAL("delete edges number is more than left, stop!");
    }

    if (num_outgoing_edges_new != 0) {
      out_edges_base_new_ = new VertexID[num_outgoing_edges_new];
      UpdateOutOffsetBaseNew(runner);

      size_t task_num = parallelism_ * task_package_factor_;
      uint32_t task_size =
          ceil((double)block_metadata_->num_vertices / task_num);
      task_size = task_size < 2 ? 2 : task_size;
      core::common::TaskPackage tasks;
      tasks.reserve(task_num);
      VertexIndex begin_index = 0, end_index = 0;
      for (; begin_index < block_metadata_->num_vertices;) {
        end_index += task_size;
        if (end_index > block_metadata_->num_vertices) {
          end_index = block_metadata_->num_vertices;
        }
        auto task = std::bind([&, begin_index, end_index]() {
          EdgeIndex index = out_offset_base_new_[begin_index];
          for (VertexIndex i = begin_index; i < end_index; i++) {
            EdgeIndex offset = out_offset_base_[i];
            for (VertexDegree j = 0; j < out_degree_base_[i]; j++) {
              if (!edge_delete_bitmap_.GetBit(offset + j)) {
                out_edges_base_new_[index++] = out_edges_base_[offset + j];
              }
            }
          }
        });
        tasks.push_back(task);
        begin_index = end_index;
      }
      runner->SubmitSync(tasks);
      memcpy(out_degree_base_, out_degree_base_new_,
             sizeof(VertexDegree) * block_metadata_->num_vertices);
      memcpy(out_offset_base_, out_offset_base_new_,
             sizeof(EdgeIndex) * block_metadata_->num_vertices);
      edge_delete_bitmap_.Clear();
      edge_delete_bitmap_.Init(num_outgoing_edges_new);
      LOGF_INFO("left edges: {}", num_outgoing_edges_new);
      graph_serialized_->GetCSRBuffer()->at(1) =
          OwnedBuffer(sizeof(VertexID) * num_outgoing_edges_new,
                      std::unique_ptr<uint8_t>((uint8_t*)out_edges_base_new_));
      out_edges_base_ = out_edges_base_new_;
      out_edges_base_new_ = nullptr;
    } else {
      LOG_INFO("No edges left, release all assistant buffer");
      graph_serialized_->GetCSRBuffer()->at(1) = OwnedBuffer(0);
      memcpy(out_degree_base_, out_degree_base_new_,
             sizeof(VertexDegree) * block_metadata_->num_vertices);
      delete[] out_degree_base_new_;
      out_degree_base_new_ = nullptr;
      delete[] out_offset_base_new_;
      out_offset_base_new_ = nullptr;
    }
    block_metadata_->num_outgoing_edges = num_outgoing_edges_new;
  }

  void DeleteEdge(VertexID idx, EdgeIndex eid) {
    edge_delete_bitmap_.SetBit(eid);
    out_degree_base_new_[idx] = out_degree_base_new_[idx] - 1;
    // TODO: Use atomic to update out_degree_base_new_ for discontinued vertex.
    //    core::util::atomic::WriteMin(&out_degree_base_new_[idx],
    //                                 out_degree_base_new_[idx] - 1);
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

  VertexDegree GetOutDegreeByID(VertexID id) {
    return out_degree_base_[GetIndex(id)];
  }

  EdgeIndex GetOutOffsetByIndex(VertexIndex index) {
    return out_offset_base_[index];
  }

  VertexID* GetOutEdgesBaseByIndex(VertexIndex index) {
    return out_edges_base_ + out_offset_base_[index];
  }

  const VertexID* GetOutEdgesByID(VertexID id) {
    return out_edges_base_ + out_offset_base_[GetIndex(id)];
  }

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

  core::common::EdgeIndex GetBlockEdgeOffset() const {
    return block_metadata_->edge_offset;
  }

 private:
  inline VertexID GetIndex(VertexID id) const {
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

  // configs
  uint32_t parallelism_;
  uint32_t task_package_factor_;
};

typedef PramBlock<core::common::Uint32VertexDataType,
                  core::common::DefaultEdgeDataType>
    BlockCSRGraphUInt32;

typedef PramBlock<core::common::Uint16VertexDataType,
                  core::common::DefaultEdgeDataType>
    BlockCSRGraphUInt16;

typedef PramBlock<core::common::FloatVertexDataType,
                  core::common::DefaultEdgeDataType>
    BlockCSRGraphFloat;

}  // namespace sics::graph::nvme::data_structures::graph

#endif  // GRAPH_SYSTEMS_NVME_DATA_STRUCTURES_GRAPH_PRAM_BLOCK_H_
