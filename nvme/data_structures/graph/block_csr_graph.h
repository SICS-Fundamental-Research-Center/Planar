#ifndef GRAPH_SYSTEMS_NVME_DATA_STRUCTURES_GRAPH_BLOCK_CSR_GRAPH_H
#define GRAPH_SYSTEMS_NVME_DATA_STRUCTURES_GRAPH_BLOCK_CSR_GRAPH_H

#include <fstream>
#include <memory>

#include "core/common/bitmap.h"
#include "core/common/types.h"
#include "core/data_structures/graph_metadata.h"
#include "core/util/atomic.h"

namespace sics::graph::nvme::data_structures::graph {

struct SubBlockImpl {
  using EdgeIndex = core::common::EdgeIndex;
  using VertexID = core::common::VertexID;

 public:
  SubBlockImpl() : out_edges_base_(nullptr) {}
  SubBlockImpl(EdgeIndex num_edges) {
    out_edges_base_ = new VertexID[num_edges];
  }

  void Init(EdgeIndex num_edges) { out_edges_base_ = new VertexID[num_edges]; }
  void Init(VertexID* out_edges_base) { out_edges_base_ = out_edges_base; }
  VertexID* GetBlockAddr() { return out_edges_base_; }
  ~SubBlockImpl() {
    //    free(out_edges_base_);
    delete[] out_edges_base_;
    out_edges_base_ = nullptr;
  }
  void Release() {
    //    free(out_edges_base_);
    delete[] out_edges_base_;
    out_edges_base_ = nullptr;
  }

 public:
  VertexID* out_edges_base_;
};

// TV : type of vertexData; TE : type of EdgeData
class BlockCSRGraph {
  using GraphID = core::common::GraphID;
  using VertexID = core::common::VertexID;
  using VertexIndex = core::common::VertexIndex;
  using EdgeIndex = core::common::EdgeIndex;
  using VertexDegree = core::common::VertexDegree;
  using BlockID = core::common::BlockID;
  using Bitmap = core::common::Bitmap;

  using BlockMetadata = core::data_structures::BlockMetadata;

 public:
  BlockCSRGraph(){};
  BlockCSRGraph(const std::string& root_path, BlockMetadata* block_meta) {
    Init(root_path, block_meta);
  }

  BlockCSRGraph(BlockCSRGraph&& graph) {}

  void Init(const std::string& root_path, BlockMetadata* block_meta) {
    meta_ = block_meta;
    root_path_ = root_path;
    mutate = core::common::Configurations::Get()->edge_mutate;
    parallelism_ = core::common::Configurations::Get()->parallelism;
    task_package_factor_ =
        core::common::Configurations::Get()->task_package_factor;

    // Read index and degree info.
    auto path = root_path + "subBlocks/index.bin";
    std::ifstream file(path, std::ios::binary);
    if (!file) {
      LOG_FATAL("Error opening bin file: ", path);
    }
    file.seekg(0, std::ios::end);
    size_t size = file.tellg();
    LOGF_INFO("Index file size: {} GB", (double)size / 1024 / 1024 / 1024);
    file.seekg(0, std::ios::beg);
    auto num_offsets =
        ((block_meta->num_vertices - 1) / block_meta->offset_ratio) + 1;
    out_offset_reduce_ = new EdgeIndex[num_offsets];
    out_degree_ = new VertexDegree[block_meta->num_vertices];
    file.read((char*)(out_offset_reduce_), num_offsets * sizeof(EdgeIndex));
    file.read((char*)(out_degree_),
              block_meta->num_vertices * sizeof(VertexDegree));
    file.close();
    // Init vector size;
    sub_blocks_.resize(block_meta->num_subBlocks);
    //    num_edges_.resize(block_meta->num_sub_blocks);
    num_edges_ = new EdgeIndex[block_meta->num_subBlocks];
    for (int i = 0; i < block_meta->num_subBlocks; i++) {
      edge_delete_bitmaps_.emplace_back(block_meta->subBlocks.at(i).num_edges);
      num_edges_[i] = block_meta->subBlocks.at(i).num_edges;
    }
    mode_ = core::common::Configurations::Get()->mode;
  }

  ~BlockCSRGraph() {
    delete[] out_offset_reduce_;
    delete[] out_degree_;
    delete[] num_edges_;
  };

  // Set the read sub_block pointer for using.
  void SetSubBlock(BlockID sub_block_id, VertexID* block_edge_base) {
    sub_blocks_.at(sub_block_id).Init(block_edge_base);
  }

  void SetSubBlocksRelease() { edge_loaded = false; }

  void Release(BlockID block_id) { sub_blocks_.at(block_id).Release(); }

  void ReleaseAllSubBlocks() {
    for (int i = 0; i < meta_->num_subBlocks; i++) {
      sub_blocks_.at(i).Release();
    }
    edge_loaded = false;
  }

  uint32_t GetVerticesNum() const { return meta_->num_vertices; }

  std::vector<SubBlockImpl>* GetSubBlocks() { return &sub_blocks_; }

  VertexDegree GetOutDegree(VertexID id) { return out_degree_[id]; }

  EdgeIndex GetOutOffset(VertexID vid) {
    auto idx = vid;
    auto b = idx / meta_->offset_ratio;
    uint64_t res = out_offset_reduce_[b];
    auto beg = b * meta_->offset_ratio;
    while (beg < idx) {
      res += out_degree_[beg++];
    }
    return res;
  }

  // TODO: for edge mode, a map is useful
  inline BlockID GetSubBlockID(VertexID id) { return id / meta_->vertex_size; }

  EdgeIndex GetInitOffset(VertexID id) {
    auto offset = GetOutOffset(id);
    auto subBlock_id = GetSubBlockID(id);
    return offset - meta_->subBlocks.at(subBlock_id).begin_offset;
  }

  VertexID* GetOutEdges(VertexID id) {
    auto offset = GetOutOffset(id);
    auto subBlock_id = GetSubBlockID(id);
    return sub_blocks_.at(subBlock_id).out_edges_base_ +
           (offset - meta_->subBlocks.at(subBlock_id).begin_offset);
  }

  VertexID* GetAllEdges(BlockID bid) {
    return sub_blocks_.at(bid).out_edges_base_;
  }

  VertexID* ApplySubBlockBuffer(BlockID bid) {
    sub_blocks_.at(bid).Init(meta_->subBlocks.at(bid).num_edges);
    return sub_blocks_.at(bid).out_edges_base_;
  }

  Bitmap* GetDelBitmap(BlockID bid) { return &edge_delete_bitmaps_.at(bid); }

  // One thread works for one sub_block, so no need for lock.
  // TODO: attention for Static mode, which maybe conflict above.
  void DeleteEdge(VertexID id, EdgeIndex idx) {
    auto subBlock_id = GetSubBlockID(id);
    edge_delete_bitmaps_.at(subBlock_id).SetBit(idx);
    if (mode_ == core::common::Static) {
      //      std::lock_guard<std::mutex> lock(mtx_);
      //      num_edges_.at(subBlock_id) = num_edges_.at(subBlock_id) - 1;
      core::util::atomic::WriteSub(num_edges_ + subBlock_id, EdgeIndex(1));
      return;
    }
    num_edges_[subBlock_id] = num_edges_[subBlock_id] - 1;
  }

  void DeleteEdgeByVertex(VertexID id, EdgeIndex idx) {
    auto subBlock_id = GetSubBlockID(id);
    auto offset = GetInitOffset(id);
    edge_delete_bitmaps_.at(subBlock_id).SetBit(offset + idx);
    if (mode_ == core::common::Static) {
      //      std::lock_guard<std::mutex> lock(mtx_);
      //      num_edges_[subBlock_id] = num_edges_[subBlock_id] - 1;
      core::util::atomic::WriteSub(num_edges_ + subBlock_id, EdgeIndex(1));
      return;
    }
    num_edges_[subBlock_id] = num_edges_[subBlock_id] - 1;
  }

  bool IsEdgeDelete(VertexID id, EdgeIndex idx) {
    auto sub_block_id = GetSubBlockID(id);
    auto offset = GetInitOffset(id);
    return edge_delete_bitmaps_.at(sub_block_id).GetBit(offset + idx);
  }

  bool IsEdgesLoaded() { return edge_loaded; }

  void SetEdgeLoaded(bool load) { edge_loaded = load; }

  VertexIndex GetVertexIDIndex(VertexID id) { return id; }

  VertexID GetNeiMinId(VertexID id) {
    auto degree = GetOutDegree(id);
    auto edges = GetOutEdges(id);
    if (degree != 0) {
      VertexID res = edges[0];
      if (mutate) {
        auto offset = GetInitOffset(id);
        auto sub_block_id = GetSubBlockID(id);
        auto& bitmap = edge_delete_bitmaps_.at(sub_block_id);
        for (int i = 0; i < degree; i++) {
          if (bitmap.GetBit(offset + i)) continue;
          res = edges[i] < res ? edges[i] : res;
        }
      } else {
        for (int i = 0; i < degree; i++) {
          res = edges[i] < res ? edges[i] : res;
        }
      }
      return res;
    }
    return MAX_VERTEX_ID;
  }

  size_t GetNumEdges() {
    if (mutate) {
      size_t res = 0;
      //      for (auto num : num_edges_) {
      //        res += num;
      //      }
      for (int i = 0; i < meta_->num_subBlocks; i++) {
        res += num_edges_[i];
      }
      return res;
    } else {
      return meta_->num_edges;
    }
  }

  size_t GetNumEdges(std::vector<BlockID>& ids) {
    size_t res = 0;
    if (mutate) {
      for (auto id : ids) {
        res += num_edges_[id];
      }
    } else {
      for (auto id : ids) {
        res += meta_->subBlocks.at(id).num_edges;
      }
    }
    return res;
  }

  void LogGraphInfo() {
    for (VertexID id = 0; id < meta_->num_vertices; id++) {
      std::string tmp = "VertexID: " + std::to_string(id) + ", ";
      auto degree = GetOutDegree(id);
      auto offset = GetOutOffset(id);
      auto edges = GetOutEdges(id);
      tmp += "Degree: " + std::to_string(degree) + ", ";
      tmp += "Offset: " + std::to_string(offset) + ", Edges: ";
      for (int i = 0; i < degree; i++) {
        tmp += std::to_string(edges[i]) + " ";
      }
      LOGF_INFO("{}", tmp);
    }
  }

  void LogSubBlock(BlockID bid) {
    auto& sub_meta = meta_->subBlocks.at(bid);
    LOGF_INFO("\nSub-Block {}, {}, {}", sub_meta.id, sub_meta.num_vertices,
              sub_meta.num_edges);
    for (VertexID i = sub_meta.begin_id; i < sub_meta.end_id; i++) {
      auto edges = GetOutEdges(i);
      auto degree = GetOutDegree(i);
      auto offset = GetOutOffset(i);
      std::string tmp = "";
      for (int j = 0; j < degree; j++) {
        tmp += std::to_string(edges[j]) + " ";
      }
      LOGF_INFO("Vertex {}, degree {}, offset {}, edges {}", i, degree, offset,
                tmp);
    }
  }

  void LogDelGraphInfo() {
    for (int i = 0; i < meta_->num_subBlocks; i++) {
      auto sub_block = meta_->subBlocks.at(i);
      auto bitmap = edge_delete_bitmaps_.at(i);
      EdgeIndex idx = 0;
      for (VertexID id = sub_block.begin_id; id < sub_block.end_id; id++) {
        auto degree = GetOutDegree(id);
        auto offset = GetOutOffset(id);
        auto edges = GetOutEdges(id);
        std::string tmp = "Vertex: " + std::to_string(id) +
                          ", Degree: " + std::to_string(degree) +
                          ", Offset: " + std::to_string(offset) + ", Edges: ";
        for (int i = 0; i < degree; i++) {
          if (!bitmap.GetBit(idx)) {
            tmp += std::to_string(edges[i]) + " ";
          }
          idx++;
        }
        LOGF_INFO("{}", tmp);
      }
    }
  }

  void LogEdgeBlockInfo(BlockID bid) { LOG_INFO("Edge Block {} Info: ", bid); }

  void ReadAllSubBlock() {
    for (BlockID bid = 0; bid < meta_->num_subBlocks; bid++) {
      ReadSubBlock(bid);
    }
  }

  // Only used for check graph info.
  void ReadSubBlock(BlockID id) {
    auto addr = ApplySubBlockBuffer(id);
    std::ifstream file(root_path_ + "subBlocks/edges.bin", std::ios::binary);
    std::streamoff offset =
        meta_->subBlocks.at(id).begin_offset * sizeof(VertexID);
    file.seekg(offset, std::ios::beg);
    auto size = meta_->subBlocks.at(id).num_edges * sizeof(VertexID);
    file.read((char*)addr, size);
    file.close();
  }

 public:
  BlockMetadata* meta_ = nullptr;

  std::string root_path_;

  EdgeIndex* out_offset_reduce_ = nullptr;
  VertexDegree* out_degree_ = nullptr;

  // Edges sub_block. init in constructor.
  std::vector<SubBlockImpl> sub_blocks_;
  std::vector<Bitmap> edge_delete_bitmaps_;
  //  std::vector<EdgeIndex> num_edges_;
  EdgeIndex* num_edges_ = nullptr;
  // bitmap read from disk, have no ownership of data
  //  common::BitmapNoOwnerShip vertex_src_or_dst_bitmap_;
  //  common::BitmapNoOwnerShip is_in_graph_bitmap_;

  // used for mutable algorithm only;
  //  VertexDegree* out_degree_base_new_;
  //  EdgeIndex* out_offset_base_new_;
  //  VertexID* out_edges_base_new_;

  bool edge_loaded = false;
  // Configurations.
  bool mutate = false;
  uint32_t parallelism_;  // Use change variable for test.
  uint32_t task_package_factor_;

  std::mutex mtx_;
  core::common::ModeType mode_ = core::common::Static;
};

}  // namespace sics::graph::nvme::data_structures::graph

#endif  // GRAPH_SYSTEMS_NVME_DATA_STRUCTURES_GRAPH_BLOCK_CSR_GRAPH_H
