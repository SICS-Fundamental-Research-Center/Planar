#ifndef GRAPH_SYSTEMS_CORE_DATA_STRUCTURES_GRAPH_MUTABLE_BLOCK_CSR_GRAPH_H_
#define GRAPH_SYSTEMS_CORE_DATA_STRUCTURES_GRAPH_MUTABLE_BLOCK_CSR_GRAPH_H_

#include <memory>

#include "common/bitmap.h"
#include "common/bitmap_no_ownership.h"
#include "common/config.h"
#include "common/types.h"
#include "data_structures/graph/serialized_mutable_csr_graph.h"
#include "data_structures/graph_metadata.h"
#include "data_structures/serializable.h"
#include "data_structures/serialized.h"
#include "util/atomic.h"
#include "util/pointer_cast.h"

namespace sics::graph::core::data_structures::graph {

struct SubBlockImpl {
 public:
  SubBlockImpl() : out_edges_base_(nullptr) {}
  SubBlockImpl(common::EdgeIndex num_edges) {
    out_edges_base_ = new common::VertexID[num_edges];
  }
  void Init(common::VertexID* out_edges_base) {
    out_edges_base_ = out_edges_base;
  }
  ~SubBlockImpl() {
    delete[] out_edges_base_;
    out_edges_base_ = nullptr;
  }
  void Release() {
    delete[] out_edges_base_;
    out_edges_base_ = nullptr;
  }

 public:
  common::VertexID* out_edges_base_;
};

// TV : type of vertexData; TE : type of EdgeData
template <typename TV, typename TE>
class MutableBlockCSRGraph {
  using GraphID = common::GraphID;
  using VertexID = common::VertexID;
  using VertexIndex = common::VertexIndex;
  using EdgeIndex = common::EdgeIndex;
  using VertexDegree = common::VertexDegree;

 public:
  using VertexData = TV;
  using EdgeData = TE;
  MutableBlockCSRGraph() = default;
  explicit MutableBlockCSRGraph(const std::string& root_path,
                                Block* block_meta) {
    Init(root_path, block_meta);
  }

  void Init(const std::string& root_path, Block* block_meta) {
    metadata_block_ = block_meta;
    parallelism_ = common::Configurations::Get()->parallelism;
    task_package_factor_ = common::Configurations::Get()->task_package_factor;

    // Read index and degree info.
    auto path = root_path + "graphs/" + std::to_string(block_meta->id) +
                "_blocks/index.bin";
    std::ifstream file(path, std::ios::binary);
    if (!file) {
      LOG_FATAL("Error opening bin file: ", path);
    }
    auto num_offsets =
        ((block_meta->num_vertices - 1) / block_meta->offset_ratio) + 1;
    out_offset_reduce_ = new EdgeIndexS[num_offsets];
    out_degree_ = new VertexDegree[block_meta->num_vertices];
    file.read((char*)(out_offset_reduce_), num_offsets * sizeof(EdgeIndexS));
    file.read((char*)(out_degree_),
              block_meta->num_vertices * sizeof(VertexDegree));
    file.close();
    // Init vector size;
    sub_blocks_.resize(block_meta->num_sub_blocks);
    is_in_memory_.resize(block_meta->num_sub_blocks, false);
  }

  ~MutableBlockCSRGraph() {
    delete[] out_offset_reduce_;
    delete[] out_degree_;
  };

  // Set the read sub_block pointer for using.
  void SetSubBlock(BlockID sub_block_id, common::VertexID* block_edge_base) {
    sub_blocks_.at(sub_block_id).Init(block_edge_base);
  }

  void Release(BlockID block_id) { sub_blocks_.at(block_id).Release(); }

  std::vector<SubBlockImpl>* GetSubBlocks() { return &sub_blocks_; }

  VertexDegree GetOutDegree(VertexID id) {
    return out_degree_[id - metadata_block_->begin_id];
  }

  EdgeIndex GetOutOffset(VertexID vid) {
    auto idx = vid - metadata_block_->begin_id;
    auto b = idx / metadata_block_->offset_ratio;
    uint64_t res = out_offset_reduce_[b];
    auto beg = b * metadata_block_->offset_ratio;
    while (beg < idx) {
      res += out_degree_[beg++];
    }
    return res;
  }

  BlockID GetSubBlockID(VertexID id) {
    auto idx = id - metadata_block_->begin_id;
    return idx / metadata_block_->vertex_offset;
  }

  VertexID* GetOutEdges(VertexID id) {
    auto offset = GetOutOffset(id);
    auto subBlock_id = GetSubBlockID(id);
    return sub_blocks_.at(subBlock_id).out_edges_base_ +
           (offset - metadata_block_->sub_blocks.at(subBlock_id).begin_offset);
  }

  void LogGraphInfo() {
    for (VertexID id = metadata_block_->begin_id; id < metadata_block_->end_id;
         id++) {
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

 public:
  Block* metadata_block_;

  EdgeIndexS* out_offset_reduce_ = nullptr;
  VertexDegree* out_degree_ = nullptr;

  // Edges sub_block. init in constructor.
  std::vector<SubBlockImpl> sub_blocks_;
  std::vector<bool> is_in_memory_;  // Indicate if the sub_block is in memory.

  // init by ptr in constructor
  VertexData* vertex_data_read_base_;
  VertexData* vertex_data_write_base_;

  // bitmap read from disk, have no ownership of data
  //  common::BitmapNoOwnerShip vertex_src_or_dst_bitmap_;
  //  common::BitmapNoOwnerShip is_in_graph_bitmap_;

  // used for mutable algorithm only;
  //  VertexDegree* out_degree_base_new_;
  //  EdgeIndex* out_offset_base_new_;
  //  VertexID* out_edges_base_new_;
  common::Bitmap edge_delete_bitmap_;  // Init when need this.

  // Configurations.
  uint32_t parallelism_;  // Use change variable for test.
  uint32_t task_package_factor_;

  std::mutex mtx;
};

typedef MutableBlockCSRGraph<common::Uint32VertexDataType,
                             common::DefaultEdgeDataType>
    MutableBlockCSRGraphUInt32;
typedef MutableBlockCSRGraph<common::Uint16VertexDataType,
                             common::DefaultEdgeDataType>
    MutableBlockCSRGraphUInt16;
typedef MutableBlockCSRGraph<common::FloatVertexDataType,
                             common::DefaultEdgeDataType>
    MutableBlockCSRGraphFloat;

}  // namespace sics::graph::core::data_structures::graph

#endif  // GRAPH_SYSTEMS_CORE_DATA_STRUCTURES_GRAPH_MUTABLE_BLOCK_CSR_GRAPH_H_
