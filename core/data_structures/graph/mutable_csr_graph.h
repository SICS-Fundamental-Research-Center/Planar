#ifndef GRAPH_SYSTEMS_CORE_DATA_STRUCTURES_GRAPH_MUTABLE_CSR_GRAPH_H_
#define GRAPH_SYSTEMS_CORE_DATA_STRUCTURES_GRAPH_MUTABLE_CSR_GRAPH_H_

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

// TV : type of vertexData; TE : type of EdgeData
template <typename TV, typename TE>
class MutableCSRGraph : public Serializable {
  using GraphID = common::GraphID;
  using VertexID = common::VertexID;
  using VertexIndex = common::VertexIndex;
  using EdgeIndex = common::EdgeIndex;
  using VertexDegree = common::VertexDegree;
  using SerializedMutableCSRGraph =
      data_structures::graph::SerializedMutableCSRGraph;

 public:
  using VertexData = TV;
  using EdgeData = TE;
  MutableCSRGraph() = default;
  explicit MutableCSRGraph(SubgraphMetadata* metadata, size_t num_all_vertices)
      : metadata_(metadata),
        num_all_vertices_(num_all_vertices),
        graph_buf_base_(nullptr),
        vertex_id_by_local_index_(nullptr),
        out_degree_base_(nullptr),
        out_offset_base_(nullptr),
        out_edges_base_(nullptr),
        parallelism_(common::Configurations::Get()->parallelism),
        task_package_factor_(
            common::Configurations::Get()->task_package_factor) {}

  ~MutableCSRGraph() override = default;

  // Serializable interface override functions
  // serialize the graph and release corresponding memory
  std::unique_ptr<Serialized> Serialize(
      const common::TaskRunner& runner) override {
    graph_buf_base_ = nullptr;
    vertex_id_by_local_index_ = nullptr;
    out_degree_base_ = nullptr;
    out_offset_base_ = nullptr;
    out_edges_base_ = nullptr;
    vertex_data_read_base_ = nullptr;
    delete[] vertex_data_write_base_;
    vertex_data_write_base_ = nullptr;

    if (common::Configurations::Get()->edge_mutate) {
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

    return util::pointer_downcast<Serialized, SerializedMutableCSRGraph>(
        std::move(graph_serialized_));
  }

  void Deserialize(const common::TaskRunner& runner,
                   std::unique_ptr<Serialized>&& serialized) override {
    graph_serialized_ =
        std::move(util::pointer_upcast<Serialized, SerializedMutableCSRGraph>(
            std::move(serialized)));

    graph_buf_base_ = graph_serialized_->GetCSRBuffer()->at(0).Get();

    // set the pointer to base address
    if (metadata_->num_incoming_edges == 0) {
      size_t offset = 0;
      vertex_id_by_local_index_ = (VertexID*)(graph_buf_base_ + offset);
      offset += sizeof(VertexID) * metadata_->num_vertices;
      out_degree_base_ = (VertexDegree*)(graph_buf_base_ + offset);
      offset += sizeof(VertexDegree) * metadata_->num_vertices;
      out_offset_base_ = (EdgeIndex*)(graph_buf_base_ + offset);
    } else {
      LOG_FATAL("Error in deserialize mutable csr graph");
    }
    // edges pointer base
    out_edges_base_ =
        (VertexID*)(graph_serialized_->GetCSRBuffer()->at(1).Get());
    // vertex data buf
    vertex_data_read_base_ =
        (VertexData*)(graph_serialized_->GetCSRBuffer()->at(2).Get());
    vertex_data_write_base_ = new VertexData[metadata_->num_vertices];
    memcpy(vertex_data_write_base_, vertex_data_read_base_,
           sizeof(VertexData) * metadata_->num_vertices);
    // bitmap
    is_in_graph_bitmap_.Init(
        num_all_vertices_,
        (uint64_t*)(graph_serialized_->GetCSRBuffer()->at(3).Get()));
    vertex_src_or_dst_bitmap_.Init(
        metadata_->num_vertices,
        (uint64_t*)(graph_serialized_->GetCSRBuffer()->at(4).Get()));
    // If graph is mutable, malloc corresponding structure used in computing.
    if (common::Configurations::Get()->edge_mutate) {
      out_degree_base_new_ = new VertexDegree[metadata_->num_vertices];
      memcpy(out_degree_base_new_, out_degree_base_,
             sizeof(VertexDegree) * metadata_->num_vertices);
      out_offset_base_new_ = new EdgeIndex[metadata_->num_vertices];
      edge_delete_bitmap_.Init(metadata_->num_outgoing_edges);
      // Out_edges_base_new_ buffer is malloc when used. And release in the same
      // function.
    }
    // If partition type is vertex cut, get index_by_global_id_.
    if (common::Configurations::Get()->partition_type ==
        common::PartitionType::VertexCut) {
      index_by_global_id_ =
          (VertexIndex*)(graph_serialized_->GetCSRBuffer()->at(5).Get());
    }
  }

  // methods for sync data
  void SyncVertexData() {
    memcpy(vertex_data_read_base_, vertex_data_write_base_,
           sizeof(VertexData) * metadata_->num_vertices);
  }

  void UpdateOutOffsetBaseNew(common::TaskRunner* runner) {
    LOG_INFO("out_offset_base_new update begin!");
    // TODO: change simple logic
    uint32_t step = ceil((double)metadata_->num_vertices / parallelism_);
    VertexIndex b1 = 0, e1 = 0;
    {
      common::TaskPackage pre_tasks;
      pre_tasks.reserve(parallelism_);
      for (uint32_t i = 0; i < parallelism_; i++) {
        b1 = i * step;
        if (b1 + step > metadata_->num_vertices) {
          e1 = metadata_->num_vertices;
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
      if (e > metadata_->num_vertices) {
        e = metadata_->num_vertices;
      }
      out_offset_base_new_[b] += accumulate_base;
      accumulate_base += out_offset_base_new_[e - 1];
      accumulate_base += out_degree_base_new_[e - 1];
    }
    {
      common::TaskPackage fix_tasks;
      fix_tasks.reserve(parallelism_);
      b1 = 0;
      e1 = 0;
      for (uint32_t i = 0; i < parallelism_; i++) {
        b1 = i * step;
        if (b1 + step > metadata_->num_vertices) {
          e1 = metadata_->num_vertices;
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
    LOG_INFO("out_offset_base_new update finish!");
  }

  void MutateGraphEdge(common::TaskRunner* runner) {
    LOG_INFO("Mutate graph edge begin");
    // Check left edges in subgraph.
    size_t num_outgoing_edges_new =
        metadata_->num_outgoing_edges - edge_delete_bitmap_.Count();
    // compute out_offset
    if (num_outgoing_edges_new != 0) {
      LOG_INFO("init new data structure for graph");
      out_edges_base_new_ = new VertexID[num_outgoing_edges_new];
      UpdateOutOffsetBaseNew(runner);
      //      for (int i = 0; i < metadata_->num_vertices; i++) {
      //        LOGF_INFO("check: id: {}, new degree: {}, new offset: {}",
      //                  vertex_id_by_local_index_[i], out_degree_base_new_[i],
      //                  out_offset_base_new_[i]);
      //      }
      size_t task_num = parallelism_ * task_package_factor_;
      uint32_t task_size = ceil((double)metadata_->num_vertices / task_num);
      task_size = task_size < 2 ? 2 : task_size;
      LOGF_INFO("task size {}", task_size);
      common::TaskPackage tasks;
      tasks.reserve(task_num);
      VertexIndex begin_index = 0, end_index = 0;
      for (; begin_index < metadata_->num_vertices;) {
        end_index += task_size;
        if (end_index > metadata_->num_vertices) {
          end_index = metadata_->num_vertices;
        }
        auto task = std::bind([&, begin_index, end_index]() {
          EdgeIndex index = out_offset_base_new_[begin_index];
          for (int i = begin_index; i < end_index; i++) {
            EdgeIndex offset = out_offset_base_[i];
            for (int j = 0; j < out_degree_base_[i]; j++) {
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
      // tear down degree and offset buffer
      LOG_INFO("tear down old data structure for graph");
      memcpy(out_degree_base_, out_degree_base_new_,
             sizeof(VertexDegree) * metadata_->num_vertices);
      memcpy(out_offset_base_, out_offset_base_new_,
             sizeof(EdgeIndex) * metadata_->num_vertices);
      // change out_edges_buffer to new one
      edge_delete_bitmap_.Clear();
      // replace edges buffer of subgraph
      graph_serialized_->GetCSRBuffer()->at(1) =
          OwnedBuffer(sizeof(VertexID) * num_outgoing_edges_new,
                      std::unique_ptr<uint8_t>((uint8_t*)out_edges_base_new_));
      out_edges_base_ = out_edges_base_new_;
      out_edges_base_new_ = nullptr;
    } else {
      // Release all assistant buffer in deserialize phase.
      LOG_INFO("No edges left, release all assistant buffer");
      graph_serialized_->GetCSRBuffer()->at(1) = OwnedBuffer(0);
      memcpy(out_degree_base_, out_degree_base_new_,
             sizeof(VertexDegree) * metadata_->num_vertices);
      delete[] out_degree_base_new_;
      out_degree_base_new_ = nullptr;
      delete[] out_offset_base_new_;
      out_offset_base_new_ = nullptr;
      // TODO: whether release bitmap now or in deconstructor
    }
    metadata_->num_outgoing_edges = num_outgoing_edges_new;
    LOG_INFO("Mutate graph edge done");
  }

  // methods for vertex info

  common::VertexCount GetVertexNums() const { return metadata_->num_vertices; }
  size_t GetOutEdgeNums() const { return metadata_->num_outgoing_edges; }

  VertexID GetVertexIDByIndex(VertexIndex index) const {
    return vertex_id_by_local_index_[index];
  }

  VertexDegree GetOutDegreeByIndex(VertexIndex index) const {
    return out_degree_base_[index];
  }

  EdgeIndex GetOutOffsetByIndex(VertexIndex index) const {
    return out_offset_base_[index];
  }

  // fetch the j-th outEdge of vertex by index i
  VertexID GetOneOutEdge(VertexIndex i, VertexIndex j) const {
    return out_edges_base_[out_offset_base_[i] + j];
  }

  // fetch all out edges of vertex by index i
  VertexID* GetOutEdgesByIndex(VertexIndex i) const {
    return out_edges_base_ + out_offset_base_[i];
  }

  VertexData* GetVertxDataByIndex(VertexIndex index) const {
    return vertex_data_read_base_ + index;
  }

  bool IsInGraph(VertexID id) const { return is_in_graph_bitmap_.GetBit(id); }

  bool IsBorderVertex(VertexID id) const { return true; }

  // all read and write methods are for basic type vertex data now

  // this will be used when VertexData is basic num type
  VertexData ReadLocalVertexDataByID(VertexID id) const {
    auto index = index_by_global_id_[id];
    return vertex_data_read_base_[index];
  }

  bool WriteMaxReadDataByID(VertexID id, VertexData data_new) {
    auto index = index_by_global_id_[id];
    return util::atomic::WriteMax(&vertex_data_read_base_[index], data_new);
  }

  // write the min value in local vertex data of vertex id
  // @return: true for global message update, or local message update only
  bool WriteMinVertexDataByID(VertexID id, VertexData data_new) {
    // TODO: need a check for unsigned type?
    auto index = index_by_global_id_[id];
    return util::atomic::WriteMin(&vertex_data_write_base_[index], data_new);
  }

  // write the value in local vertex data of vertex id
  // this is not an atomic method. and the write operation 100% success;
  bool WriteVertexDataByID(VertexID id, VertexData data_new) {
    auto index = index_by_global_id_[id];
    vertex_data_write_base_[index] = data_new;
    return true;
  }

  // TOOD: maybe used later
  VertexData* GetWriteDataByID(VertexID id) {
    auto index = index_by_global_id_[id];
    return vertex_data_write_base_ + index;
  }

  void DeleteEdge(VertexID id, EdgeIndex edge_index) {
    edge_delete_bitmap_.SetBit(edge_index);
    auto index = index_by_global_id_[id];
    util::atomic::WriteMin(&out_degree_base_new_[index],
                           out_degree_base_new_[index] - 1);
  }

  void set_status(const std::string& new_status) { status_ = new_status; }

  void LogVertexData() {
    for (int i = 0; i < metadata_->num_vertices; i++) {
      LOGF_INFO("{} -> Vertex id: {}, read_data: {} write_data: {}", i,
                vertex_id_by_local_index_[i], vertex_data_read_base_[i],
                vertex_data_write_base_[i]);
    }
  }

  void LogEdges() {
    for (int i = 0; i < metadata_->num_vertices; i++) {
      std::string edges = "";
      for (int j = 0; j < out_degree_base_[i]; j++) {
        edges += std::to_string(out_edges_base_[out_offset_base_[i] + j]) + " ";
      }
      LOGF_INFO("{} -> Vertex id: {}, out_edge: {}", i,
                vertex_id_by_local_index_[i], edges);
    }
  }

  void LogGraphInfo() {
    LOGF_INFO("Graph info: num_vertices: {}, num_outgoing_edges: {}",
              metadata_->num_vertices, metadata_->num_outgoing_edges);
    for (int i = 0; i < metadata_->num_vertices; i++) {
      LOGF_INFO(
          "index: {} ---> Vertex id: {}, degree: {}, offset: {}, is_src: {}", i,
          vertex_id_by_local_index_[i], out_degree_base_[i],
          out_offset_base_[i], vertex_src_or_dst_bitmap_.GetBit(i));
    }

    std::string edges = "";
    for (int i = 0; i < metadata_->num_outgoing_edges; i++) {
      edges += std::to_string(out_edges_base_[i]) + ", ";
    }
    LOGF_INFO("Edges: {}", edges);
  }

  void LogIsIngraphInfo() {
    LOG_INFO("Is in graph bitmap info:");
    for (size_t i = 0; i < num_all_vertices_; i++) {
      LOGF_INFO("Vertex id: {}, is_in_graph: {}", i,
                is_in_graph_bitmap_.GetBit(i));
    }
  }

  void LogIndexInfo() {
    for (int i = 0; i < num_all_vertices_; i++) {
      LOGF_INFO("Vertex id: {}, index: {}", i, index_by_global_id_[i]);
    }
  }

 private:
  // use binary search to find the index of id
  [[nodiscard]] VertexIndex GetIndexByID(VertexID id) const {
    // TODO: binary search
    return id;
    VertexIndex begin = 0;
    VertexIndex end = metadata_->num_vertices - 1;
    VertexIndex mid = 0;
    while (begin <= end) {
      mid = begin + (end - begin) / 2;
      if (vertex_id_by_local_index_[mid] < id) {
        begin = mid + 1;
      } else if (vertex_id_by_local_index_[mid] > id) {
        end = mid - 1;
      } else {
        return mid;
      }
    }
    return INVALID_VERTEX_INDEX;
  }

 public:
  SubgraphMetadata* metadata_;

  std::unique_ptr<data_structures::graph::SerializedMutableCSRGraph>
      graph_serialized_;

  // deserialized data pointer in CSR format

  // below pointer need method to replace memory
  uint8_t* graph_buf_base_;
  VertexID* vertex_id_by_local_index_;
  VertexDegree* out_degree_base_;
  EdgeIndex* out_offset_base_;
  VertexID* out_edges_base_;
  VertexData* vertex_data_read_base_;

  VertexIndex* index_by_global_id_;

  VertexData* vertex_data_write_base_;

  // bitmap read from disk, have no ownership of data
  common::BitmapNoOwnerShip vertex_src_or_dst_bitmap_;
  common::BitmapNoOwnerShip is_in_graph_bitmap_;

  // used for mutable algorithm only;
  VertexDegree* out_degree_base_new_;
  EdgeIndex* out_offset_base_new_;
  VertexID* out_edges_base_new_;
  common::Bitmap edge_delete_bitmap_;

  std::string status_;
  size_t num_all_vertices_;
  uint32_t parallelism_;  // use change variable for test
  const uint32_t task_package_factor_;
};

typedef MutableCSRGraph<common::Uint32VertexDataType,
                        common::DefaultEdgeDataType>
    MutableCSRGraphUInt32;
typedef MutableCSRGraph<common::Uint16VertexDataType,
                        common::DefaultEdgeDataType>
    MutableCSRGraphUInt16;

}  // namespace sics::graph::core::data_structures::graph

#endif  // GRAPH_SYSTEMS_CORE_DATA_STRUCTURES_GRAPH_MUTABLE_CSR_GRAPH_H_
