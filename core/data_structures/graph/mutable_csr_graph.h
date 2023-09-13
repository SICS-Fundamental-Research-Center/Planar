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
  using VertexDegree = uint32_t;
  using VertexOffset = uint32_t;
  using SerializedMutableCSRGraph =
      data_structures::graph::SerializedMutableCSRGraph;

 public:
  using VertexData = TV;
  using EdgeData = TE;
  MutableCSRGraph() = default;
  explicit MutableCSRGraph(SubgraphMetadata* metadata)
      : metadata_(metadata),
        graph_buf_base_(nullptr),
        vertex_id_by_local_index_(nullptr),
        out_degree_base_(nullptr),
        out_offset_base_(nullptr),
        out_edges_base_(nullptr) {}

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
      out_offset_base_ = (VertexOffset*)(graph_buf_base_ + offset);
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
        metadata_->num_vertices,
        (uint64_t*)(graph_serialized_->GetCSRBuffer()->at(3).Get()));
    vertex_src_or_dst_bitmap_.Init(
        metadata_->num_vertices,
        (uint64_t*)(graph_serialized_->GetCSRBuffer()->at(4).Get()));
    // If graph is mutable, malloc corresponding structure used in computing.
    if (common::Configurations::Get()->edge_mutate) {
      out_degree_base_new_ = new VertexDegree[metadata_->num_vertices];
      memcpy(out_degree_base_new_, out_degree_base_,
             sizeof(VertexDegree) * metadata_->num_vertices);
      out_offset_base_new_ = new VertexOffset[metadata_->num_vertices];
      edge_delete_bitmap_.Init(metadata_->num_outgoing_edges);
      // Out_edges_base_new_ buffer is malloc when used. And release in the same
      // function.
    }
  }

  // methods for sync data
  void SyncVertexData() {
    memcpy(vertex_data_read_base_, vertex_data_write_base_,
           sizeof(VertexData) * metadata_->num_vertices);
  }

  void MutateGraphEdge(common::TaskRunner* runner) {
    uint32_t task_size = metadata_->num_vertices /
                         common::Configurations::Get()->max_task_package;
    task_size = task_size < 2 ? 2 : task_size;
    // Check left edges in subgraph.
    size_t num_outgoing_edges_new =
        metadata_->num_outgoing_edges - edge_delete_bitmap_.Count();
    // compute out_offset
    if (num_outgoing_edges_new != 0) {
      out_edges_base_new_ = new VertexID[num_outgoing_edges_new];
      out_offset_base_new_[0] = 0;
      for (int i = 1; i < metadata_->num_vertices; i++) {
        out_offset_base_new_[i] =
            out_offset_base_new_[i - 1] + out_degree_base_new_[i - 1];
      }
      for (int i = 0; i < metadata_->num_vertices; i++) {
        LOGF_INFO("check: id: {}, new degree: {}, new offset: {}",
                  vertex_id_by_local_index_[i], out_degree_base_new_[i],
                  out_offset_base_new_[i]);
      }

      common::TaskPackage tasks;
      VertexIndex begin_index = 0, end_index = 0;
      for (; begin_index < metadata_->num_vertices;) {
        end_index += task_size;
        if (end_index > metadata_->num_vertices) {
          end_index = metadata_->num_vertices;
        }
        auto task = std::bind([&, begin_index, end_index]() {
          VertexOffset index = out_offset_base_new_[begin_index];
          for (int i = begin_index; i < end_index; i++) {
            VertexOffset offset = out_offset_base_[i];
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
      memcpy(out_degree_base_, out_degree_base_new_,
             sizeof(VertexDegree) * metadata_->num_vertices);
      memcpy(out_offset_base_, out_offset_base_new_,
             sizeof(VertexOffset) * metadata_->num_vertices);
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

  VertexOffset GetOutOffsetByIndex(VertexIndex index) const {
    return out_offset_base_[index];
  }

  // fetch one out edge of vertex by index
  VertexID GetOneOutEdge(VertexIndex index, VertexIndex out_edge_index) const {
    return out_edges_base_[out_offset_base_[index] + out_edge_index];
  }

  // fetch all out edges of vertex by index
  VertexID* GetOutEdges(VertexIndex index) const {
    return out_edges_base_ + index;
  }

  VertexData* GetVertxDataByIndex(VertexIndex index) const {
    return vertex_data_read_base_ + index;
  }

  bool IsInGraph(VertexID id) const { return true; }

  bool IsBorderVertex(VertexID id) const { return true; }

  // this will be used when VertexData is basic num type
  VertexData ReadLocalVertexDataByID(VertexID id) const {
    return vertex_data_read_base_[GetIndexByID(id)];
  }

  // all read and write methods are for basic type vertex data now

  // write the min value in local vertex data of vertex id
  // @return: true for global message update, or local message update only
  bool WriteMinVertexDataByID(VertexID id, VertexData data_new) {
    // TODO: need a check for unsigned type?
    auto index = GetIndexByID(id);
    if (vertex_src_or_dst_bitmap_.GetBit(index)) {
      return util::atomic::WriteMin(&vertex_data_write_base_[index], data_new);
    } else {
      return true;
    }
  }

  // write the value in local vertex data of vertex id
  // this is not an atomic method. and the write operation 100% success;
  bool WriteVertexDataByID(VertexID id, VertexData data_new) {
    auto index = GetIndexByID(id);
    if (vertex_src_or_dst_bitmap_.GetBit(index)) {
      vertex_data_write_base_[index] = data_new;
      return true;
    }
    return false;
  }

  void DeleteEdge(VertexID id, EdgeIndex edge_index) {
    edge_delete_bitmap_.SetBit(edge_index);
    VertexIndex index = GetIndexByID(id);
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

 private:
  // use binary search to find the index of id
  [[nodiscard]] VertexIndex GetIndexByID(VertexID id) const {
    // TODO: binary search
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

 private:
  SubgraphMetadata* metadata_;

  std::unique_ptr<data_structures::graph::SerializedMutableCSRGraph>
      graph_serialized_;

  // deserialized data pointer in CSR format

  // below pointer need method to replace memory
  uint8_t* graph_buf_base_;
  VertexID* vertex_id_by_local_index_;
  VertexDegree* out_degree_base_;
  VertexOffset* out_offset_base_;
  VertexID* out_edges_base_;
  VertexData* vertex_data_read_base_;

  VertexData* vertex_data_write_base_;

  // bitmap read from disk, have no ownership of data
  common::BitmapNoOwnerShip vertex_src_or_dst_bitmap_;
  common::BitmapNoOwnerShip is_in_graph_bitmap_;

  // used for mutable algorithm only;
  VertexDegree* out_degree_base_new_;
  VertexOffset* out_offset_base_new_;
  VertexID* out_edges_base_new_;
  common::Bitmap edge_delete_bitmap_;

  std::string status_;
};

typedef MutableCSRGraph<common::Uint32VertexDataType,
                        common::DefaultEdgeDataType>
    MutableCSRGraphUInt32;
typedef MutableCSRGraph<common::Uint16VertexDataType,
                        common::DefaultEdgeDataType>
    MutableCSRGraphUInt16;

}  // namespace sics::graph::core::data_structures::graph

#endif  // GRAPH_SYSTEMS_CORE_DATA_STRUCTURES_GRAPH_MUTABLE_CSR_GRAPH_H_
