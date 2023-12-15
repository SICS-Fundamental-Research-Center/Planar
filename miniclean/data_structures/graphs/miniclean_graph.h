#ifndef MINICLEAN_DATA_STRUCTURES_GRAPHS_MINICLEAN_GRAPH_H_
#define MINICLEAN_DATA_STRUCTURES_GRAPHS_MINICLEAN_GRAPH_H_

#include <cstdint>
#include <memory>
#include <vector>

#include "core/common/bitmap_no_ownership.h"
#include "core/common/multithreading/task_runner.h"
#include "core/data_structures/buffer.h"
#include "core/data_structures/serializable.h"
#include "core/data_structures/serialized.h"
#include "core/util/logging.h"
#include "miniclean/common/types.h"
#include "miniclean/data_structures/graphs/miniclean_graph_metadata.h"
#include "miniclean/data_structures/graphs/serialized_miniclean_graph.h"

namespace sics::graph::miniclean::data_structures::graphs {

class MiniCleanGraph : public sics::graph::core::data_structures::Serializable {
 private:
  using Serialized = sics::graph::core::data_structures::Serialized;
  using TaskRunner = sics::graph::core::common::TaskRunner;
  using OwnedBuffer = sics::graph::core::data_structures::OwnedBuffer;
  using BitmapHandle = sics::graph::core::common::BitmapNoOwnerShip;
  using GraphID = sics::graph::miniclean::common::GraphID;
  using VertexLabel = sics::graph::miniclean::common::VertexLabel;
  using VertexID = sics::graph::miniclean::common::VertexID;
  using VertexAttributeID = sics::graph::miniclean::common::VertexAttributeID;
  using EdgeIndex = sics::graph::miniclean::common::EdgeIndex;
  using EdgeLabel = sics::graph::miniclean::common::EdgeLabel;

 public:
  explicit MiniCleanGraph(const MiniCleanSubgraphMetadata metadata)
      : metadata_(metadata) {}
  ~MiniCleanGraph() = default;

  std::unique_ptr<Serialized> Serialize(const TaskRunner& runner) override;

  void Deserialize(const TaskRunner& runner,
                   std::unique_ptr<Serialized>&& serialized) override;

  VertexID GetNumVertices() const { return metadata_.num_vertices; }

  VertexLabel GetVertexLabel(VertexID vidl) const;

  const uint8_t* GetVertexAttributePtr(VertexID vidl,
                                       VertexAttributeID vattr_id) const {
    if (vattr_base_pointers_[vattr_id] == nullptr) {
      throw std::runtime_error("The vertex do not have the attribute: " +
                               std::to_string(vattr_id));
    }

    if (metadata_.vattr_id_to_vlabel_id[vattr_id] != GetVertexLabel(vidl)) {
      throw std::runtime_error("The vertex do not have the attribute: " +
                               std::to_string(vattr_id));
    }

    uint8_t* base_ptr = vattr_base_pointers_[vattr_id];
    VertexAttributeType vattr_type = vattr_types_[vattr_id];
    VertexID relative_vid =
        vidl - metadata_.vlabel_id_to_vidl_range[GetVertexLabel(vidl)].first;

    switch (vattr_type) {
      case kString: {
        uint16_t max_string_length =
            metadata_.vattr_id_to_max_string_length[vattr_id];
        return base_ptr + relative_vid * max_string_length;
      }
      case kUInt8:
        return base_ptr + relative_vid * sizeof(uint8_t);
      case kUInt16:
        return base_ptr + relative_vid * sizeof(uint16_t);
      case kUInt32:
        return base_ptr + relative_vid * sizeof(uint32_t);
      case kUInt64:
        return base_ptr + relative_vid * sizeof(uint64_t);
      default:
        LOG_FATAL("Unsupported vertex attribute type: ", vattr_type);
    }
  }

 private:
  void ParseSubgraphCSR(const OwnedBuffer& buffer);
  void ParseBitmapHandle(const OwnedBuffer& buffer);
  void ParseVertexAttribute(size_t vattr_id, const OwnedBuffer& buffer);

  // Graph metadata
  const MiniCleanSubgraphMetadata metadata_;

  // Serialized graph for I/O.
  std::unique_ptr<SerializedMiniCleanGraph> serialized_graph_;

  // CSR base pointer
  uint8_t* graph_base_pointer_;
  VertexID* vidl_to_vidg_base_pointer_;
  VertexID* indegree_base_pointer_;
  VertexID* outdegree_base_pointer_;
  EdgeIndex* in_offset_base_pointer_;
  EdgeIndex* out_offset_base_pointer_;
  VertexID* incoming_vidl_base_pointer_;
  VertexID* outgoing_vidl_base_pointer_;

  // Bitmap `is_in_graph`
  // Note: the ownership of `is_in_graph` is not owned by the bitmap since the
  // memory is not allocated by itself and it would not free the memory when
  // destructing.
  BitmapHandle is_in_graph_bitmap_;

  // Vertex attributes
  std::vector<uint8_t*> vattr_base_pointers_;
  std::vector<VertexAttributeType> vattr_types_;
};
}  // namespace sics::graph::miniclean::data_structures::graphs

#endif  // MINICLEAN_DATA_STRUCTURES_GRAPHS_MINICLEAN_GRAPH_H_