#ifndef MINICLEAN_DATA_STRUCTURES_GRAPHS_MINICLEAN_GRAPH_H_
#define MINICLEAN_DATA_STRUCTURES_GRAPHS_MINICLEAN_GRAPH_H_

#include <cstdint>
#include <memory>
#include <vector>

#include "core/common/multithreading/task_runner.h"
#include "core/data_structures/buffer.h"
#include "core/data_structures/serializable.h"
#include "core/data_structures/serialized.h"
#include "miniclean/common/types.h"
#include "miniclean/data_structures/graphs/miniclean_graph_metadata.h"

namespace sics::graph::miniclean::data_structures::graphs {

class MiniCleanGraph : public sics::graph::core::data_structures::Serializable {
 private:
  using Serialized = sics::graph::core::data_structures::Serialized;
  using TaskRunner = sics::graph::core::common::TaskRunner;
  using OwnedBuffer = sics::graph::core::data_structures::OwnedBuffer;
  using GraphID = sics::graph::miniclean::common::GraphID;
  using VertexLabel = sics::graph::miniclean::common::VertexLabel;
  using VertexID = sics::graph::miniclean::common::VertexID;
  using VertexAttributeID = sics::graph::miniclean::common::VertexAttributeID;
  using EdgeIndex = sics::graph::miniclean::common::EdgeIndex;
  using EdgeLabel = sics::graph::miniclean::common::EdgeLabel;

 public:
  explicit MiniCleanGraph(MiniCleanGraphMetadata* metadata_ptr,
                          sics::graph::miniclean::common::GraphID gid)
      : gid_(gid), metadata_ptr_(metadata_ptr) {}
  ~MiniCleanGraph() = default;

  std::unique_ptr<Serialized> Serialize(const TaskRunner& runner) override;

  void Deserialize(const TaskRunner& runner,
                   std::unique_ptr<Serialized>&& serialized) override;

 private:
  void ParseSubgraphCSR(const std::vector<OwnedBuffer>& buffer_list);

  GraphID gid_;

  // Graph metadata
  MiniCleanGraphMetadata* metadata_ptr_;

  // CSR base pointer
  uint8_t* graph_base_pointer_;
  VertexID* vidl_to_vidg_base_pointer_;
  VertexID* outgoing_vidl_base_pointer_;
  VertexID* incoming_vidl_base_pointer_;
  VertexID* indegree_base_pointer_;
  VertexID* outdegree_base_pointer_;
  EdgeIndex* in_offset_base_pointer_;
  EdgeIndex* out_offset_base_pointer_;

  // Graph attributes
  std::vector<uint8_t*> vattr_id_to_base_ptr_vec_;
};
}  // namespace sics::graph::miniclean::data_structures::graphs

#endif  // MINICLEAN_DATA_STRUCTURES_GRAPHS_MINICLEAN_GRAPH_H_