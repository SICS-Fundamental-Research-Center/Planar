#ifndef MINICLEAN_GRAPHS_MINICLEAN_CSR_GRAPH_H_
#define MINICLEAN_GRAPHS_MINICLEAN_CSR_GRAPH_H_

#include <memory>

#include "core/data_structures/graph/immutable_csr_graph.h"
#include "core/data_structures/graph/serialized_immutable_csr_graph.h"
#include "core/data_structures/serializable.h"
#include "core/data_structures/serialized.h"
#include "miniclean/graphs/miniclean_csr_graph_config.h"

namespace sics::graph::miniclean::graphs {

// TODO (bai-wenchao): Refactor this class after Xiaoke makes changes to the
// base class.
class MiniCleanCSRGraph
    : public sics::graph::core::data_structures::graph::ImmutableCSRGraph {

 private:
  using ImmutableCSRGraph =
      sics::graph::core::data_structures::graph::ImmutableCSRGraph;
  using GraphID = sics::graph::core::common::GraphID;
  using VertexID = sics::graph::core::common::VertexID;
  using VertexLabel = sics::graph::core::common::VertexLabel;
  using OwnedBuffer = sics::graph::core::data_structures::OwnedBuffer;
  using Serialized = sics::graph::core::data_structures::Serialized; 
  using TaskRunner = sics::graph::core::common::TaskRunner;
  using SerializedImmutableCSRGraph =
      sics::graph::core::data_structures::graph::SerializedImmutableCSRGraph;

 public:
  explicit MiniCleanCSRGraph(GraphID gid) : ImmutableCSRGraph(gid) {}
  MiniCleanCSRGraph(GraphID gid, MiniCleanCSRGraphConfig csr_config)
      : ImmutableCSRGraph(gid), csr_config_(csr_config) {}


  std::unique_ptr<Serialized> Serialize(const TaskRunner& runner) override;

  void Deserialize(const TaskRunner& runner,
                   std::unique_ptr<Serialized>&& serialized) override;

  VertexLabel* get_vertex_label() const { return vertex_label_base_pointer_; }
  VertexLabel* get_in_edge_label() const { return in_edge_label_base_pointer; }
  VertexLabel* get_out_edge_label() const { return out_edge_label_base_pointer; }

 protected:
  void ParseSubgraphCSR(const std::list<OwnedBuffer>& buffer_list);
  void ParseVertexLabel(const std::list<OwnedBuffer>& buffer_list);
  void ParseInedgeLabel(const std::list<OwnedBuffer>& buffer_list);
  void ParseOutedgeLabel(const std::list<OwnedBuffer>& buffer_list);

 private:
  // config. attributes to build the CSR.
  MiniCleanCSRGraphConfig csr_config_;
  // Vertex labels
  VertexLabel* vertex_label_base_pointer_ = nullptr;
  // Edge labels
  VertexLabel* in_edge_label_base_pointer = nullptr;
  VertexLabel* out_edge_label_base_pointer = nullptr;
};
}  // namespace sics::graph::miniclean::graphs

#endif  // MINICLEAN_GRAPHS_MINICLEAN_CSR_GRAPH_H_