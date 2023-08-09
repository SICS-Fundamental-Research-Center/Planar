#ifndef MINICLEAN_GRAPHS_MINICLEAN_CSR_GRAPH_H_
#define MINICLEAN_GRAPHS_MINICLEAN_CSR_GRAPH_H_

#include <memory>

#include "core/data_structures/graph/immutable_csr_graph.h"
#include "core/data_structures/graph/serialized_immutable_csr_graph.h"
#include "core/data_structures/serializable.h"
#include "core/data_structures/serialized.h"
#include "miniclean/graphs/miniclean_csr_graph_config.h"

namespace sics::graph::miniclean::graphs {

class MinicleanCSRGraph
    : public sics::graph::core::data_structures::graph::ImmutableCSRGraph {

 private:
  using ImmutableCSRGraph =
      sics::graph::core::data_structures::graph::ImmutableCSRGraph;
  using GraphID = sics::graph::core::common::GraphID;
  using VertexID = sics::graph::core::common::VertexID;
  using OwnedBuffer = sics::graph::core::data_structures::OwnedBuffer;
  using Serialized = sics::graph::core::data_structures::Serialized; 
  using TaskRunner = sics::graph::core::common::TaskRunner;
  using SerializedImmutableCSRGraph =
      sics::graph::core::data_structures::graph::SerializedImmutableCSRGraph;

 public:
  explicit MinicleanCSRGraph(const GraphID gid) : ImmutableCSRGraph(gid) {}
  MinicleanCSRGraph(const GraphID gid, MinicleanCSRGraphConfig&& csr_config)
      : ImmutableCSRGraph(gid) {
    this->csr_config_ = std::move(csr_config);
  }


  std::unique_ptr<Serialized> Serialize(const TaskRunner& runner) override;

  void Deserialize(const TaskRunner& runner,
                   std::unique_ptr<Serialized>&& serialized) override;

  VertexID* get_vertex_label() const { return vertex_label_; }
  VertexID* get_in_edge_label() const { return in_edge_label_; }
  VertexID* get_out_edge_label() const { return out_edge_label_; }

 protected:
  // The graph ID.
  GraphID gid_;
  void ParseSubgraphCSR(const std::list<OwnedBuffer>& buffer_list);
  void ParseVertexLabel(const std::list<OwnedBuffer>& buffer_list);
  void ParseInedgeLabel(const std::list<OwnedBuffer>& buffer_list);
  void ParseOutedgeLabel(const std::list<OwnedBuffer>& buffer_list);

 private:
  // config. attributes to build the CSR.
  MinicleanCSRGraphConfig csr_config_;
  // Vertex labels
  VertexID* vertex_label_ = nullptr;
  // Edge labels
  VertexID* in_edge_label_ = nullptr;
  VertexID* out_edge_label_ = nullptr;
};
}  // namespace sics::graph::miniclean::graphs

#endif  // MINICLEAN_GRAPHS_MINICLEAN_CSR_GRAPH_H_