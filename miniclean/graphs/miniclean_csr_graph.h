#ifndef MINICLEAN_GRAPHS_MINICLEAN_CSR_GRAPH_H_
#define MINICLEAN_GRAPHS_MINICLEAN_CSR_GRAPH_H_

#include <memory>

#include "core/data_structures/graph/immutable_csr_graph.h"
#include "core/data_structures/graph/serialized_immutable_csr_graph.h"
#include "core/data_structures/graph_metadata.h"
#include "core/data_structures/serializable.h"
#include "core/data_structures/serialized.h"
#include "core/util/logging.h"

namespace sics::graph::miniclean::graphs {

// TODO (bai-wenchao): Refactor this class after Xiaoke makes changes to the
// base class.
class MiniCleanCSRGraph
    : public sics::graph::core::data_structures::graph::ImmutableCSRGraph {

 private:
  using ImmutableCSRGraph =
      sics::graph::core::data_structures::graph::ImmutableCSRGraph;
  using SubgraphMetadata = sics::graph::core::data_structures::SubgraphMetadata;
  using GraphID = sics::graph::core::common::GraphID;
  using VertexID = sics::graph::core::common::VertexID;
  using VertexLabel = sics::graph::core::common::VertexLabel;
  using OwnedBuffer = sics::graph::core::data_structures::OwnedBuffer;
  using Serialized = sics::graph::core::data_structures::Serialized; 
  using TaskRunner = sics::graph::core::common::TaskRunner;
  using SerializedImmutableCSRGraph =
      sics::graph::core::data_structures::graph::SerializedImmutableCSRGraph;

 public:
  explicit MiniCleanCSRGraph(SubgraphMetadata metadata)
      : metadata_(metadata) {}
  
  MiniCleanCSRGraph(GraphID gid) : ImmutableCSRGraph(gid) {} 

  std::unique_ptr<Serialized> Serialize(const TaskRunner& runner) override;

  void Deserialize(const TaskRunner& runner,
                   std::unique_ptr<Serialized>&& serialized) override;

  GraphID get_gid() const { return gid_; }
  VertexID get_num_vertices() const { return num_vertices_; }
  VertexID get_num_incoming_edges() const { return num_incoming_edges_; }
  VertexID get_num_outgoing_edges() const { return num_outgoing_edges_; }
  VertexID get_max_vid() const { return max_vid_; }
  VertexID get_min_vid() const { return min_vid_; }

  VertexLabel* get_vertex_label() const { return vertex_label_base_pointer_; }
  VertexLabel* get_in_edge_label() const { return in_edge_label_base_pointer_; }
  VertexLabel* get_out_edge_label() const { return out_edge_label_base_pointer_; }

 private:
  void ParseSubgraphCSR(const std::list<OwnedBuffer>& buffer_list);
  void ParseVertexLabel(const std::list<OwnedBuffer>& buffer_list);
  void ParseInedgeLabel(const std::list<OwnedBuffer>& buffer_list);
  void ParseOutedgeLabel(const std::list<OwnedBuffer>& buffer_list);

 private:
  std::unique_ptr<SerializedImmutableCSRGraph> serialized_;

  GraphID gid_ = 0;
  VertexID num_vertices_ = 0;
  VertexID num_incoming_edges_ = 0;
  VertexID num_outgoing_edges_ = 0;
  VertexID max_vid_ = 0;
  VertexID min_vid_ = 0;

  // config. attributes to build the CSR.
  SubgraphMetadata metadata_;
  // Vertex labels
  VertexLabel* vertex_label_base_pointer_;
  // Edge labels
  VertexLabel* in_edge_label_base_pointer_;
  VertexLabel* out_edge_label_base_pointer_;
};
}  // namespace sics::graph::miniclean::graphs

#endif  // MINICLEAN_GRAPHS_MINICLEAN_CSR_GRAPH_H_