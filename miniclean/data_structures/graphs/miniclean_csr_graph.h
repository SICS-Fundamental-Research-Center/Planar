#ifndef MINICLEAN_DATA_STRUCTURES_GRAPHS_MINICLEAN_CSR_GRAPH_H_
#define MINICLEAN_DATA_STRUCTURES_GRAPHS_MINICLEAN_CSR_GRAPH_H_

#include <memory>

#include "core/data_structures/graph/immutable_csr_graph.h"
#include "core/data_structures/graph/serialized_immutable_csr_graph.h"
#include "core/data_structures/graph_metadata.h"
#include "core/data_structures/serializable.h"
#include "core/data_structures/serialized.h"
#include "core/util/logging.h"
#include "miniclean/common/types.h"

namespace sics::graph::miniclean::data_structures::graphs {
// TODO (bai-wenchao): implement MiniCleanCSRVertex with label and attributes.
class MiniCleanCSRGraph
    : public sics::graph::core::data_structures::graph::ImmutableCSRGraph {
 private:
  using GraphID = sics::graph::miniclean::common::GraphID;
  using ImmutableCSRGraph =
      sics::graph::core::data_structures::graph::ImmutableCSRGraph;
  using OwnedBuffer = sics::graph::core::data_structures::OwnedBuffer;
  using Serialized = sics::graph::core::data_structures::Serialized;
  using SerializedImmutableCSRGraph =
      sics::graph::core::data_structures::graph::SerializedImmutableCSRGraph;
  using TaskRunner = sics::graph::core::common::TaskRunner;
  using SubgraphMetadata = sics::graph::core::data_structures::SubgraphMetadata;
  using VertexID = sics::graph::miniclean::common::VertexID;
  using VertexLabel = sics::graph::miniclean::common::VertexLabel;

 public:
  explicit MiniCleanCSRGraph(SubgraphMetadata metadata)
      : ImmutableCSRGraph(metadata) {}

  std::unique_ptr<Serialized> Serialize(const TaskRunner& runner) override;

  void Deserialize(const TaskRunner& runner,
                   std::unique_ptr<Serialized>&& serialized) override;

  VertexLabel* get_vertex_label() const { return vertex_label_base_pointer_; }
  VertexLabel* get_in_edge_label() const { return in_edge_label_base_pointer_; }
  VertexLabel* get_out_edge_label() const {
    return out_edge_label_base_pointer_;
  }

  SubgraphMetadata get_metadata() const { return metadata_; }

 private:
  void ParseSubgraphCSR(const std::vector<OwnedBuffer>& buffer_list);
  void ParseVertexLabel(const std::vector<OwnedBuffer>& buffer_list);
  void ParseInedgeLabel(const std::vector<OwnedBuffer>& buffer_list);
  void ParseOutedgeLabel(const std::vector<OwnedBuffer>& buffer_list);

 private:
  // Vertex labels
  VertexLabel* vertex_label_base_pointer_;
  // Edge labels
  VertexLabel* in_edge_label_base_pointer_;
  VertexLabel* out_edge_label_base_pointer_;
  // TODO (bai-wenchao): design and add vertex attribute base pointer
};
}  // namespace sics::graph::miniclean::data_structures::graphs

#endif  // MINICLEAN_GRAPHS_MINICLEAN_CSR_GRAPH_H_