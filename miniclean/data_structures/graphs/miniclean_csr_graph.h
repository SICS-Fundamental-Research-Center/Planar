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
  using EdgeLabel = sics::graph::miniclean::common::EdgeLabel;
  using GraphID = sics::graph::miniclean::common::GraphID;
  using ImmutableCSRGraph =
      sics::graph::core::data_structures::graph::ImmutableCSRGraph;
  using OwnedBuffer = sics::graph::core::data_structures::OwnedBuffer;
  using Serialized = sics::graph::core::data_structures::Serialized;
  using SerializedImmutableCSRGraph =
      sics::graph::core::data_structures::graph::SerializedImmutableCSRGraph;
  using TaskRunner = sics::graph::core::common::TaskRunner;
  using SubgraphMetadata = sics::graph::core::data_structures::SubgraphMetadata;
  using VertexAttributeValue =
      sics::graph::miniclean::common::VertexAttributeValue;
  using VertexID = sics::graph::miniclean::common::VertexID;
  using VertexLabel = sics::graph::miniclean::common::VertexLabel;

 public:
  explicit MiniCleanCSRGraph(SubgraphMetadata metadata)
      : ImmutableCSRGraph(metadata) {}

  std::unique_ptr<Serialized> Serialize(const TaskRunner& runner) override;

  void Deserialize(const TaskRunner& runner,
                   std::unique_ptr<Serialized>&& serialized) override;

  VertexLabel* GetVertexLabelBasePointer() const {
    return vertex_label_base_pointer_;
  }
  EdgeLabel* GetOutEdgeLabelBasePointer() const {
    return out_edge_label_base_pointer_;
  }

  SubgraphMetadata get_metadata() const { return metadata_; }

  VertexLabel GetVertexLabelByLocalID(VertexID i) const {
    return vertex_label_base_pointer_[i * 2 + 1];
  }

  EdgeLabel* GetOutgoingEdgeLabelsByLocalID(VertexID i) const {
    return out_edge_label_base_pointer_ + out_offset_base_pointer_[i];
  }

  VertexAttributeValue* GetVertexAttributeValuesByLocalID(VertexID i) const {
    return vertex_attribute_value_base_pointer_ +
           vertex_attribute_offset_base_pointer_[i];
  }

 private:
  void ParseSubgraphCSR(const std::vector<OwnedBuffer>& buffer_list);
  void ParseOutedgeLabel(const std::vector<OwnedBuffer>& buffer_list);
  void ParseVertexLabel(const std::vector<OwnedBuffer>& buffer_list);
  void ParseVertexAttribute(const std::vector<OwnedBuffer>& buffer_list);

 private:
  // Vertex labels
  VertexLabel* vertex_label_base_pointer_;
  // Out edge labels
  EdgeLabel* out_edge_label_base_pointer_;
  // Vertex attribute offsets
  VertexID* vertex_attribute_offset_base_pointer_;
  // Vertex attribute values
  //   Using vertex attribute offsets, we can locate the vertex attribute
  //   values.
  //   Here is an example:
  //   - value_ptr[offser_ptr[i]][0]: number of all possible attributes
  //     corresponding to the label of #i vertex.
  //   - value_ptr[offser_ptr[i]][1]: value of #0 attribute.
  //   - value_ptr[offser_ptr[i]][2]: value of #1 attribute.
  VertexAttributeValue* vertex_attribute_value_base_pointer_;
};
}  // namespace sics::graph::miniclean::data_structures::graphs

#endif  // MINICLEAN_GRAPHS_MINICLEAN_CSR_GRAPH_H_