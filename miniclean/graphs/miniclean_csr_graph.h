#ifndef MINICLEAN_GRAPHS_MINICLEAN_CSR_GRAPH_H_
#define MINICLEAN_GRAPHS_MINICLEAN_CSR_GRAPH_H_

#include "core/data_structures/graph/immutable_csr_graph.h"
#include "graphs/miniclean_csr_graph_config.h"

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

 public:
  explicit MinicleanCSRGraph(const GraphID gid) : ImmutableCSRGraph(gid) {}
  MinicleanCSRGraph(const GraphID gid, MinicleanCSRGraphConfig&& csr_config)
      : ImmutableCSRGraph(gid) {
    this->csr_config_ = std::move(csr_config);
  }

  std::unique_ptr<Serialized> Serialize(const TaskRunner& runner) override;

  void Deserialize(const TaskRunner& runner,
                   std::unique_ptr<Serialized>&& serialized) override;

 protected:
  void ParseSubgraphCSR(const std::list<OwnedBuffer>& buffer_list) override;

 private:
  MinicleanCSRGraphConfig csr_config_;
};
}  // namespace sics::graph::miniclean::graphs

#endif  // MINICLEAN_GRAPHS_MINICLEAN_CSR_GRAPH_H_