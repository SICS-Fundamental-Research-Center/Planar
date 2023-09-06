#ifndef GRAPH_SYSTEMS_CORE_DATA_STRUCTURES_GRAPH_MUTABLE_GROUP_CSR_GRAH_H_
#define GRAPH_SYSTEMS_CORE_DATA_STRUCTURES_GRAPH_MUTABLE_GROUP_CSR_GRAH_H_

#include "data_structures/graph/mutable_csr_graph.h"

namespace sics::graph::core::data_structures::graph {

// TV : type of vertexData; TE : type of EdgeData
template <typename TV, typename TE>
class MutableGroupCSRGraph : public Serializable {
 public:
  MutableGroupCSRGraph() = default;
  ~MutableGroupCSRGraph() override {
    // TODO: delete pointer malloc in deserialize
  }

  std::unique_ptr<Serialized> Serialize(
      const common::TaskRunner& runner) override {
    // TODO: serialized all sub-graph
  }

  void Deserialize(const common::TaskRunner& runner,
                   std::unique_ptr<Serialized>&& serialized) override {

  }

  //TODO: add corresponding methods

 private:
  SubgraphMetadata group_metadata_;
};

}  // namespace sics::graph::core::data_structures::graph

#endif  // GRAPH_SYSTEMS_CORE_DATA_STRUCTURES_GRAPH_MUTABLE_GROUP_CSR_GRAH_H_
