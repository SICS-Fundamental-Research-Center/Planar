#ifndef GRAPH_SYSTEMS_CORE_DATA_STRUCTURES_GRAPH_MUTABLE_GROUP_CSR_GRAH_H_
#define GRAPH_SYSTEMS_CORE_DATA_STRUCTURES_GRAPH_MUTABLE_GROUP_CSR_GRAH_H_

#include <vector>

#include "data_structures/graph/mutable_csr_graph.h"

namespace sics::graph::core::data_structures::graph {

// TV : type of vertexData; TE : type of EdgeData
template <typename TV, typename TE>
class MutableGroupCSRGraph : public Serializable {
  using GraphID = common::GraphID;
  using VertexID = common::VertexID;
  using VertexIndex = common::VertexIndex;
  using EdgeIndex = common::EdgeIndex;
  using SerializedMutableCSRGraph =
      data_structures::graph::SerializedMutableCSRGraph;

 public:
  using VertexData = TV;
  using EdgeData = TE;
  MutableGroupCSRGraph() = default;
  ~MutableGroupCSRGraph() override = default;

  std::unique_ptr<Serialized> Serialize(
      const common::TaskRunner& runner) override {
    // TODO: serialized all sub-graph
  }

  void Deserialize(const common::TaskRunner& runner,
                   std::unique_ptr<Serialized>&& serialized) override {
    // TODO: deserialize all sub-graph
  }

  // TODO: add corresponding methods

  VertexData ReadLocalVertexDataByID(VertexID id) const {
    for (auto& subgraph : subgraphs_) {
      return subgraph.ReadLocalVertexDataByID(id);
    }
  }

  bool WriteMinVertexDataByID(VertexID id, VertexData data_new) {
    bool flag = false;
    for (auto& subgraph : subgraphs_) {
      flag = subgraph.WriteMinVertexDataByID(id, data_new);
    }
    return flag;
  }

  bool WriteVertexDataByID(VertexID id, VertexData data_new) {
    bool flag = false;
    for (auto& subgraph : subgraphs_) {
      flag = subgraph.WriteVertexDataByID(id, data_new);
    }
    return flag;
  }

  int GetNumSubgraphs() const { return subgraphs_.size(); }

  // log methods
  void LogVertexData() const {
    for (auto& subgraph : subgraphs_) {
      subgraph.LogVertexData();
    }
  }

  void LogEdges() const {
    for (auto& subgraph : subgraphs_) {
      subgraph.LogEdges();
    }
  }

  void LogGraphInfo(VertexID id) const {
    for (auto& subgraph : subgraphs_) {
      subgraph.LogGraphInfo(id);
    }
  }

 private:
  SubgraphMetadata group_metadata_;
  std::vector<MutableCSRGraph<VertexData, EdgeData>> subgraphs_;
};

// TODO: typedef for uint32 and uint16

}  // namespace sics::graph::core::data_structures::graph

#endif  // GRAPH_SYSTEMS_CORE_DATA_STRUCTURES_GRAPH_MUTABLE_GROUP_CSR_GRAH_H_
