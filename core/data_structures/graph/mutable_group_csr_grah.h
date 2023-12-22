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

  void AddSubgraph(MutableCSRGraph<TV, TE>* subgraph) {
    subgraphs_.emplace_back(subgraph);
  }

  std::unique_ptr<Serialized> Serialize(
      const common::TaskRunner& runner) override {
    LOG_INFO("should not be called!");
  }

  void Deserialize(const common::TaskRunner& runner,
                   std::unique_ptr<Serialized>&& serialized) override {
    LOG_INFO("should not be called!");
  }

  // TODO: add corresponding methods

  common::VertexCount GetVertexNums() const {
    common::VertexCount num_vertices = 0;
    for (auto& subgraph : subgraphs_) {
      num_vertices += subgraph->GetVertexNums();
    }
    return num_vertices;
  }

  bool IsInGraph(VertexID id) const {
    bool res = false;
    for (auto& subgraph : subgraphs_) {
      res = subgraph->IsInGraph(id);
    }
    return res;
  }

  int GetGroupNums() const { return subgraphs_.size(); }

  size_t GetOutEdgeNums() const {
    size_t num_edges = 0;
    for (auto& subgraph : subgraphs_) {
      num_edges += subgraph->GetOutEdgeNums();
    }
    return num_edges;
  }

  VertexData ReadLocalVertexDataByID(VertexID id) const {
    for (auto& subgraph : subgraphs_) {
      if (subgraph->IsInGraph(id)) {
        return subgraph->ReadLocalVertexDataByID(id);
      }
    }
    return VertexData();
  }

  bool WriteMinVertexDataByID(VertexID id, VertexData data_new) {
    bool flag = false;
    for (auto& subgraph : subgraphs_) {
      if (subgraph->IsInGraph(id)) {
        flag = subgraph->WriteMinVertexDataByID(id, data_new);
      }
    }
    return flag;
  }

  bool WriteVertexDataByID(VertexID id, VertexData data_new) {
    bool flag = false;
    for (auto& subgraph : subgraphs_) {
      if (subgraph->IsInGraph(id)) {
        flag = subgraph->WriteVertexDataByID(id, data_new);
      }
    }
    return flag;
  }

  void SyncVertexData(bool use_readdata_only = false) {
    for (auto& subgraph : subgraphs_) {
      subgraph->SyncVertexData(use_readdata_only);
    }
  }

  int GetNumSubgraphs() const { return subgraphs_.size(); }

  void MutateGraphEdge(common::TaskRunner* runner) {
    for (auto& subgraph : subgraphs_) {
      subgraph->MutateGraphEdge(runner);
    }
  }

  void DeleteEdge(GraphID gid, VertexID id, EdgeIndex index) {
    subgraphs_.at(gid)->DeleteEdge(id, index);
  }

  void SyncAllSubgraphData(common::TaskRunner* runner) {
    if (subgraphs_.size() == 1) {
      return;
    }

    for (int i = 0; i < subgraphs_.size(); i++) {
      common::TaskPackage tasks;
      auto subgraph = subgraphs_[i];
      size_t task_num = parallelism_ * task_package_factor_;
      uint32_t task_size = ceil(
          (double)(subgraph->metadata_->num_vertices->num_vertices) / task_num);
      task_size = task_size < 2 ? 2 : task_size;
      VertexIndex begin_index = 0, end_index = 0;
      for (; begin_index < subgraph->metadata_->num_vertices;) {
        end_index += task_size;
        if (end_index > subgraph->metadata_->num_vertices) {
          end_index = subgraph->metadata_->num_vertices;
        }
        auto task = [subgraph]() {
          for (int j = 0; j < subgraph->GetVertexNums(); j++) {
            //            auto id = subgraph->GetVertexIDByIndex(i);
            //            auto data = subgraph->ReadLocalVertexDataByID(id);
          }
        };
        tasks.push_back(task);
        begin_index = end_index;
      }
      runner->SubmitSync(tasks);
    }
  }

  // log methods
  void LogVertexData() const {
    for (auto& subgraph : subgraphs_) {
      subgraph->LogVertexData();
    }
  }

  void LogEdges() const {
    for (auto& subgraph : subgraphs_) {
      subgraph->LogEdges();
    }
  }

  void LogGraphInfo() const {
    for (auto& subgraph : subgraphs_) {
      subgraph->LogGraphInfo();
    }
  }

  void LogIndexInfo() {
    for (auto subgraph : subgraphs_) {
      subgraph->LogIndexInfo();
    }
  }

  MutableCSRGraph<VertexData, EdgeData>* GetSubgraph(common::GraphID id) {
    return subgraphs_[id];
  }

 public:
  std::vector<MutableCSRGraph<VertexData, EdgeData>*> subgraphs_;

  // configs
  uint32_t parallelism_ = 1;
  uint32_t task_package_factor_ = 50;
};

typedef MutableGroupCSRGraph<common::Uint32VertexDataType,
                             common::DefaultEdgeDataType>
    MutableGroupCSRGraphUInt32;
typedef MutableGroupCSRGraph<common::Uint16VertexDataType,
                             common::DefaultEdgeDataType>
    MutableGroupCSRGraphUInt16;

}  // namespace sics::graph::core::data_structures::graph

#endif  // GRAPH_SYSTEMS_CORE_DATA_STRUCTURES_GRAPH_MUTABLE_GROUP_CSR_GRAH_H_
