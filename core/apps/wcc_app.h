#ifndef GRAPH_SYSTEMS_CORE_APPS_WCC_APP_H_
#define GRAPH_SYSTEMS_CORE_APPS_WCC_APP_H_

#include "apis/planar_app_base.h"
#include "data_structures/graph/mutable_csr_graph.h"

namespace sics::graph::core::apps {

// TODO: use real graph replace this
// A dummy graph type for testing purposes. Do *NOT* use it in production.
class TestGraph : public data_structures::Serializable {
 public:
  using VertexData = int;
  using EdgeData = int;
  using VertexID = common::VertexID;
  using VertexIndex = common::VertexIndex;
  using EdgeIndex = common::EdgeIndex;
  using GraphID = common::GraphID;

  TestGraph();
  ~TestGraph() = default;

  std::unique_ptr<data_structures::Serialized> Serialize(
      const common::TaskRunner& runner) final;

  // Deserialize the `Serializable` object from a `Serialized` object.
  // This function will submit the deserialization task to the given TaskRunner
  void Deserialize(
      const common::TaskRunner& runner,
      std::unique_ptr<data_structures::Serialized>&& serialized) final;

  std::string get_status() const { return status_; }
  void set_status(const std::string& new_status) { status_ = new_status; }

  bool Write(common::VertexID id, VertexData data) { return true; }

  VertexData* Read(common::VertexID id) { return nullptr; }

  int GetVertexNums() { return 4; }

  int GetVertexIdByIndex(int index) { return index; }

  void SyncVertexData() {}

  void MutateGraphEdge() {}

  size_t GetOutDegree(VertexIndex i) { return 2; }

  size_t GetOutEdge(VertexIndex i, VertexIndex j) { return i % 2 == 0 ? 1 : 0; }

  size_t GetOutOffset(VertexIndex i) { return 2 * (i - 1); }

 private:
  std::string status_;
};

struct WccVertexData {
  int p;
  WccVertexData() = default;
  WccVertexData(int p) : p(p) {}
};

using CSRGraph = data_structures::graph::MutableCSRGraph<int, int>;

class WCCApp : public apis::PlanarAppBase<CSRGraph> {
  using VertexData = typename CSRGraph::VertexData;
  using EdgeData = typename CSRGraph::EdgeData;
  using EdgeIndex = common::EdgeIndex;

 public:
  explicit WCCApp(
      common::TaskRunner* runner,
      update_stores::BspUpdateStore<VertexData, EdgeData>* update_store,
      data_structures::Serializable* graph);
  ~WCCApp() override = default;

  void PEval() final;
  void IncEval() final;
  void Assemble() final;

 private:
  void init(common::VertexID id) {
    // TODO: init for vertex
    graph_->GetOutDegreeByIndex(id);
    auto a = graph_->GetVertxDataByIndex(id);

    LOG_INFO("test for init");
  }

  void graft(common::VertexID src_id, common::VertexID dst_id) {
    //    auto t = update_store_->Read(src_id);
    //    graph_->Write(src_id, *t);
    // TODO: graft for vertex
    LOG_INFO("test for graft");
  }

  void contract(common::VertexID src_id, common::VertexID dst_id,
                EdgeIndex idx) {
    //    auto t = update_store_->Read(src_id);
    //    graph_->Write(src_id, *t);
    // TODO: contract for vertex
    LOG_INFO("test for contract");
  }
};

}  // namespace sics::graph::core::apps

#endif  // GRAPH_SYSTEMS_CORE_APPS_WCC_APP_H_
