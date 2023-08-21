#ifndef GRAPH_SYSTEMS_CORE_APPS_WCC_APP_H_
#define GRAPH_SYSTEMS_CORE_APPS_WCC_APP_H_

#include "apis/planar_app_base.h"

namespace sics::graph::core::apps {

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

  int GetVertexNums() { return 2; }

  int GetVertexIdByIndex(int index) { return 0; }

  void SyncVertexData() {}

  void MutateGraphEdge() {}

  size_t GetOutDegree(VertexIndex i) { return 0; }

  size_t GetOutEdge(VertexIndex i, VertexIndex j) { return 0; }

  size_t GetOutOffset(VertexIndex i) { return 0; }

 private:
  std::string status_;
};

class WCCApp : public apis::PlanarAppBase<TestGraph> {
  using VertexData = TestGraph::VertexData;
  using EdgeData = TestGraph::EdgeData;
  using EdgeIndex = TestGraph::EdgeIndex;

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
    graph_->GetOutDegree(id);
    LOG_INFO("test for init");
  }

  void graft(common::VertexID src_id, common::VertexID dst_id) {
    auto t = update_store_->Read(src_id);
    graph_->Write(src_id, *t);
  }

  void contract(common::VertexID src_id, common::VertexID dst_id,
                EdgeIndex idx) {
    auto t = update_store_->Read(src_id);
    graph_->Write(src_id, *t);
  }

 private:
  static bool registered_;
};

}  // namespace sics::graph::core::apps

#endif  // GRAPH_SYSTEMS_CORE_APPS_WCC_APP_H_
