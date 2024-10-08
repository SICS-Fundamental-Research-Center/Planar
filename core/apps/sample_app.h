#ifndef GRAPH_SYSTEMS_SAMPLE_APP_H
#define GRAPH_SYSTEMS_SAMPLE_APP_H

#include <memory>
#include <string>

#include "apis/planar_app_base.h"

namespace sics::graph::core::apps {

// TODO: test for paralleldo functions
// A dummy graph type for testing purposes. Do *NOT* use it in production.
class DummyGraph : public data_structures::Serializable {
 public:
  using VertexData = int;
  using EdgeData = int;
  using VertexID = common::VertexID;
  using VertexIndex = common::VertexIndex;
  using EdgeIndex = common::EdgeIndex;
  using GraphID = common::GraphID;

  DummyGraph();
  ~DummyGraph() = default;

  std::unique_ptr<data_structures::Serialized> Serialize(
      const common::TaskRunner& runner) final;

  // Deserialize the `Serializable` object from a `Serialized` object.
  // This function will submit the deserialization task to the given TaskRunner
  void Deserialize(
      const common::TaskRunner& runner,
      std::unique_ptr<data_structures::Serialized>&& serialized) final;

  std::string get_status() const { return status_; }
  void set_status(const std::string& new_status) { status_ = new_status; }

 private:
  std::string status_;
};

// A sample Planar app that does nothing.
class SampleApp : public apis::PlanarAppBase<DummyGraph> {
  using VertexData = DummyGraph::VertexData;
  using EdgeData = DummyGraph::EdgeData;
  using EdgeIndex = DummyGraph::EdgeIndex;

 public:
  explicit SampleApp(
      common::TaskRunner* runner,
      update_stores::BspUpdateStore<DummyGraph::VertexData,
                                    DummyGraph::EdgeData>* update_store,
      data_structures::Serializable* graph);
  ~SampleApp() override = default;

  void PEval() final;
  void IncEval() final;
  void Assemble() final;
  void SetCurrentGid(common::GraphID gid) final {}
  size_t IsActive() final {}
  void SetRound(int round) final {}
  void SetInActive() {}

  // Note: all Planar apps must implement this static method.
  // PlanarAppFactory will use this method to create an app instance.
  static std::unique_ptr<apis::PlanarAppBase<DummyGraph>> Create(
      common::TaskRunner* runner,
      update_stores::BspUpdateStore<DummyGraph::VertexData,
                                    DummyGraph::EdgeData>* update_store,
      data_structures::Serializable* graph) {
    return std::make_unique<SampleApp>(runner, update_store, graph);
  }

  // Note: all Planar apps must implement this static method, with a unique
  // name.
  static std::string GetAppName() { return "SampleApp"; }

 private:
  // Note: all PIE apps must have static member, and get initialized in the
  // accompanying .cpp file. See sample_app.cpp.
  static bool registered_;
};

}  // namespace sics::graph::core::apps

#endif  // GRAPH_SYSTEMS_SAMPLE_APP_H
