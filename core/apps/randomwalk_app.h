#ifndef GRAPH_SYSTEMS_CORE_APPS_RANDOMWALK_APP_H_
#define GRAPH_SYSTEMS_CORE_APPS_RANDOMWALK_APP_H_

#include <cstdlib>

#include "apis/planar_app_base.h"
#include "common/types.h"
#include "data_structures/graph/mutable_csr_graph.h"

namespace sics::graph::core::apps {

using CSRGraph = data_structures::graph::MutableCSRGraphUInt32;

class RandomWalkApp : public apis::PlanarAppBase<CSRGraph> {
  using VertexID = common::VertexID;

 public:
  using VertexData = CSRGraph::VertexData;
  using EdgeData = CSRGraph::EdgeData;
  RandomWalkApp() = default;
  explicit RandomWalkApp(
      common::TaskRunner* runner,
      update_stores::BspUpdateStore<VertexData, EdgeData>* update_store,
      data_structures::Serializable* graph);

  void AppInit(common::TaskRunner* runner,
               update_stores::BspUpdateStore<VertexData, EdgeData>*
                   update_store) override {
    apis::PlanarAppBase<CSRGraph>::AppInit(runner, update_store);
    //    active_.Init(update_store->GetMessageCount());
    //    active_next_.Init(update_store->GetMessageCount());
    walk_length_ = core::common::Configurations::Get()->walk;
    num_vertices_ = update_store->GetMessageCount();
    uint64_t size = num_vertices_ * walk_length_;
    matrix_ = new uint32_t[size];
    LOGF_INFO("matrix size : {}", size);
  }

  ~RandomWalkApp() override { delete[] matrix_; };

  void PEval() final;
  void IncEval() final;
  void Assemble() final;

 private:
  void Init(VertexID id);

  void Sample(VertexID id);

  void Walk(VertexID id);

  uint32_t GetRandom(uint32_t max) {
    uint32_t res = rand() % max;
    return res;
  }

  uint32_t* GetRoad(VertexID id) { return matrix_ + id * walk_length_; }

 private:
  // configs
  uint32_t walk_length_ = 5;
  uint32_t* matrix_ = nullptr;
  uint32_t num_vertices_ = 0;
};

}  // namespace sics::graph::core::apps

#endif  // GRAPH_SYSTEMS_CORE_APPS_RANDOMWALK_APP_H_
