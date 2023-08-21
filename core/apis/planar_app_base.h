#ifndef GRAPH_SYSTEMS_PLANAR_APP_BASE_H
#define GRAPH_SYSTEMS_PLANAR_APP_BASE_H

#include <functional>
#include <type_traits>
#include <vector>

#include "apis/pie.h"
#include "common/multithreading/task_runner.h"
#include "data_structures/serializable.h"
#include "update_stores/bsp_update_store.h"
#include "util/logging.h"

namespace sics::graph::core::apis {

// Planar's extended PIE interfaces for graph applications.
//
// Each application should be defined by inheriting the PIE with a
// concrete graph type, and implement the following methods:
// - PEval: the initial evaluation phase.
// - IncEval: the incremental evaluation phase.
// - Assemble: the final assembly phase.
template <typename GraphType>
class PlanarAppBase : public PIE {
  static_assert(
      std::is_base_of<data_structures::Serializable, GraphType>::value,
      "GraphType must be a subclass of Serializable");

  using VertexData = typename GraphType::VertexData;
  using EdgeData = typename GraphType::EdgeData;
  using VertexID = common::VertexID;
  using VertexIndex = common::VertexIndex;
  using GraphID = common::GraphID;

 public:
  // TODO: add UpdateStore as a parameter, so that PEval, IncEval and Assemble
  //  can access global messages in it.
  PlanarAppBase(
      common::TaskRunner* runner,
      update_stores::BspUpdateStore<VertexData, EdgeData>* update_store,
      data_structures::Serializable* graph)
      : runner_(runner),
        update_store_(update_store),
        graph_(static_cast<GraphType*>(graph)) {}

  ~PlanarAppBase() override = default;

 protected:
  // TODO: implement this here. This function is not intended for overriden.
  void ParallelVertexDo(const std::function<void(int)>& func) {
    LOG_DEBUG("ParallelVertexDo is begin");
    // TODO: task granularity
//    int task_size = FLAGS_task_size;

    common::TaskPackage tasks;
    for (VertexIndex idx = 0; idx < graph_->GetVertexNums(); idx++) {
      auto beginIdx = 0;
      auto endIdx = 0;
      auto task = std::bind([&, beginIdx, endIdx]() {
        for (VertexIndex idx = beginIdx; idx < endIdx; idx++) {
          func(graph_->GetVertexIdByIndex(idx));
        }
      });
      tasks.push_back(task);
    }
    runner_->SubmitSync(tasks);
    // TODO: sync of update_store and graph_ vertex data
    graph_->SyncVertexData();
    LOG_DEBUG("ParallelVertexDo is done");
  }

  // TODO: implement this here. This function is not intended for overriden.
  void ParallelEdgeDo() {
    LOG_DEBUG("EApply is not implemented");

    common::TaskPackage tasks;

    for (VertexIndex src_idx = 0; src_idx < graph_->GetVertexNums();
         src_idx++) {
      auto src = graph_->GetVertexByIndex(src_idx);
      auto src_degree = src->GetOutDegree();
      for (VertexIndex idx = 0; idx < src_degree; idx++) {
        auto dst = graph_->GetVertexByIndex(src->GetOutEdge(idx));
      }
    }

    LOG_DEBUG("EApply is not implemented");
  }

 protected:
  common::TaskRunner* runner_;

  update_stores::BspUpdateStore<VertexData, EdgeData>* update_store_;

  GraphType* graph_;

  // TODO: add UpdateStore as a member here.
};

}  // namespace sics::graph::core::apis

#endif  // GRAPH_SYSTEMS_PLANAR_APP_BASE_H
