#ifndef GRAPH_SYSTEMS_PLANAR_APP_BASE_H
#define GRAPH_SYSTEMS_PLANAR_APP_BASE_H

#include <functional>
#include <type_traits>
#include <vector>

#include "apis/pie.h"
#include "common/config.h"
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

  using GraphID = common::GraphID;
  using VertexID = common::VertexID;
  using VertexIndex = common::VertexIndex;
  using EdgeIndex = common::EdgeIndex;

 public:
  PlanarAppBase() = default;
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

  virtual void AppInit(
      common::TaskRunner* runner,
      update_stores::BspUpdateStore<VertexData, EdgeData>* update_store) {
    runner_ = runner;
    update_store_ = update_store;
  }

  virtual void SetRuntimeGraph(GraphType* graph) { graph_ = graph; }

 protected:
  // Parallel execute vertex_func in task_size chunks.
  void ParallelVertexDo(const std::function<void(VertexID)>& vertex_func) {
    LOG_DEBUG("ParallelVertexDo is begin");
    uint32_t task_size = GetTaskSize(graph_->GetVertexNums());
    common::TaskPackage tasks;
    VertexIndex begin_index = 0, end_index = 0;
    for (; begin_index < graph_->GetVertexNums();) {
      end_index += task_size;
      if (end_index > graph_->GetVertexNums())
        end_index = graph_->GetVertexNums();
      auto task = std::bind([&, begin_index, end_index]() {
        for (VertexIndex idx = begin_index; idx < end_index; idx++) {
          vertex_func(graph_->GetVertexIDByIndex(idx));
        }
      });
      tasks.push_back(task);
      begin_index = end_index;
    }
    runner_->SubmitSync(tasks);
    // TODO: sync of update_store and graph_ vertex data
    graph_->SyncVertexData();
    LOG_DEBUG("ParallelVertexDo is done");
  }

  // Parallel execute edge_func in task_size chunks.
  void ParallelEdgeDo(
      const std::function<void(VertexID, VertexID)>& edge_func) {
    LOG_DEBUG("ParallelEdgeDo is begin");
    uint32_t task_size = GetTaskSize(graph_->GetVertexNums());
    common::TaskPackage tasks;
    VertexIndex begin_index = 0, end_index = 0;
    for (; begin_index < graph_->GetVertexNums();) {
      end_index += task_size;
      if (end_index > graph_->GetVertexNums())
        end_index = graph_->GetVertexNums();
      auto task = std::bind([&, begin_index, end_index]() {
        for (VertexIndex i = begin_index; i < end_index; i++) {
          for (VertexIndex j = 0; j < graph_->GetOutDegreeByIndex(i); j++) {
            LOGF_INFO("edge_func: {}, {}", graph_->GetVertexIDByIndex(i),
                      graph_->GetOneOutEdge(i, j));
            edge_func(graph_->GetVertexIDByIndex(i),
                      graph_->GetOneOutEdge(i, j));
          }
        }
      });
      tasks.push_back(task);
      begin_index = end_index;
    }
    runner_->SubmitSync(tasks);
    graph_->SyncVertexData();
    LOG_DEBUG("ParallelEdgeDo is done");
  }

  // Parallel execute edge_delete_func in task_size chunks.
  void ParallelEdgeMutateDo(
      const std::function<void(VertexID, VertexID, EdgeIndex)>& edge_del_func) {
    LOG_DEBUG("ParallelEdgeDelDo is begin");
    uint32_t task_size = GetTaskSize(graph_->GetVertexNums());
    common::TaskPackage tasks;
    VertexIndex begin_index = 0, end_index = 0;
    for (; begin_index < graph_->GetVertexNums();) {
      end_index += task_size;
      if (end_index > graph_->GetVertexNums())
        end_index = graph_->GetVertexNums();
      auto task = std::bind([&, begin_index, end_index]() {
        for (VertexIndex i = begin_index; i < end_index; i++) {
          for (VertexIndex j = 0; j < graph_->GetOutDegreeByIndex(i); j++) {
            LOGF_INFO("edge_del_func: {}, {}, {}",
                      graph_->GetVertexIDByIndex(i),
                      graph_->GetOneOutEdge(i, j),
                      graph_->GetOutOffsetByIndex(i) + j);
            edge_del_func(graph_->GetVertexIDByIndex(i),
                          graph_->GetOneOutEdge(i, j),
                          graph_->GetOutOffsetByIndex(i) + j);
          }
        }
      });
      tasks.push_back(task);
      begin_index = end_index;
    }
    runner_->SubmitSync(tasks);
    graph_->MutateGraphEdge(runner_);
    LOG_DEBUG("ParallelEdgedelDo is done");
  }

  uint32_t GetTaskSize(VertexID max_vid) const {
    uint32_t task_size =
        max_vid / common::Configurations::Get()->max_task_package;
    return task_size < 2 ? 2 : task_size;
  }

 protected:
  common::TaskRunner* runner_;

  update_stores::BspUpdateStore<VertexData, EdgeData>* update_store_;

  GraphType* graph_;

  // TODO: add UpdateStore as a member here.
};

}  // namespace sics::graph::core::apis

#endif  // GRAPH_SYSTEMS_PLANAR_APP_BASE_H
