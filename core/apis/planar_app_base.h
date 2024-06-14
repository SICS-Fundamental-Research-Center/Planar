#ifndef GRAPH_SYSTEMS_PLANAR_APP_BASE_H
#define GRAPH_SYSTEMS_PLANAR_APP_BASE_H

#include <functional>
#include <type_traits>
#include <vector>

#include "apis/pie.h"
#include "common/blocking_queue.h"
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
  PlanarAppBase()
      : runner_(nullptr),
        update_store_(nullptr),
        graph_(nullptr),
        parallelism_(common::Configurations::Get()->parallelism),
        task_package_factor_(
            common::Configurations::Get()->task_package_factor),
        app_type_(common::Configurations::Get()->application) {
    use_readdata_only_ = app_type_ == common::Coloring;
    LOGF_INFO("vertex data sync: {}", !use_readdata_only_);
    LOGF_INFO("use read data only: {}", use_readdata_only_);
  }
  // TODO: add UpdateStore as a parameter, so that PEval, IncEval and Assemble
  //  can access global messages in it.
  PlanarAppBase(
      common::TaskRunner* runner,
      update_stores::BspUpdateStore<VertexData, EdgeData>* update_store,
      data_structures::Serializable* graph)
      : runner_(runner),
        update_store_(update_store),
        graph_(static_cast<GraphType*>(graph)),
        parallelism_(common::Configurations::Get()->parallelism),
        task_package_factor_(
            common::Configurations::Get()->task_package_factor),
        app_type_(common::Configurations::Get()->application) {
    use_readdata_only_ = app_type_ == common::Coloring;
    LOGF_INFO("vertex data sync: {}", !use_readdata_only_);
    LOGF_INFO("use read data only: {}", use_readdata_only_);
  }

  ~PlanarAppBase() override = default;

  virtual void AppInit(
      common::TaskRunner* runner,
      update_stores::BspUpdateStore<VertexData, EdgeData>* update_store) {
    runner_ = runner;
    update_store_ = update_store;
    //    active_.Init(update_store->GetMessageCount());
    //    active_next_.Init(update_store->GetMessageCount());
  }

  virtual void SetRound(int round) { round_ = round; }

  virtual void SetRuntimeGraph(GraphType* graph) { graph_ = graph; }

 protected:
  // Parallel execute vertex_func in task_size chunks.
  void ParallelVertexDo(const std::function<void(VertexID)>& vertex_func) {
    LOG_DEBUG("ParallelVertexDo is begin");
    uint32_t task_size = GetTaskSize(graph_->GetVertexNums());
    common::TaskPackage tasks;
    tasks.reserve(parallelism_ * task_package_factor_);
    VertexIndex begin_index = 0, end_index = 0;
    for (; begin_index < graph_->GetVertexNums();) {
      end_index += task_size;
      if (end_index > graph_->GetVertexNums())
        end_index = graph_->GetVertexNums();
      auto task = [&vertex_func, this, begin_index, end_index]() {
        for (VertexIndex idx = begin_index; idx < end_index; idx++) {
          vertex_func(graph_->GetVertexIDByIndex(idx));
        }
      };
      tasks.push_back(task);
      begin_index = end_index;
    }
    //    LOGF_INFO("ParallelVertexDo task_size: {}, num tasks: {}", task_size,
    //              tasks.size());
    runner_->SubmitSync(tasks);
    // TODO: sync of update_store and graph_ vertex data
    LOG_INFO("task finished");
    graph_->SyncVertexData(use_readdata_only_);
    LOG_INFO("ParallelVertexDo is done");
  }

  void ParallelVertexDoByIndex(
      const std::function<void(VertexID)>& vertex_func) {
    LOG_DEBUG("ParallelVertexDoByIndex is begin");
    uint32_t task_size = GetTaskSize(graph_->GetVertexNums());
    common::TaskPackage tasks;
    tasks.reserve(parallelism_ * task_package_factor_);
    VertexIndex begin_index = 0, end_index = 0;
    for (; begin_index < graph_->GetVertexNums();) {
      end_index += task_size;
      if (end_index > graph_->GetVertexNums())
        end_index = graph_->GetVertexNums();
      auto task = [&vertex_func, begin_index, end_index]() {
        for (VertexIndex idx = begin_index; idx < end_index; idx++) {
          vertex_func(idx);
        }
      };
      tasks.push_back(task);
      begin_index = end_index;
    }
    //    LOGF_INFO("ParallelVertexDo task_size: {}, num tasks: {}", task_size,
    //              tasks.size());
    runner_->SubmitSync(tasks);
    // TODO: sync of update_store and graph_ vertex data
    graph_->SyncVertexData(use_readdata_only_);
    LOG_DEBUG("ParallelVertexDoByIndex is done");
  }

  void ParallelVertexDoWithActive(
      const std::function<void(VertexID)>& vertex_func) {
    LOG_DEBUG("ParallelVertexDoWithActive is begin");
    uint32_t task_size = 64 * 100;

    common::TaskPackage tasks;
    tasks.reserve((graph_->GetVertexNums() / 64 / 100) + 1);
    VertexIndex begin_index = 0, end_index = 0;
    for (; begin_index < graph_->GetVertexNums();) {
      end_index += task_size;
      if (end_index > graph_->GetVertexNums())
        end_index = graph_->GetVertexNums();
      auto task = [&vertex_func, this, begin_index, end_index]() {
        for (VertexIndex idx = begin_index; idx < end_index;) {
          if (active_.GetBit64(idx)) {
            for (int j = 0; j < 64 && (idx + j) < end_index; j++) {
              if (active_.GetBit(idx + j))
                vertex_func(graph_->GetVertexIDByIndex(idx + j));
            }
          }
          idx += 64;
        }
        //        for (VertexIndex idx = begin_index; idx < end_index; idx++) {
        //          auto id = graph_->GetVertexIDByIndex(idx);
        //          if (active_.GetBit(idx)) {
        //            vertex_func(id);
        //          }
        //        }
      };
      tasks.push_back(task);
      begin_index = end_index;
    }
    //    LOGF_INFO("ParallelVertexDo task_size: {}, num tasks: {}", task_size,
    //              tasks.size());
    runner_->SubmitSync(tasks);
    // TODO: sync of update_store and graph_ vertex data
    graph_->SyncVertexData(use_readdata_only_);
    LOG_DEBUG("ParallelVertexDoWithActive is done");
  }

  // Parallel execute vertex_func in task_size chunks.
  void ParallelVertexDoStep(const std::function<void(VertexID)>& vertex_func) {
    LOG_DEBUG("ParallelVertexDoStep begins");
    // auto task_size = GetTaskSize(graph_->GetVertexNums());
    common::TaskPackage tasks;
    tasks.reserve(parallelism_ * task_package_factor_);
    VertexIndex end = graph_->GetVertexNums();
    for (uint32_t i = 0; i < parallelism_; i++) {
      auto index = end - 1 - i;
      auto task = [&vertex_func, this, index]() {
        for (int idx = index; idx >= 0;) {
          vertex_func(graph_->GetVertexIDByIndex(idx));
          idx -= parallelism_;
        }
      };
      tasks.push_back(task);
    }
    //    LOGF_INFO("ParallelVertexDoStep task_size: {}, num tasks: {}",
    //    task_size,
    //              tasks.size());
    runner_->SubmitSync(tasks);
    // TODO: sync of update_store and graph_ vertex data
    graph_->SyncVertexData(use_readdata_only_);
    LOG_DEBUG("ParallelVertexDoStep done");
  }

  // Parallel execute edge_func in task_size chunks.
  void ParallelEdgeDo(
      const std::function<void(VertexID, VertexID)>& edge_func) {
    LOG_DEBUG("ParallelEdgeDo begins");
    uint32_t task_size = GetTaskSize(graph_->GetVertexNums());
    common::TaskPackage tasks;
    tasks.reserve(parallelism_ * task_package_factor_);
    int count = 0;
    VertexIndex begin_index = 0, end_index = 0;
    for (; begin_index < graph_->GetVertexNums();) {
      end_index += task_size;
      if (end_index > graph_->GetVertexNums())
        end_index = graph_->GetVertexNums();
      auto task = [&edge_func, this, begin_index, end_index]() {
        for (VertexIndex i = begin_index; i < end_index; i++) {
          auto degree = graph_->GetOutDegreeByIndex(i);
          if (degree != 0) {
            VertexID* outEdges = graph_->GetOutEdgesByIndex(i);
            for (VertexIndex j = 0; j < degree; j++) {
              //            LOGF_INFO("edge_func: {}, {}",
              //            graph_->GetVertexIDByIndex(i),
              //                      graph_->GetOneOutEdge(i, j));
              edge_func(graph_->GetVertexIDByIndex(i), outEdges[j]);
            }
          }
        }
      };
      tasks.push_back(task);
      begin_index = end_index;
      count++;
    }
    //    LOGF_INFO("task_size: {}, num tasks: {}. left edges: {}", task_size,
    //    count,
    //              graph_->GetOutEdgeNums());
    LOGF_INFO("task num: {}", tasks.size());
    runner_->SubmitSync(tasks);
    graph_->SyncVertexData(use_readdata_only_);
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
      auto task = [&edge_del_func, this, begin_index, end_index]() {
        for (VertexIndex i = begin_index; i < end_index; i++) {
          auto degree = graph_->GetOutDegreeByIndex(i);
          if (degree != 0) {
            EdgeIndex outOffset_base = graph_->GetOutOffsetByIndex(i);
            VertexID* outEdges = graph_->GetOutEdgesByIndex(i);
            for (VertexIndex j = 0; j < degree; j++) {
              //            LOGF_INFO("edge_del_func: {}, {}, {}",
              //                      graph_->GetVertexIDByIndex(i),
              //                      graph_->GetOneOutEdge(i, j),
              //                      graph_->GetOutOffsetByIndex(i) + j);
              edge_del_func(graph_->GetVertexIDByIndex(i), outEdges[j],
                            outOffset_base + j);
            }
          }
        }
      };
      tasks.push_back(task);
      begin_index = end_index;
    }
    runner_->SubmitSync(tasks);
    graph_->MutateGraphEdge(runner_);
    LOG_DEBUG("ParallelEdgedelDo is done");
  }

  size_t GetTaskSize(VertexID max_vid) const {
    auto task_num = parallelism_ * task_package_factor_;
    size_t task_size = ceil((double)max_vid / task_num);
    return task_size < 2 ? 2 : task_size;
  }

  void SyncActive() {
    std::swap(active_, active_next_);
    active_next_.Clear();
  }

 protected:
  common::TaskRunner* runner_;

  update_stores::BspUpdateStore<VertexData, EdgeData>* update_store_;

  GraphType* graph_;

  int round_ = 0;

  common::Bitmap active_;
  common::Bitmap active_next_;

  common::BlockingQueue<common::BlockID> queue_;

  // configs
  const uint32_t parallelism_;
  const uint32_t task_package_factor_;
  const common::ApplicationType app_type_;
  bool use_readdata_only_ = true;
  // TODO: add UpdateStore as a member here.
};

}  // namespace sics::graph::core::apis

#endif  // GRAPH_SYSTEMS_PLANAR_APP_BASE_H
