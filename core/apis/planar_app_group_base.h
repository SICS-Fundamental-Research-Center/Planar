#ifndef GRAPH_SYSTEMS_CORE_APIS_PLANAR_APP_GROUP_BASE_H_
#define GRAPH_SYSTEMS_CORE_APIS_PLANAR_APP_GROUP_BASE_H_

#include <functional>
#include <type_traits>
#include <vector>

#include "apis/pie.h"
#include "common/config.h"
#include "common/multithreading/task_runner.h"
#include "data_structures/graph/mutable_csr_graph.h"
#include "data_structures/graph/mutable_group_csr_grah.h"
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
class PlanarAppGroupBase : public PIE {
  static_assert(
      std::is_base_of<data_structures::Serializable, GraphType>::value,
      "GraphType must be a subclass of Serializable");
  using VertexData = typename GraphType::VertexData;
  using EdgeData = typename GraphType::EdgeData;

  using GraphID = common::GraphID;
  using VertexID = common::VertexID;
  using VertexIndex = common::VertexIndex;
  using EdgeIndex = common::EdgeIndex;
  using MutableCSRGraphUInt32 = data_structures::graph::MutableCSRGraphUInt32;
  using MutableGroupCSRGraphUInt32 =
      data_structures::graph::MutableGroupCSRGraphUInt32;

 public:
  PlanarAppGroupBase()
      : runner_(nullptr),
        update_store_(nullptr),
        graph_(nullptr),
        parallelism_(common::Configurations::Get()->parallelism),
        task_package_factor_(
            common::Configurations::Get()->task_package_factor),
        app_type_(common::Configurations::Get()->application) {
    use_readdata_only_ = app_type_ == common::Coloring;
    LOGF_INFO("vertex data sync: {}", !use_readdata_only_);
  }
  // TODO: add UpdateStore as a parameter, so that PEval, IncEval and Assemble
  //  can access global messages in it.
  PlanarAppGroupBase(
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
  }

  ~PlanarAppGroupBase() override = default;

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
    auto group_graphs = (MutableGroupCSRGraphUInt32*)(graph_);
    uint32_t task_size = GetTaskSize(group_graphs->GetVertexNums());
    common::TaskPackage tasks;
    tasks.reserve(parallelism_ * task_package_factor_);

    for (int i = 0; i < group_graphs->GetGroupNums(); i++) {
      auto graph = group_graphs->GetSubgraph(i);
      VertexIndex begin_index = 0, end_index = 0;
      for (; begin_index < graph->GetVertexNums();) {
        end_index += task_size;
        if (end_index > graph->GetVertexNums())
          end_index = graph->GetVertexNums();
        auto task = [&vertex_func, graph, begin_index, end_index]() {
          for (VertexIndex idx = begin_index; idx < end_index; idx++) {
            vertex_func(graph->GetVertexIDByIndex(idx));
          }
        };
        tasks.push_back(task);
        begin_index = end_index;
      }
      //    LOGF_INFO("ParallelVertexDo task_size: {}, num tasks: {}",
      //    task_size,
      //              tasks.size());
    }
    runner_->SubmitSync(tasks);
    // TODO: sync of update_store and graph_ vertex data
    group_graphs->SyncVertexData(use_readdata_only_);
    LOG_DEBUG("ParallelVertexDo is done");
  }

  // Parallel execute edge_func in task_size chunks.
  void ParallelEdgeDo(
      const std::function<void(VertexID, VertexID)>& edge_func) {
    LOG_DEBUG("ParallelEdgeDo is begin");
    auto group_graphs = (MutableGroupCSRGraphUInt32*)(graph_);
    uint32_t task_size = GetTaskSize(group_graphs->GetVertexNums());
    common::TaskPackage tasks;
    tasks.reserve(parallelism_ * task_package_factor_);
    int count = 0;

    for (int i = 0; i < group_graphs->GetGroupNums(); i++) {
      auto graph = group_graphs->GetSubgraph(i);
      VertexIndex begin_index = 0, end_index = 0;
      for (; begin_index < graph->GetVertexNums();) {
        end_index += task_size;
        if (end_index > graph->GetVertexNums())
          end_index = graph->GetVertexNums();
        auto task = [&edge_func, graph, begin_index, end_index]() {
          for (VertexIndex i = begin_index; i < end_index; i++) {
            auto degree = graph->GetOutDegreeByIndex(i);
            if (degree != 0) {
              VertexID* outEdges = graph->GetOutEdgesByIndex(i);
              for (VertexIndex j = 0; j < degree; j++) {
                //            LOGF_INFO("edge_func: {}, {}",
                //            graph_->GetVertexIDByIndex(i),
                //                      graph_->GetOneOutEdge(i, j));
                edge_func(graph->GetVertexIDByIndex(i), outEdges[j]);
              }
            }
          }
        };
        tasks.push_back(task);
        begin_index = end_index;
        count++;
      }
    }
    //    LOGF_INFO("task_size: {}, num tasks: {}. left edges: {}", task_size,
    //    count,
    //              graph_->GetOutEdgeNums());
    runner_->SubmitSync(tasks);
    group_graphs->SyncVertexData(use_readdata_only_);
    LOG_DEBUG("ParallelEdgeDo is done");
  }

  // Parallel execute edge_delete_func in task_size chunks.
  void ParallelEdgeMutateDo(
      const std::function<void(GraphID, VertexID, VertexID, EdgeIndex)>&
          edge_del_func) {
    LOG_DEBUG("ParallelEdgeDelDo is begin");
    auto group_graphs = (MutableGroupCSRGraphUInt32*)(graph_);
    uint32_t task_size = GetTaskSize(group_graphs->GetVertexNums());
    common::TaskPackage tasks;
    for (int i = 0; i < group_graphs->GetNumSubgraphs(); i++) {
      auto graph = group_graphs->GetSubgraph(i);
      VertexIndex begin_index = 0, end_index = 0;
      for (; begin_index < graph->GetVertexNums();) {
        end_index += task_size;
        if (end_index > graph->GetVertexNums())
          end_index = graph->GetVertexNums();
        auto task = [&edge_del_func, graph, begin_index, end_index,
                     graphIndex = i]() {
          for (VertexIndex i = begin_index; i < end_index; i++) {
            auto degree = graph->GetOutDegreeByIndex(i);
            if (degree != 0) {
              EdgeIndex outOffset_base = graph->GetOutOffsetByIndex(i);
              VertexID* outEdges = graph->GetOutEdgesByIndex(i);
              for (VertexIndex j = 0; j < degree; j++) {
                //            LOGF_INFO("edge_del_func: {}, {}, {}",
                //                      graph_->GetVertexIDByIndex(i),
                //                      graph_->GetOneOutEdge(i, j),
                //                      graph_->GetOutOffsetByIndex(i) + j);
                edge_del_func(graphIndex, graph->GetVertexIDByIndex(i),
                              outEdges[j], outOffset_base + j);
              }
            }
          }
        };
        tasks.push_back(task);
        begin_index = end_index;
      }
    }
    runner_->SubmitSync(tasks);
    group_graphs->MutateGraphEdge(runner_);
    LOG_DEBUG("ParallelEdgedelDo is done");
  }

  uint32_t GetTaskSize(VertexID max_vid) const {
    auto task_num = parallelism_ * task_package_factor_;
    uint32_t task_size = ceil((double)max_vid / task_num);
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

  // configs
  const uint32_t parallelism_;
  const uint32_t task_package_factor_;
  const common::ApplicationType app_type_;
  bool use_readdata_only_ = true;
  // TODO: add UpdateStore as a member here.
};

}  // namespace sics::graph::core::apis

#endif  // GRAPH_SYSTEMS_CORE_APIS_PLANAR_APP_GROUP_BASE_H_
