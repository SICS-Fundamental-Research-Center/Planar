#ifndef GRAPH_SYSTEMS_PLANAR_APP_BASE_OP_H
#define GRAPH_SYSTEMS_PLANAR_APP_BASE_OP_H

#include <condition_variable>
#include <functional>
#include <mutex>
#include <type_traits>
#include <vector>

#include "apis/pie.h"
#include "common/blocking_queue.h"
#include "common/config.h"
#include "common/multithreading/task_runner.h"
#include "data_structures/graph/mutable_block_csr_graph.h"
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
template <typename VertexData>
class PlanarAppBaseOp : public PIE {
  using GraphID = common::GraphID;
  using BlockID = common::BlockID;
  using VertexID = common::VertexID;
  using VertexIndex = common::VertexIndex;
  using EdgeIndex = common::EdgeIndex;
  using EdgeIndexS = common::EdgeIndexS;

 public:
  PlanarAppBaseOp()
      : runner_(nullptr),
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
  PlanarAppBaseOp(common::TaskRunner* runner,
                  data_structures::Serializable* graph)
      : runner_(runner),
        parallelism_(common::Configurations::Get()->parallelism),
        task_package_factor_(
            common::Configurations::Get()->task_package_factor),
        app_type_(common::Configurations::Get()->application) {
    use_readdata_only_ = app_type_ == common::Coloring;
    LOGF_INFO("vertex data sync: {}", !use_readdata_only_);
    LOGF_INFO("use read data only: {}", use_readdata_only_);
  }

  ~PlanarAppBaseOp() {
    delete[] read_;
    read_ = nullptr;
    delete[] write_;
    write_ = nullptr;
  }

  virtual void AppInit(
      common::TaskRunner* runner, common::BlockingQueue<common::BlockID>* queue,
      std::vector<data_structures::graph::MutableBlockCSRGraph>* graphs) {
    runner_ = runner;
    queue_ = queue;
    graphs_ = graphs;
    //    active_.Init(update_store->GetMessageCount());
    //    active_next_.Init(update_store->GetMessageCount());
  }

  virtual void SetRound(int round) { round_ = round; }

 protected:
  // Parallel execute vertex_func in task_size chunks.
  void ParallelVertexDo(GraphID gid,
                        const std::function<void(VertexID)>& vertex_func) {
    LOG_DEBUG("ParallelVertexDo is begin");
    auto block_meta = meta_->blocks.at(gid);
    uint32_t task_size = GetTaskSize(block_meta.num_vertices);
    common::TaskPackage tasks;
    tasks.reserve(parallelism_ * task_package_factor_);
    VertexID begin_id = 0, end_id = 0;
    for (; begin_id < block_meta.num_vertices;) {
      end_id += task_size;
      if (end_id > block_meta.num_vertices) end_id = block_meta.num_vertices;
      auto task = [&vertex_func, this, begin_id, end_id]() {
        for (VertexID id = begin_id; id < end_id; id++) {
          vertex_func(id);
        }
      };
      tasks.push_back(task);
      begin_id = end_id;
    }
    //    LOGF_INFO("ParallelVertexDo task_size: {}, num tasks: {}", task_size,
    //              tasks.size());
    runner_->SubmitSync(tasks);
    // TODO: sync of update_store and graph_ vertex data
    LOG_INFO("task finished");
    Sync(use_readdata_only_);
    LOG_INFO("ParallelVertexDo is done");
  }

  void ParallelVertexDoWithEdges(
      GraphID gid, const std::function<void(VertexID)>& vertex_func) {
    LOG_DEBUG("ParallelVertexDo is begin");
    auto block_meta = meta_->blocks.at(gid);
    int size_num = block_meta.num_sub_blocks;
    while (true) {
      auto bid = queue_->PopOrWait();
      if (bid == MAX_VERTEX_ID) break;
      auto sub_block_meta = block_meta.sub_blocks.at(bid);
      auto task = [&vertex_func, &size_num, this, sub_block_meta]() {
        auto begin_id = sub_block_meta.begin_id;
        auto end_id = sub_block_meta.end_id;
        for (VertexID id = begin_id; id < end_id; id++) {
          vertex_func(id);
        }
        std::lock_guard<std::mutex> lock(mtx_);
        size_num -= 1;
        cv_.notify_all();
      };
      runner_->SubmitAsync(task);
    }
    // TODO: sync of update_store and graph_ vertex data
    std::unique_lock<std::mutex> lock(mtx_);
    if (size_num != 0) {
      cv_.wait(lock, [&size_num]() { return size_num == 0; });
    }
    LOG_INFO("task finished");
    Sync(use_readdata_only_);
    LOG_INFO("ParallelVertexDo is done");
  }

  //  void ParallelVertexDoByIndex(
  //      const std::function<void(VertexID)>& vertex_func) {
  //    LOG_DEBUG("ParallelVertexDoByIndex is begin");
  //    uint32_t task_size = GetTaskSize(graph_->GetVertexNums());
  //    common::TaskPackage tasks;
  //    tasks.reserve(parallelism_ * task_package_factor_);
  //    VertexIndex begin_index = 0, end_index = 0;
  //    for (; begin_index < graph_->GetVertexNums();) {
  //      end_index += task_size;
  //      if (end_index > graph_->GetVertexNums())
  //        end_index = graph_->GetVertexNums();
  //      auto task = [&vertex_func, begin_index, end_index]() {
  //        for (VertexIndex idx = begin_index; idx < end_index; idx++) {
  //          vertex_func(idx);
  //        }
  //      };
  //      tasks.push_back(task);
  //      begin_index = end_index;
  //    }
  //    //    LOGF_INFO("ParallelVertexDo task_size: {}, num tasks: {}",
  //    task_size,
  //    //              tasks.size());
  //    runner_->SubmitSync(tasks);
  //    // TODO: sync of update_store and graph_ vertex data
  //    graph_->SyncVertexData(use_readdata_only_);
  //    LOG_DEBUG("ParallelVertexDoByIndex is done");
  //  }

  //  void ParallelVertexDoWithActive(
  //      const std::function<void(VertexID)>& vertex_func) {
  //    LOG_DEBUG("ParallelVertexDoWithActive is begin");
  //    uint32_t task_size = 64 * 100;
  //
  //    common::TaskPackage tasks;
  //    tasks.reserve((graph_->GetVertexNums() / 64 / 100) + 1);
  //    VertexIndex begin_index = 0, end_index = 0;
  //    for (; begin_index < graph_->GetVertexNums();) {
  //      end_index += task_size;
  //      if (end_index > graph_->GetVertexNums())
  //        end_index = graph_->GetVertexNums();
  //      auto task = [&vertex_func, this, begin_index, end_index]() {
  //        for (VertexIndex idx = begin_index; idx < end_index;) {
  //          if (active_.GetBit64(idx)) {
  //            for (int j = 0; j < 64 && (idx + j) < end_index; j++) {
  //              if (active_.GetBit(idx + j))
  //                vertex_func(graph_->GetVertexIDByIndex(idx + j));
  //            }
  //          }
  //          idx += 64;
  //        }
  //        //        for (VertexIndex idx = begin_index; idx < end_index;
  //        idx++) {
  //        //          auto id = graph_->GetVertexIDByIndex(idx);
  //        //          if (active_.GetBit(idx)) {
  //        //            vertex_func(id);
  //        //          }
  //        //        }
  //      };
  //      tasks.push_back(task);
  //      begin_index = end_index;
  //    }
  //    //    LOGF_INFO("ParallelVertexDo task_size: {}, num tasks: {}",
  //    task_size,
  //    //              tasks.size());
  //    runner_->SubmitSync(tasks);
  //    // TODO: sync of update_store and graph_ vertex data
  //    graph_->SyncVertexData(use_readdata_only_);
  //    LOG_DEBUG("ParallelVertexDoWithActive is done");
  //  }

  //  // Parallel execute vertex_func in task_size chunks.
  //  void ParallelVertexDoStep(const std::function<void(VertexID)>&
  //  vertex_func) {
  //    LOG_DEBUG("ParallelVertexDoStep begins");
  //    // auto task_size = GetTaskSize(graph_->GetVertexNums());
  //    common::TaskPackage tasks;
  //    tasks.reserve(parallelism_ * task_package_factor_);
  //    VertexIndex end = graph_->GetVertexNums();
  //    for (uint32_t i = 0; i < parallelism_; i++) {
  //      auto index = end - 1 - i;
  //      auto task = [&vertex_func, this, index]() {
  //        for (int idx = index; idx >= 0;) {
  //          vertex_func(graph_->GetVertexIDByIndex(idx));
  //          idx -= parallelism_;
  //        }
  //      };
  //      tasks.push_back(task);
  //    }
  //    //    LOGF_INFO("ParallelVertexDoStep task_size: {}, num tasks: {}",
  //    //    task_size,
  //    //              tasks.size());
  //    runner_->SubmitSync(tasks);
  //    // TODO: sync of update_store and graph_ vertex data
  //    graph_->SyncVertexData(use_readdata_only_);
  //    LOG_DEBUG("ParallelVertexDoStep done");
  //  }

  // Parallel execute edge_func in task_size chunks.
  void ParallelEdgeDo(
      GraphID gid, const std::function<void(VertexID, VertexID)>& edge_func) {
    LOG_DEBUG("ParallelEdgeDo begins");
    auto block_meta = meta_->blocks.at(gid);
    int size_num = block_meta.num_sub_blocks;

    while (true) {
      auto bid = queue_->PopOrWait();
      if (bid == MAX_VERTEX_ID) break;
      auto sub_block_meta = block_meta.sub_blocks.at(bid);
      auto task = [&edge_func, &size_num, this, sub_block_meta, gid]() {
        auto id = sub_block_meta.begin_id;
        auto& block = graphs_->at(gid);
        auto num_edges = sub_block_meta.num_edges;
        auto edges = graphs_->at(gid).GetAllEdges(sub_block_meta.id);
        uint32_t degree = block.GetOutDegree(id);
        while (degree == 0) {
          id++;
          degree = block.GetOutDegree(id);
        }
        for (EdgeIndexS i = 0; i < num_edges; i++) {
          edge_func(id, edges[i]);
          degree--;
          while (degree == 0) {
            id++;
            degree = block.GetOutDegree(id);
          }
        }
        if (id != sub_block_meta.end_id) {
          LOG_FATAL("Error executing edge parallel do!");
        }
        std::lock_guard<std::mutex> lock(mtx_);
        size_num -= 1;
        cv_.notify_all();
      };
      runner_->SubmitAsync(task);
    }
    Sync(use_readdata_only_);
    LOG_DEBUG("ParallelEdgeDo is done");
  }

  //    // Parallel execute edge_delete_func in task_size chunks.
  //    void ParallelEdgeMutateDo(
  //        const std::function<void(VertexID, VertexID, EdgeIndex)>&
  //        edge_del_func) {
  //      LOG_DEBUG("ParallelEdgeDelDo is begin");
  //      uint32_t task_size = GetTaskSize(graph_->GetVertexNums());
  //      common::TaskPackage tasks;
  //      VertexIndex begin_index = 0, end_index = 0;
  //      for (; begin_index < graph_->GetVertexNums();) {
  //        end_index += task_size;
  //        if (end_index > graph_->GetVertexNums())
  //          end_index = graph_->GetVertexNums();
  //        auto task = [&edge_del_func, this, begin_index, end_index]() {
  //          for (VertexIndex i = begin_index; i < end_index; i++) {
  //            auto degree = graph_->GetOutDegreeByIndex(i);
  //            if (degree != 0) {
  //              EdgeIndex outOffset_base = graph_->GetOutOffsetByIndex(i);
  //              VertexID* outEdges = graph_->GetOutEdgesByIndex(i);
  //              for (VertexIndex j = 0; j < degree; j++) {
  //                //            LOGF_INFO("edge_del_func: {}, {}, {}",
  //                //                      graph_->GetVertexIDByIndex(i),
  //                //                      graph_->GetOneOutEdge(i, j),
  //                //                      graph_->GetOutOffsetByIndex(i) + j);
  //                edge_del_func(graph_->GetVertexIDByIndex(i), outEdges[j],
  //                              outOffset_base + j);
  //              }
  //            }
  //          }
  //        };
  //        tasks.push_back(task);
  //        begin_index = end_index;
  //      }
  //      runner_->SubmitSync(tasks);
  //      graph_->MutateGraphEdge(runner_);
  //      LOG_DEBUG("ParallelEdgedelDo is done");
  //    }

  size_t GetTaskSize(VertexID max_vid) const {
    auto task_num = parallelism_ * task_package_factor_;
    size_t task_size = ceil((double)max_vid / task_num);
    return task_size < 2 ? 2 : task_size;
  }

  void SyncActive() {
    std::swap(active_, active_next_);
    active_next_.Clear();
  }

  void Sync(bool read_only = false) {
    if (!read_only) {
      memcpy(read_, write_, sizeof(VertexData) * meta_->num_vertices);
    }
  }

  // Read and Write function.

  VertexData Read(VertexID id) { return read_[id]; }

  void Write(VertexID id, VertexData vdata) { write_[id] = vdata; }

  void WriteMin(VertexID id, VertexData vdata) {
    core::util::atomic::WriteMin(&write_[id], vdata);
  }

  void WriteMax(VertexID id, VertexData vdata) {
    core::util::atomic::WriteMax(&write_[id], vdata);
  }

  void WriteAdd(VertexID id, VertexData vdata) {
    core::util::atomic::WriteAdd(&write_[id], vdata);
  }

 protected:
  data_structures::TwoDMetadata* meta_;

  common::TaskRunner* runner_;

  data_structures::graph::MutableBlockCSRGraph* graph_;

  int round_ = 0;

  common::Bitmap active_;
  common::Bitmap active_next_;

  // block ids in one sub_graph, -1 means the subgraph is all loaded.
  common::BlockingQueue<common::BlockID>* queue_;

  VertexData* read_;
  VertexData* write_;

  GraphID current_gid_;
  std::vector<data_structures::graph::MutableBlockCSRGraph>* graphs_;

  std::mutex mtx_;
  std::condition_variable cv_;

  // configs
  const uint32_t parallelism_;
  const uint32_t task_package_factor_;
  const common::ApplicationType app_type_;
  bool use_readdata_only_ = true;
  // TODO: add UpdateStore as a member here.
};

}  // namespace sics::graph::core::apis

#endif  // GRAPH_SYSTEMS_PLANAR_APP_BASE_OP_H
