#ifndef GRAPH_SYSTEMS_CORE_APIS_PLANAR_APP_OP_H_
#define GRAPH_SYSTEMS_CORE_APIS_PLANAR_APP_OP_H_

#include <functional>
#include <type_traits>
#include <vector>

#include "apis/pie.h"
#include "common/bitmap.h"
#include "common/blocking_queue.h"
#include "common/config.h"
#include "common/multithreading/task_runner.h"
#include "components/discharger.h"
#include "components/executor_op.h"
#include "components/loader_op.h"
#include "data_structures/serializable.h"
#include "io/mutable_csr_reader.h"
#include "io/mutable_csr_writer.h"
#include "scheduler/graph_manager.h"
#include "update_stores/bsp_update_store.h"
#include "util/atomic.h"
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
class PlanarAppOpBase : public PIE {
  using VertexData = typename GraphType::VertexData;
  using EdgeData = typename GraphType::EdgeData;

  using GraphID = common::GraphID;
  using BlockID = common::BlockID;
  using VertexID = common::VertexID;
  using VertexIndex = common::VertexIndex;
  using EdgeIndex = common::EdgeIndex;
  using EdgeIndexS = common::EdgeIndexS;
  using VertexDegree = common::VertexDegree;

 public:
  PlanarAppOpBase() = default;
  PlanarAppOpBase(const std::string& root_path)
      : metadata_(root_path),
        graph_(nullptr),
        parallelism_(common::Configurations::Get()->parallelism),
        task_package_factor_(
            common::Configurations::Get()->task_package_factor),
        app_type_(common::Configurations::Get()->application) {
    loader_.Init(root_path, &hub);
    //    discharger_.Init(root_path, &hub);
    executer_.Init(&hub);

    use_readdata_only_ = app_type_ == common::Coloring;
    LOGF_INFO("vertex data sync: {}", !use_readdata_only_);
    LOGF_INFO("use read data only: {}", use_readdata_only_);
  }
  // TODO: add UpdateStore as a parameter, so that PEval, IncEval and Assemble
  //  can access global messages in it.
  PlanarAppOpBase(
      common::TaskRunner* runner,
      update_stores::BspUpdateStore<VertexData, EdgeData>* update_store,
      data_structures::graph::MutableBlockCSRGraph<float, unsigned char>* graph)
      : graph_(static_cast<GraphType*>(graph)),
        parallelism_(common::Configurations::Get()->parallelism),
        task_package_factor_(
            common::Configurations::Get()->task_package_factor),
        app_type_(common::Configurations::Get()->application) {
    use_readdata_only_ = app_type_ == common::Coloring;
    LOGF_INFO("vertex data sync: {}", !use_readdata_only_);
    LOGF_INFO("use read data only: {}", use_readdata_only_);
  }

  ~PlanarAppOpBase() override = default;

  virtual void AppInit(
      common::TaskRunner* runner,
      update_stores::BspUpdateStore<VertexData, EdgeData>* update_store) {
    //    active_.Init(update_store->GetMessageCount());
    //    active_next_.Init(update_store->GetMessageCount());
  }

  virtual void SetRound(int round) { round_ = round; }

  virtual void SetRuntimeGraph(GraphType* graph) { graph_ = graph; }

 protected:
  // Parallel execute vertex_func in task_size chunks.
  void ParallelVertexDo(const std::function<void(VertexID)>& vertex_func) {
    LOG_DEBUG("ParallelVertexDo is begin");
    for (common::GraphID gid = 0; gid < metadata_.num_blocks; gid++) {
      uint32_t task_size = GetTaskSize(metadata_.num_vertices);
      common::TaskPackage tasks;
      tasks.reserve(parallelism_ * task_package_factor_);
      VertexID begin_id = 0, end_id = 0;
      for (; begin_id < metadata_.num_vertices;) {
        end_id += task_size;
        if (end_id > metadata_.num_vertices) end_id = metadata_.num_vertices;
        auto task = [&vertex_func, begin_id, end_id]() {
          for (VertexID id = begin_id; id < end_id; id++) {
            vertex_func(id);
          }
        };
        tasks.push_back(task);
        begin_id = end_id;
      }
      //    LOGF_INFO("ParallelVertexDo task_size: {}, num tasks: {}",
      //    task_size,
      //              tasks.size());
      executer_.GetTaskRunner()->SubmitSync(tasks);
      LOG_INFO("task finished");
    }
    SyncVertexState(use_readdata_only_);
    LOG_INFO("ParallelVertexDo is done");
  }

  // Users will use edges of block, so we need to load edge block first.
  void ParallelVertexDoWithEdges(
      const std::function<void(VertexID)>& vertex_func) {
    LOG_DEBUG("ParallelVertexDoWithEdges begins!");
    for (uint32_t gid = 0; gid < metadata_.num_blocks; gid++) {
      active_edge_blocks_.at(gid).Fill();  // Now active all blocks.
      CheckActiveEdgeBLocksInfo(gid);
      // First, edge block current in memory.
      common::TaskPackage tasks;
      for (unsigned int sub_block_id : blocks_in_memory_) {
        auto sub_block = metadata_.blocks.at(gid).sub_blocks.at(sub_block_id);
        tasks.push_back([&vertex_func, sub_block]() {
          auto begin_id = sub_block.begin_id;
          auto end_id = sub_block.end_id;
          for (VertexID id = begin_id; id < end_id; id++) {
            vertex_func(id);
          }
        });
      }
      executer_.GetTaskRunner()->SubmitSync(tasks);
      // Secondly, Wait for io load left edg block.
      // TODO: Block when no edge block is ready!
      bool flag = false;
      while (true) {
        tasks.clear();
        auto blocks = loader_.GetReadBlocks();

        auto resp = hub.get_response_queue()->PopOrWait();
        scheduler::ReadMessage read_resp;
        resp.Get(&read_resp);
        flag = read_resp.terminated;
        auto read_edge_block_id = loader_.GetReadBlocks();
        for (int j = 0; j < read_resp.read_block_size_; j++) {
          auto sub_block_id = read_edge_block_id[j];
          auto sub_block = metadata_.blocks.at(gid).sub_blocks.at(sub_block_id);
          tasks.push_back([sub_block, vertex_func]() {
            auto begin_id = sub_block.begin_id;
            auto end_id = sub_block.end_id;
            for (VertexID id = begin_id; id < end_id; id++) {
              vertex_func(id);
            }
          });
        }
        executer_.GetTaskRunner()->SubmitSync(tasks);
        // Decide if release edge block for load other block.

        // Check all activated edge blocks are executed.
        if (flag) {
          break;
        }
      }
    }
    LOG_DEBUG("ParallelVertexDoWithEdges is done!");
  }

  void ParallelEdgeDo2(
      const std::function<void(VertexID, VertexID)>& edge_func) {}

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
    executer_.GetTaskRunner()->SubmitSync(tasks);
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
    executer_.GetTaskRunner()->SubmitSync(tasks);
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
    executer_.GetTaskRunner()->SubmitSync(tasks);
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
    executer_.GetTaskRunner()->SubmitSync(tasks);
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
    executer_.GetTaskRunner()->SubmitSync(tasks);
    graph_->MutateGraphEdge(executer_.GetTaskRunner());
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

  std::vector<VertexID> GetReadEdgeBlock(size_t size) {
    std::vector<VertexID> res;
    for (size_t i = 0; i < size; i++) {
      res.push_back(active_queue_->PopOrWait());
    }
    return res;
  }

  // Check if active edge blocks are in memory and add to in_memory list.
  // If not add to read list.
  void CheckActiveEdgeBLocksInfo(common::GraphID gid) {
    auto& active = active_edge_blocks_.at(gid);
    for (int i = 0; i < active.size(); i++) {
      if (active.GetBit(i)) {
        if (is_in_memory_.at(gid).at(i)) {
          blocks_in_memory_.push_back(i);
        } else {
          blocks_to_read_.push_back(i);
        }
      }
    }
  }

  // Read and Write functions.

  VertexData Read(VertexID id) { return read_data_[id]; }

  void Write(VertexID id, VertexData vdata) { write_data_[id] = vdata; }

  void WriteMin(VertexID id, VertexData vdata) {
    core::util::atomic::WriteMin(&write_data_[id], vdata);
  }

  void WriteMax(VertexID id, VertexData vdata) {
    core::util::atomic::WriteMax(&write_data_[id], vdata);
  }

  void WriteAdd(VertexID id, VertexData vdata) {
    core::util::atomic::WriteAdd(&write_data_[id], vdata);
  }

  // Graph info functions.

  size_t GetVertexNum() { return metadata_.num_vertices; }

  size_t GetEdgeNum() { return metadata_.num_edges; }

  VertexDegree GetOutDegree(VertexID id) {
    auto block_id = GetBlockID(id);
    return graphs_.at(block_id).GetOutDegree(id);
  }

  VertexID* GetOutEdges(VertexID id) {
    auto block_id = GetBlockID(id);
    return graphs_.at(block_id).GetOutEdges(id);
  }

 private:
  BlockID GetBlockID(VertexID id) {
    for (int i = 0; i < metadata_.num_blocks; i++) {
      if (id < metadata_.blocks.at(i).end_id) {
        return i;
      }
    }
    LOG_FATAL("VertexID: ", id, " is out of range!");
  }

  // Sync functions.
  void SyncVertexState(bool read_only) {
    if (!read_only) {
      memcpy(read_data_, write_data_,
             sizeof(VertexData) * metadata_.num_vertices);
    }
  }

 protected:
  GraphType* graph_;
  common::BlockingQueue<VertexID>* active_queue_;

  common::Bitmap in_;
  common::Bitmap out_;

  int round_ = 0;

  common::Bitmap active_;
  common::Bitmap active_next_;

  // graph_manager ------
  data_structures::TwoDMetadata metadata_;

  // Vertex states.
  VertexData* read_data_ = nullptr;
  VertexData* write_data_ = nullptr;

  common::GraphID active_gid_ = 0;
  std::vector<common::Bitmap> active_edge_blocks_;
  // Keep the edge block in memory.
  // Or write back to disk for memory space release.
  std::vector<std::vector<bool>> is_in_memory_;
  std::vector<std::vector<bool>> is_finished_;
  std::vector<BlockID> blocks_in_memory_;
  std::vector<BlockID> blocks_to_read_;

  // Graph index and edges structure. Init at beginning.
  std::vector<GraphType> graphs_;
  // Buffer size management.
  size_t buffer_size_;
  // --------

  scheduler::MessageHub hub;

  components::LoaderOp loader_;
  //  components::Discharger<io::MutableCSRWriter> discharger_;
  components::ExecutorOp executer_;

  // configs
  const uint32_t parallelism_;
  const uint32_t task_package_factor_;
  const common::ApplicationType app_type_;
  bool use_readdata_only_ = true;
  // TODO: add UpdateStore as a member here.
};

}  // namespace sics::graph::core::apis

#endif  // GRAPH_SYSTEMS_CORE_APIS_PLANAR_APP_OP_H_
