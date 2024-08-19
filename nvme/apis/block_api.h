#ifndef GRAPH_SYSTEMS_NVME_APIS_BLOCK_API_H_
#define GRAPH_SYSTEMS_NVME_APIS_BLOCK_API_H_

#include <condition_variable>
#include <functional>
#include <mutex>
#include <type_traits>
#include <vector>

#include "core/common/multithreading/task_runner.h"
#include "core/common/types.h"
#include "core/data_structures/graph_metadata.h"
#include "core/data_structures/serializable.h"
#include "core/util/logging.h"
#include "nvme/apis/block_api_base.h"
#include "nvme/common/config.h"
#include "nvme/components/discharge.h"
#include "nvme/components/executor.h"
#include "nvme/components/loader.h"
#include "nvme/data_structures/graph/block_csr_graph.h"
#include "nvme/data_structures/graph/pram_block.h"
#include "nvme/data_structures/memory_buffer.h"
#include "nvme/data_structures/neighbor_hop.h"
#include "nvme/io/io_uring_reader.h"
#include "nvme/scheduler/message_hub.h"

namespace sics::graph::nvme::apis {

using core::data_structures::Serializable;

class BlockModel : public BlockModelBase {
  using EdgeData = core::common::DefaultEdgeDataType;

  using BlockID = core::common::BlockID;
  using GraphID = core::common::GraphID;
  using VertexID = core::common::VertexID;
  using VertexIndex = core::common::VertexIndex;
  using EdgeIndex = core::common::EdgeIndex;
  using VertexDegree = core::common::VertexDegree;

  using FuncVertex = core::common::FuncVertex;
  using FuncEdge = core::common::FuncEdge;
  using FuncEdgeAndMutate = core::common::FuncEdgeAndMutate;
  using FuncEdgeMutate = core::common::FuncEdgeMutate;

  using ExecuteMessage = sics::graph::nvme::scheduler::ExecuteMessage;
  using ExecuteType = sics::graph::nvme::scheduler::ExecuteType;
  using MapType = sics::graph::nvme::scheduler::MapType;

  using Block = sics::graph::nvme::data_structures::graph::BlockCSRGraph;

 public:
  BlockModel() = default;
  BlockModel(const std::string& root_path)
      : root_path_(root_path),
        parallelism_(core::common::Configurations::Get()->parallelism),
        task_runner_(parallelism_) {
    task_package_factor_ =
        core::common::Configurations::Get()->task_package_factor;
    graph_.Init(root_path, &meta_);
    buffer_.Init(&meta_, &graph_);
    loader_.Init(root_path, &message_hub_, &buffer_);
    //    discharge_ =
    //    std::make_unique<components::Discharger<io::PramBlockWriter>>(
    //        root_path, scheduler_.GetMessageHub());

    //    scheduler_.Init(loader_->GetReader());
  }

  ~BlockModel() override {
    Stop();
    whole_end_time_ = std::chrono::system_clock::now();
    if (core::common::Configurations::Get()->in_memory) {
      LOGF_INFO(" =========== In memory time: {} s ===========",
                std::chrono::duration<double>(common::end_time_in_memory -
                                              common::start_time_in_memory)
                    .count());
    }
    LOGF_INFO(
        " =========== Compute Runtime: {} s ===========",
        std::chrono::duration<double>(compute_end_time_ - begin_time_).count());
    LOGF_INFO(
        " =========== Whole Runtime: {} s ===========",
        std::chrono::duration<double>(whole_end_time_ - begin_time_).count());
  }

  // ===============================================================
  // Map functions should use scheduler to iterate over all blocks
  // ===============================================================

  // init all data of blocks
  // TODO:
  //  move init in update_store to here
  void Init() {
    // for different blocks, init different data
  }

  void EvenAllocateBlocksInMemory(std::vector<BlockID>& blocks) {
    auto num = blocks.size();
    uint32_t index = 0;
    for (int i = 0; i < num; i++) {
      queues_.at(index++).Push(blocks.at(i));
      index = index % parallelism_;
    }
  }

  void InitAllBlocksInMemory() {
    auto num = meta_.num_subBlocks;
    auto index = 0;
    for (BlockID i = 0; i < num; i++) {
      queues_.at(index++).Push(i);
      index = index % parallelism_;
    }
  }

  void StopRunningThreads() {
    for (uint32_t tid; tid < parallelism_; tid++) {
      queues_.at(tid).Push(INVALID_BID);
    }
  }

  void CheckStop() {
    std::lock_guard<std::mutex> lck(mtx_);
    num_active_subBlock_--;
    if (num_active_subBlock_ == 0) {
      for (uint32_t tid; tid < parallelism_; tid++) {
        queues_.at(tid).Push(INVALID_BID);
      }
    }
  }

  void MapVertexInit(FuncVertex& func_vertex) {
    auto task_num = meta_.num_subBlocks;
    core::common::TaskPackage tasks;
    for (BlockID bid = 0; bid < task_num; bid++) {
      auto task = [&func_vertex, this, bid]() {
        auto sub_meta = meta_.subBlocks.at(bid);
        for (VertexID id = sub_meta.begin_id; id < sub_meta.end_id; id++) {
          func_vertex(id);
        }
      };
      tasks.push_back(task);
    }
    task_runner_.SubmitSync(tasks);
  }

  void MapVertex(FuncVertex& func_vertex) {
    InitAllBlocksInMemory();
    core::common::TaskPackage tasks;
    for (auto i = 0; i < parallelism_; i++) {
      auto task = [&func_vertex, i, this]() {
        auto& queue = this->queues_.at(i);
        while (true) {
          auto bid = queue.PopOrWait();
          if (bid == INVALID_BID) break;  // Quit when receive invalid bid.
          auto sub_meta = meta_.subBlocks.at(bid);
          for (VertexID j = sub_meta.begin_id; j < sub_meta.begin_id; j++) {
            func_vertex(j);
          }
        }
      };
      tasks.push_back(task);
    }
    task_runner_.SubmitSync(tasks);
  }

  void MapVertexWithEdges(FuncVertex& func_vertex) {
    // Decide load and execute in memory.
    auto blocks_in_memory = buffer_.GetBlocksInMemory();
    auto blocks_to_read = buffer_.GetBlocksToRead();
    EvenAllocateBlocksInMemory(blocks_in_memory);
    buffer_.LoadBlocksNotInMemory();
    num_active_subBlock_ = meta_.num_subBlocks;
    core::common::TaskPackage tasks;
    for (auto i = 0; i < parallelism_; i++) {
      auto task = [&func_vertex, i, this]() {
        auto& queue = this->queues_.at(i);
        while (true) {
          auto bid = queue.PopOrWait();
          if (bid == INVALID_BID) break;  // Quit when receive invalid bid.
          auto sub_meta = meta_.subBlocks.at(bid);
          for (VertexID j = sub_meta.begin_id; j < sub_meta.begin_id; j++) {
            func_vertex(j);
          }
          CheckStop();
        }
      };
      tasks.push_back(task);
    }
    task_runner_.SubmitSync(tasks);
    LOG_INFO("MapVertex finished");
  }

  void MapVertexWithPrecomputing(FuncVertex& func_vertex) {
    ParallelVertexDo(func_vertex);
    LOG_INFO("MapVertexWithPrecomputing finishes");
  }

  void MapEdge(std::function<void(VertexID, VertexID)>& func_edge) {
    // all blocks should be executor the edge function
    LOG_INFO("MapEdge finishes");
  }

  void MapAndMutateEdge(
      std::function<void(VertexID, VertexID, EdgeIndex)>& func_edge_del) {
    // all blocks should be executor the edge function
    LOG_INFO("MapEdgeAndMutate finishes");
  }

  void MapAndMutateEdgeBool(
      std::function<bool(VertexID, VertexID)>& func_edge_del) {
    LOG_INFO("MapEdgeAndMutate finishes");
  }

  // ===============================================================
  // Map functions should use scheduler to iterate over all blocks
  // ===============================================================

  void Run() {
    LOG_INFO("Start running");
    begin_time_ = std::chrono::system_clock::now();
    loader_.Start();

    LOG_INFO(" ================ Start Algorithm executing! ================= ");
    Compute();
    common::end_time_in_memory = std::chrono::system_clock::now();
    compute_end_time_ = std::chrono::system_clock::now();
    LOG_INFO("Running finished!");
  }

  void Stop() { loader_.StopAndJoin(); }

  void LockAndWaitResult() {
    //    std::unique_lock<std::mutex> lock(pram_mtx_);
    //    pram_cv_.wait(lock, [] { return true; });
  }

  // Used for block api.

  void DeleteEdge(VertexID src, VertexID dst, EdgeIndex idx) {}

  VertexDegree GetOutDegree(VertexID id) { return 1; }

  const VertexID* GetEdges(VertexID id) { return nullptr; }

  VertexID GetNumVertices() { return 1; }

  // Precomputing info apis

  VertexID GetOneHopMinId(VertexID id) {
    return neighbor_hop_info_.GetOneHopMinId(id);
  }
  VertexID GetOneHopMaxId(VertexID id) {
    return neighbor_hop_info_.GetOneHopMaxId(id);
  }
  VertexID GetTwoHopMinId(VertexID id) {
    return neighbor_hop_info_.GetTwoHopMinId(id);
  }
  VertexID GetTwoHopMaxId(VertexID id) {
    return neighbor_hop_info_.GetTwoHopMaxId(id);
  }

  void ParallelVertexDo(FuncVertex& vertex_func) {
    auto num_vertices = meta_.num_vertices;
    auto task_num = parallelism_ * task_package_factor_;
    uint32_t task_size = (num_vertices + task_num - 1) / task_num;
    core::common::TaskPackage tasks;
    //    tasks.reserve(ceil((double)block->GetVertexNums() / task_size));
    VertexID begin_id = 0, end_id = 0;
    for (; begin_id < num_vertices;) {
      end_id += task_size;
      if (end_id > num_vertices) {
        end_id = num_vertices;
      }
      auto task = [&vertex_func, begin_id, end_id]() {
        for (VertexID id = begin_id; id < end_id; id++) {
          vertex_func(id);
        }
      };
      tasks.push_back(task);
      begin_id = end_id;
    }
    LOGF_INFO("Precomputing task num: {}", tasks.size());
    task_runner_.SubmitSync(tasks);
  }

  // ===========================================
  // parallel functions for vertexes and edges
  // ===========================================

  //  void ParallelVertexDo(const FuncVertex& vertex_func) {
  //    //    LOG_DEBUG("ParallelVertexDo begins!");
  //    auto block = static_cast<Block*>(graph);
  //    uint32_t task_size = GetTaskSize(block->GetVertexNums());
  //    //    uint32_t task_size = task_size_;
  //    core::common::TaskPackage tasks;
  //    //    tasks.reserve(ceil((double)block->GetVertexNums() / task_size));
  //    VertexIndex begin_index = 0, end_index = 0;
  //    for (; begin_index < block->GetVertexNums();) {
  //      end_index += task_size;
  //      if (end_index > block->GetVertexNums()) {
  //        end_index = block->GetVertexNums();
  //      }
  //      auto task = [&vertex_func, block, begin_index, end_index]() {
  //        for (VertexIndex idx = begin_index; idx < end_index; idx++) {
  //          vertex_func(block->GetVertexID(idx));
  //        }
  //      };
  //      tasks.push_back(task);
  //      begin_index = end_index;
  //    }
  //    //    LOGF_INFO("ParallelVertexDo task_size: {}, num tasks: {}",
  //    task_size,
  //    //              tasks.size());
  //    //    block->LogBlockVertices();
  //    //    block->LogBlockEdges();
  //    //    LOGF_INFO("task num: {}", tasks.size());
  //    task_runner_.SubmitSync(tasks);
  //    // TODO: sync of update_store and graph_ vertex data
  //    //    graph->SyncVertexData();
  //    //    LOG_DEBUG("ParallelVertexDo ends!");
  //  }

  //  void ParallelEdgeDo(core::data_structures::Serializable* graph,
  //                      const FuncEdge& edge_func) {
  //    //    LOG_DEBUG("ParallelEdgeDo begins!");
  //    //    uint32_t task_size = GetTaskSize(block->GetVertexNums());
  //    auto block = static_cast<BLockCSR*>(graph);
  //    uint32_t task_size = GetTaskSize(block->GetVertexNums());
  //    //    uint32_t task_size = task_size_;
  //    core::common::TaskPackage tasks;
  //    VertexIndex begin_index = 0, end_index = 0;
  //    for (; begin_index < block->GetVertexNums();) {
  //      end_index += task_size;
  //      if (end_index > block->GetVertexNums()) {
  //        end_index = block->GetVertexNums();
  //      }
  //      auto task = [&edge_func, block, begin_index, end_index]() {
  //        for (VertexIndex idx = begin_index; idx < end_index; idx++) {
  //          auto degree = block->GetOutDegreeByIndex(idx);
  //          if (degree != 0) {
  //            auto src_id = block->GetVertexID(idx);
  //            VertexID* outEdges = block->GetOutEdgesBaseByIndex(idx);
  //            for (VertexIndex j = 0; j < degree; j++) {
  //              edge_func(src_id, outEdges[j]);
  //            }
  //          }
  //        }
  //      };
  //      tasks.push_back(task);
  //      begin_index = end_index;
  //    }
  //    //    LOGF_INFO("task num: {}", tasks.size());
  //    task_runner_.SubmitSync(tasks);
  //    //    LOG_DEBUG("ParallelEdgeDo ends!");
  //  }
  //
  //  void ParallelEdgeDoWithMutate(core::data_structures::Serializable* graph,
  //                                const FuncEdge& edge_func) {
  //    //    LOG_DEBUG("ParallelEdgeDelDo begins!");
  //    //    uint32_t task_size = GetTaskSize(block->GetVertexNums());
  //    auto block = static_cast<BLockCSR*>(graph);
  //    uint32_t task_size = GetTaskSize(block->GetVertexNums());
  //    //    uint32_t task_size = task_size_;
  //    core::common::TaskPackage tasks;
  //    VertexIndex begin_index = 0, end_index = 0;
  //    //    auto del_bitmap = block->GetEdgeDeleteBitmap();
  //    auto del_bitmap = new core::common::Bitmap();
  //    core::common::EdgeIndex edge_offset = block->GetBlockEdgeOffset();
  //
  //    for (; begin_index < block->GetVertexNums();) {
  //      end_index += task_size;
  //      if (end_index > block->GetVertexNums()) {
  //        end_index = block->GetVertexNums();
  //      }
  //      auto task = [&edge_func, block, begin_index, end_index, del_bitmap,
  //                   edge_offset]() {
  //        for (VertexIndex idx = begin_index; idx < end_index; idx++) {
  //          auto degree = block->GetOutDegreeByIndex(idx);
  //          if (degree != 0) {
  //            auto src_id = block->GetVertexID(idx);
  //            EdgeIndex outOffset_base = block->GetOutOffsetByIndex(idx);
  //            VertexID* outEdges = block->GetOutEdgesBaseByIndex(idx);
  //            for (VertexIndex j = 0; j < degree; j++) {
  //              EdgeIndex edge_index = outOffset_base + j + edge_offset;
  //              if (!del_bitmap->GetBit(edge_index)) {
  //                edge_func(src_id, outEdges[j]);
  //              }
  //            }
  //          }
  //        }
  //      };
  //      tasks.push_back(task);
  //      begin_index = end_index;
  //    }
  //    //    LOGF_INFO("task num: {}", tasks.size());
  //    task_runner_.SubmitSync(tasks);
  //    //    LOG_DEBUG("ParallelEdgedelDo ends!");
  //  }
  //
  //  void ParallelEdgeAndMutateDo(core::data_structures::Serializable* graph,
  //                               const FuncEdgeMutate& edge_del_func) {
  //    //    LOG_INFO("ParallelEdgeAndMutateDo begins!");
  //    auto block = static_cast<BLockCSR*>(graph);
  //    uint32_t task_size = GetTaskSize(block->GetVertexNums());
  //    //    uint32_t task_size = task_size_;
  //    core::common::TaskPackage tasks;
  //    VertexIndex begin_index = 0, end_index = 0;
  //    //    auto del_bitmap = block->GetEdgeDeleteBitmap();
  //    //    core::common::EdgeIndex edge_offset = block->GetBlockEdgeOffset();
  //
  //    for (; begin_index < block->GetVertexNums();) {
  //      end_index += task_size;
  //      if (end_index > block->GetVertexNums()) {
  //        end_index = block->GetVertexNums();
  //      }
  //
  //      auto task = [&edge_del_func, block, begin_index, end_index]() {
  //        for (VertexIndex idx = begin_index; idx < end_index; idx++) {
  //          auto degree = block->GetOutDegreeByIndex(idx);
  //          if (degree != 0) {
  //            auto src_id = block->GetVertexID(idx);
  //            EdgeIndex outOffset_base = block->GetOutOffsetByIndex(idx);
  //            VertexID* outEdges = block->GetOutEdgesBaseByIndex(idx);
  //            for (VertexIndex j = 0; j < degree; j++) {
  //              EdgeIndex edge_index = outOffset_base + j;
  //              if (edge_del_func(src_id, outEdges[j])) {
  //                block->DeleteEdge(idx, edge_index);
  //              }
  //            }
  //          }
  //        }
  //      };
  //      tasks.push_back(task);
  //      begin_index = end_index;
  //    }
  //    //    LOGF_INFO("task num: {}", tasks.size());
  //    task_runner_.SubmitSync(tasks);
  //
  //    block->MutateGraphEdge(&task_runner_);
  //    //    LOG_INFO("ParallelEdgeAndMutateDo ends!");
  //  }

  size_t GetTaskSize(VertexID max_vid) const {
    auto task_num = parallelism_ * task_package_factor_;
    size_t task_size = ceil((double)max_vid / task_num);
    return task_size < 2 ? 2 : task_size;
  }

  // methods for graph
  //  size_t GetGraphEdges() const { return scheduler_.GetGraphEdges(); }

  // two hop info
  VertexDegree GetTwoHopOutDegree(VertexID id) { return 0; }

 protected:
  std::string root_path_ = "";
  // configs
  uint32_t parallelism_ = 10;
  uint32_t task_package_factor_ = 50;

  core::data_structures::BlockMetadata meta_;

  // all blocks are stored in the GraphState of scheduler

  scheduler::MessageHub message_hub_;
  //  scheduler::PramScheduler scheduler_;
  components::Loader loader_;
  //  std::unique_ptr<components::Discharger<io::PramBlockWriter>> discharge_;

  data_structures::NeighborHopInfo neighbor_hop_info_;

  data_structures::graph::BlockCSRGraph graph_;

  core::common::ThreadPool task_runner_;

  data_structures::EdgeBuffer buffer_;
  std::vector<core::common::BlockingQueue<BlockID>> queues_;

  int round_ = 0;

  std::mutex mtx_;
  std::condition_variable cv_;
  uint32_t num_active_subBlock_;

  //  std::mutex pram_mtx_;
  //  std::condition_variable pram_cv_;

  std::chrono::time_point<std::chrono::system_clock> begin_time_;
  std::chrono::time_point<std::chrono::system_clock> compute_end_time_;
  std::chrono::time_point<std::chrono::system_clock> whole_end_time_;
};

}  // namespace sics::graph::nvme::apis

#endif  // GRAPH_SYSTEMS_NVME_APIS_BLOCK_API_H_
