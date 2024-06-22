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
#include "scheduler/edge_buffer2.h"
#include "scheduler/message_hub.h"
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
  using VertexDegree = common::VertexDegree;
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

  ~PlanarAppBaseOp() {
    delete[] read_;
    read_ = nullptr;
    delete[] write_;
    write_ = nullptr;
  }

  virtual void AppInit(
      common::TaskRunner* runner, data_structures::TwoDMetadata* meta,
      scheduler::EdgeBuffer2* buffer,
      std::vector<data_structures::graph::MutableBlockCSRGraph>* graphs,
      scheduler::MessageHub* hub) {
    meta_ = meta;
    runner_ = runner;
    graphs_ = graphs;
    hub_ = hub;
    buffer_ = buffer;
    //    active_.Init(update_store->GetMessageCount());
    //    active_next_.Init(update_store->GetMessageCount());
    read_ = new VertexData[meta_->num_vertices];
    write_ = new VertexData[meta_->num_vertices];
  }

 protected:
  // Parallel execute vertex_func in task_size chunks.
  void ParallelVertexDo(const std::function<void(VertexID)>& vertex_func) {
    LOG_INFO("ParallelVertexDo is begin");
    auto block_meta = meta_->blocks.at(current_gid_);
    uint32_t task_size = GetTaskSize(block_meta.num_vertices);
    common::TaskPackage tasks;
    tasks.reserve(parallelism_ * task_package_factor_);
    VertexID begin_id = 0, end_id = 0;
    for (; begin_id < block_meta.num_vertices;) {
      end_id += task_size;
      if (end_id > block_meta.num_vertices) end_id = block_meta.num_vertices;
      auto task = [&vertex_func, begin_id, end_id]() {
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

  void ParallelAllVertexDo(const std::function<void(VertexID)>& vertex_func) {
    LOG_INFO("ParallelAllVertexDo is begin");
    uint32_t task_size = GetTaskSize(meta_->num_vertices);
    common::TaskPackage tasks;
    tasks.reserve(parallelism_ * task_package_factor_);
    VertexID begin_id = 0, end_id = 0;
    for (; begin_id < meta_->num_vertices;) {
      end_id += task_size;
      if (end_id > meta_->num_vertices) end_id = meta_->num_vertices;
      auto task = [&vertex_func, begin_id, end_id]() {
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
    LOG_INFO("ParallelAllVertexDo is done");
  }

  void ParallelVertexInitDo(const std::function<void(VertexID)>& vertex_func) {
    if (!data_init_) {
      LOG_INFO("ParallelVertexInitDo is begin");
      uint32_t task_size = GetTaskSize(meta_->num_vertices);
      common::TaskPackage tasks;
      tasks.reserve(parallelism_ * task_package_factor_);
      VertexID begin_id = 0, end_id = 0;
      for (; begin_id < meta_->num_vertices;) {
        end_id += task_size;
        if (end_id > meta_->num_vertices) end_id = meta_->num_vertices;
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
      runner_->SubmitSync(tasks);
      // TODO: sync of update_store and graph_ vertex data
      LOG_INFO("task finished");
      Sync(use_readdata_only_);
      LOG_INFO("ParallelVertexInitDo is done");
      data_init_ = true;
    }
  }

  void ParallelVertexDoWithEdges(
      const std::function<void(VertexID)>& vertex_func) {
    LOG_DEBUG("ParallelVertexDoWithEdges is begin");
    auto load = graphs_->at(current_gid_).IsEdgesLoaded();
    auto block_meta = meta_->blocks.at(current_gid_);
    if (!load) {
      int size_num = block_meta.num_sub_blocks;
      scheduler::ReadMessage read;
      read.graph_id = current_gid_;
      hub_->get_reader_queue()->Push(read);
      auto queue = buffer_->GetQueue();
      while (true) {
        auto bid = queue->PopOrWait();
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
        //        LOGF_INFO("Submit task {}", bid);
      }
      std::unique_lock<std::mutex> lock(mtx_);
      if (size_num != 0) {
        cv_.wait(lock, [&size_num]() { return size_num == 0; });
      }
    } else {
      common::TaskPackage tasks;
      for (int i = 0; i < block_meta.num_sub_blocks; i++) {
        auto sub_block_meta = block_meta.sub_blocks.at(i);
        auto task = [&vertex_func, sub_block_meta]() {
          auto begin_id = sub_block_meta.begin_id;
          auto end_id = sub_block_meta.end_id;
          for (VertexID id = begin_id; id < end_id; id++) {
            vertex_func(id);
          }
        };
        tasks.emplace_back(task);
      }
      runner_->SubmitSync(tasks);
    }
    LOG_INFO("task finished");
    Sync(use_readdata_only_);
    LOG_INFO("ParallelVertexDoWithEdges is done");
  }

  // Parallel execute edge_func in task_size chunks.
  void ParallelEdgeDo(
      const std::function<void(VertexID, VertexID)>& edge_func) {
    LOG_DEBUG("ParallelEdgeDo begins");
    auto block_meta = meta_->blocks.at(current_gid_);
    int size_num = block_meta.num_sub_blocks;
    auto queue = buffer_->GetQueue();
    while (true) {
      auto bid = queue->PopOrWait();
      if (bid == MAX_VERTEX_ID) break;
      auto sub_block_meta = block_meta.sub_blocks.at(bid);
      auto task = [&edge_func, &size_num, this, sub_block_meta]() {
        auto id = sub_block_meta.begin_id;
        auto& block = graphs_->at(current_gid_);
        auto num_edges = sub_block_meta.num_edges;
        auto edges = block.GetAllEdges(sub_block_meta.id);
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

  void ParallelEdgeMutateDo(
      const std::function<void(VertexID, VertexID)>& edge_func) {
    LOG_DEBUG("ParallelEdgeMutateDo begins");
    auto load = graphs_->at(current_gid_).IsEdgesLoaded();
    auto block_meta = meta_->blocks.at(current_gid_);
    if (!load) {
      int size_num = block_meta.num_sub_blocks;
      scheduler::ReadMessage read;
      read.graph_id = current_gid_;
      hub_->get_reader_queue()->Push(read);
      auto queue = buffer_->GetQueue();
      while (true) {
        auto bid = queue->PopOrWait();
        if (bid == MAX_VERTEX_ID) break;
        auto sub_block_meta = block_meta.sub_blocks.at(bid);
        auto task = [&edge_func, &size_num, this, sub_block_meta]() {
          auto id = sub_block_meta.begin_id;
          auto& block = graphs_->at(current_gid_);
          auto num_edges = sub_block_meta.num_edges;
          auto edges = block.GetAllEdges(sub_block_meta.id);
          auto bitmap = block.GetDelBitmap(sub_block_meta.id);
          uint32_t degree = block.GetOutDegree(id);
          while (degree == 0 && id < sub_block_meta.end_id) {
            id++;
            degree = block.GetOutDegree(id);
          }
          for (EdgeIndexS i = 0; i < num_edges; i++) {
            if (!bitmap->GetBit(i)) {
              edge_func(id, edges[i]);
            }
            degree--;
            while (degree == 0 && id < sub_block_meta.end_id) {
              id++;
              degree = block.GetOutDegree(id);
            }
          }
          if (id != sub_block_meta.end_id) {
            LOGF_FATAL("Error executing edge parallel do! {}",
                       sub_block_meta.id);
          }
          std::lock_guard<std::mutex> lock(mtx_);
          size_num -= 1;
          cv_.notify_all();
        };
        runner_->SubmitAsync(task);
        //        LOGF_INFO("submit task {}", bid);
      }
      std::unique_lock<std::mutex> lock(mtx_);
      if (size_num != 0) {
        cv_.wait(lock, [&size_num]() { return size_num == 0; });
      }
    } else {
      common::TaskPackage tasks;
      for (int i = 0; i < block_meta.num_sub_blocks; i++) {
        auto sub_block_meta = block_meta.sub_blocks.at(i);
        auto task = [&edge_func, this, sub_block_meta]() {
          auto id = sub_block_meta.begin_id;
          auto& block = graphs_->at(current_gid_);
          auto num_edges = sub_block_meta.num_edges;
          auto edges = block.GetAllEdges(sub_block_meta.id);
          auto bitmap = block.GetDelBitmap(sub_block_meta.id);
          auto degree = block.GetOutDegree(id);
          while (degree == 0 && id < sub_block_meta.end_id) {
            id++;
            degree = block.GetOutDegree(id);
          }
          for (EdgeIndexS i = 0; i < num_edges; i++) {
            if (!bitmap->GetBit(i)) {
              edge_func(id, edges[i]);
            }
            degree--;
            while (degree == 0 && id < sub_block_meta.end_id) {
              id++;
              degree = block.GetOutDegree(id);
            }
          }
          if (id != sub_block_meta.end_id) {
            LOG_FATAL("Error executing edge parallel do!");
          }
        };
        tasks.push_back(task);
      }
      runner_->SubmitSync(tasks);
    }
    Sync(use_readdata_only_);
    LOG_DEBUG("ParallelEdgeMutateDo is done");
  }

  void ParallelEdgeMutateDo(
      const std::function<void(VertexID, VertexID, EdgeIndex)>& edge_del_func) {
    LOG_DEBUG("ParallelEdgeDeleteDo begins");
    auto load = graphs_->at(current_gid_).IsEdgesLoaded();
    auto block_meta = meta_->blocks.at(current_gid_);
    if (!load) {
      int size_num = block_meta.num_sub_blocks;
      scheduler::ReadMessage read;
      read.graph_id = current_gid_;
      hub_->get_reader_queue()->Push(read);
      auto queue = buffer_->GetQueue();
      while (true) {
        auto bid = queue->PopOrWait();
        if (bid == MAX_VERTEX_ID) break;
        auto sub_block_meta = block_meta.sub_blocks.at(bid);
        auto task = [&edge_del_func, &size_num, this, sub_block_meta]() {
          auto id = sub_block_meta.begin_id;
          auto& block = graphs_->at(current_gid_);
          auto num_edges = sub_block_meta.num_edges;
          auto edges = block.GetAllEdges(sub_block_meta.id);
          auto bitmap = block.GetDelBitmap(sub_block_meta.id);
          uint32_t degree = block.GetOutDegree(id);
          while (degree == 0 && id < sub_block_meta.end_id) {
            id++;
            degree = block.GetOutDegree(id);
          }
          for (EdgeIndexS i = 0; i < num_edges; i++) {
            if (!bitmap->GetBit(i)) {
              edge_del_func(id, edges[i], i);
            }
            degree--;
            while (degree == 0 && id < sub_block_meta.end_id) {
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
        //        LOGF_INFO("submit task {}", bid);
      }
      std::unique_lock<std::mutex> lock(mtx_);
      if (size_num != 0) {
        cv_.wait(lock, [&size_num]() { return size_num == 0; });
      }
    } else {
      common::TaskPackage tasks;
      for (int i = 0; i < block_meta.num_sub_blocks; i++) {
        auto sub_block_meta = block_meta.sub_blocks.at(i);
        auto task = [&edge_del_func, this, sub_block_meta]() {
          auto id = sub_block_meta.begin_id;
          auto& block = graphs_->at(current_gid_);
          auto num_edges = sub_block_meta.num_edges;
          auto edges = block.GetAllEdges(sub_block_meta.id);
          auto bitmap = block.GetDelBitmap(sub_block_meta.id);
          auto degree = block.GetOutDegree(id);
          while (degree == 0 && id < sub_block_meta.end_id) {
            id++;
            degree = block.GetOutDegree(id);
          }
          for (EdgeIndexS i = 0; i < num_edges; i++) {
            if (!bitmap->GetBit(i)) {
              edge_del_func(id, edges[i], i);
            }
            degree--;
            while (degree == 0 && id < sub_block_meta.end_id) {
              id++;
              degree = block.GetOutDegree(id);
            }
          }
          if (id != sub_block_meta.end_id) {
            LOG_FATAL("Error executing edge parallel do!");
          }
        };
        tasks.push_back(task);
      }
      runner_->SubmitSync(tasks);
    }
    Sync(use_readdata_only_);
    LOG_DEBUG("ParallelEdgeDeleteDo is done");
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

  void Sync(bool read_only = false) {
    if (!read_only) {
      memcpy(read_, write_, sizeof(VertexData) * meta_->num_vertices);
    }
  }

  // Read and Write function.

  VertexData Read(VertexID id) { return read_[id]; }

  void Write(VertexID id, VertexData vdata) { write_[id] = vdata; }

  void WriteMin(VertexID id, VertexData vdata) {
    if (core::util::atomic::WriteMin(&write_[id], vdata)) {
      active++;
    }
  }

  void WriteMax(VertexID id, VertexData vdata) {
    if (core::util::atomic::WriteMax(&write_[id], vdata)) {
      active++;
    }
  }

  void WriteAdd(VertexID id, VertexData vdata) {
    if (core::util::atomic::WriteAdd(&write_[id], vdata)) {
      active++;
    }
  }

  VertexDegree GetOutDegree(VertexID id) {
    auto block_id = GetBlockID(id);
    return graphs_->at(block_id).GetOutDegree(id);
  }

  VertexID* GetOutEdges(VertexID id) {
    auto block_id = GetBlockID(id);
    return graphs_->at(block_id).GetOutEdges(id);
  }

  void DeleteEdge(VertexID id, EdgeIndexS idx) {
    auto block_id = GetBlockID(id);
    graphs_->at(block_id).DeleteEdge(id, idx);
  }

  void SetRound(int round) override { round_ = round; }

  void SetCurrentGid(GraphID gid) override { current_gid_ = gid; }

  void SetActive() { active = 1; }

  void UnsetActive() { active = 0; }

  size_t IsActive() override { return active; }

  void SetInActive() override { active = 0; }

  // Log functions.
  void LogVertexState() {
    for (VertexID id = 0; id < meta_->num_vertices; id++) {
      LOGF_INFO("Vertex: {}, read: {} write: {}", id, read_[id], write_[id]);
    }
  }

  void LogCurrentGraphVertexState() {
    auto meta = meta_->blocks.at(current_gid_);
    for (VertexID id = meta.begin_id; id < meta.end_id; id++) {
      LOGF_INFO("Vertex: {}, read: {} write: {}", id, read_[id], write_[id]);
    }
  }

  void LogCurrentGraphDelInfo() { graphs_->at(current_gid_).LogDelGraphInfo(); }

  size_t GetSubGraphNumEdges() {
    return graphs_->at(current_gid_).GetNumEdges();
  }

 private:
  BlockID GetBlockID(VertexID id) {
    for (int i = 0; i < meta_->num_blocks; i++) {
      if (id < meta_->blocks.at(i).end_id) {
        return i;
      }
    }
    LOG_FATAL("VertexID: ", id, " is out of range!");
  }

 protected:
  data_structures::TwoDMetadata* meta_;

  common::TaskRunner* runner_;

  data_structures::graph::MutableBlockCSRGraph* graph_;

  int round_ = 0;

  size_t active = 0;

  common::Bitmap active_;
  common::Bitmap active_next_;

  // block ids in one sub_graph, -1 means the subgraph is all loaded.
  scheduler::EdgeBuffer2* buffer_;
  scheduler::MessageHub* hub_;

  bool data_init_ = false;
  VertexData* read_;
  VertexData* write_;

  GraphID current_gid_;
  std::vector<data_structures::graph::MutableBlockCSRGraph>* graphs_;

  std::mutex mtx_;
  std::condition_variable cv_;

  // configs
  uint32_t parallelism_;
  uint32_t task_package_factor_;
  common::ApplicationType app_type_;
  bool use_readdata_only_ = true;
  // TODO: add UpdateStore as a member here.
};

}  // namespace sics::graph::core::apis

#endif  // GRAPH_SYSTEMS_PLANAR_APP_BASE_OP_H
