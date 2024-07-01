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
#include "scheduler/graph_state.h"
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
      scheduler::MessageHub* hub, scheduler::GraphState* state) {
    meta_ = meta;
    runner_ = runner;
    graphs_ = graphs;
    hub_ = hub;
    buffer_ = buffer;
    state_ = state;
    app_type_ = common::Configurations::Get()->application;
    use_data_ = app_type_ != common::RandomWalk;
    //    active_.Init(update_store->GetMessageCount());
    //    active_next_.Init(update_store->GetMessageCount());
    if (app_type_ != common::RandomWalk) {
      read_ = new VertexData[meta_->num_vertices];
      write_ = new VertexData[meta_->num_vertices];
    }

    if (app_type_ == common::Sssp) {
      for (int i = 0; i < meta->num_blocks; i++) {
        auto block_meta = meta->blocks.at(i);
        actives_.emplace_back(block_meta.num_vertices);
        next_actives_.emplace_back(block_meta.num_vertices);
      }
    }

    mode_ = common::Configurations::Get()->mode;
  }

 protected:
  // Parallel execute vertex_func in task_size chunks.
  void ParallelVertexDo(const std::function<void(VertexID)>& vertex_func) {
    LOG_INFO("ParallelVertexDo is begin");
    // No need for edges, so operating on all vertex in current subgraph.
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
    //    LOG_INFO("ParallelVertexDo is done");
  }

  void ParallelVertexDoWithActive(
      const std::function<void(VertexID)>& vertex_func) {
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
    //    LOG_INFO("ParallelVertexDo is done");
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
    //    LOG_INFO("ParallelAllVertexDo is done");
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
      //      LOG_INFO("ParallelVertexInitDo is done");
      data_init_ = true;
    }
  }

  void ParallelVertexDoWithEdges(
      const std::function<void(VertexID)>& vertex_func) {
    LOG_DEBUG("ParallelVertexDoWithEdges is begin");
    if (mode_ != common::Normal) {
      auto load = state_->IsEdgesLoaded(static_gid_);
      auto block_meta = meta_->blocks.at(0);
      if (!load) {
        int size_num = state_->GetSubBlockNum(static_gid_);
        scheduler::ReadMessage read;
        read.graph_id = current_gid_;
        hub_->get_reader_queue()->Push(read);
        auto queue = buffer_->GetQueue();
        while (true) {
          auto bid = queue->PopOrWait();
          if (bid == MAX_VERTEX_ID) break;
          auto sub_block_meta = block_meta.sub_blocks.at(bid);
          if (mode_ == common::Static) {
            auto task_size =
                (sub_block_meta.num_vertices + parallelism_ - 1) / parallelism_;
            uint32_t b = sub_block_meta.begin_id, e = sub_block_meta.begin_id;
            common::TaskPackage tasks;
            for (int i = 0; i < parallelism_; i++) {
              e += task_size;
              if (e >= sub_block_meta.end_id) e = sub_block_meta.end_id;
              auto task = [&vertex_func, b, e] {
                for (VertexID id = b; id < e; id++) {
                  vertex_func(id);
                }
              };
              tasks.push_back(task);
              b = e;
            }
            runner_->SubmitSync(tasks);
            size_num = 0;
          } else {
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
            //            LOGF_INFO("Submit task {}", bid);
          }
        }
        std::unique_lock<std::mutex> lock(mtx_);
        if (size_num != 0) {
          cv_.wait(lock, [&size_num]() { return size_num == 0; });
        }
      } else {
        common::TaskPackage tasks;
        auto sub_ids = state_->GetSubBlockIDs(static_gid_);
        if (mode_ == common::Static) {
          assert(sub_ids.size() == 1);
          auto sub_block_meta = block_meta.sub_blocks.at(sub_ids.at(0));
          auto task_size =
              (sub_block_meta.num_vertices + parallelism_ - 1) / parallelism_;
          uint32_t b = sub_block_meta.begin_id, e = sub_block_meta.begin_id;
          for (int i = 0; i < parallelism_; i++) {
            e += task_size;
            if (e >= sub_block_meta.end_id) e = sub_block_meta.end_id;
            auto task = [&vertex_func, b, e] {
              for (VertexID id = b; id < e; id++) {
                vertex_func(id);
              }
            };
            tasks.push_back(task);
            b = e;
          }
          runner_->SubmitSync(tasks);
        } else {
          for (int i = 0; i < sub_ids.size(); i++) {
            auto id = sub_ids.at(i);
            auto sub_block_meta = block_meta.sub_blocks.at(id);
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
      }
      LOG_INFO("task finished");
      Sync(use_readdata_only_);
      return;
    }
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
    //    LOG_INFO("ParallelVertexDoWithEdges is done");
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
        for (EdgeIndex i = 0; i < num_edges; i++) {
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
    if (mode_ != common::Normal) {
      auto load = state_->IsEdgesLoaded(static_gid_);
      auto block_meta = meta_->blocks.at(0);
      if (!load) {
        int size_num = state_->GetSubBlockNum(static_gid_);
        scheduler::ReadMessage read;
        read.graph_id = static_gid_;
        hub_->get_reader_queue()->Push(read);
        auto queue = buffer_->GetQueue();
        while (true) {
          auto bid = queue->PopOrWait();
          if (bid == MAX_VERTEX_ID) break;
          auto sub_block_meta = block_meta.sub_blocks.at(bid);
          if (mode_ == common::Static) {
            auto task_size =
                (sub_block_meta.num_vertices + parallelism_ - 1) / parallelism_;
            uint32_t b = sub_block_meta.begin_id, e = sub_block_meta.begin_id;
            common::TaskPackage tasks;
            for (int i = 0; i < parallelism_; i++) {
              e += task_size;
              if (e >= sub_block_meta.end_id) e = sub_block_meta.end_id;
              auto task = [&edge_func, this, sub_block_meta, b, e]() {
                auto& block = graphs_->at(0);
                auto bitmap = block.GetDelBitmap(sub_block_meta.id);
                for (VertexID id = b; id < e; id++) {
                  auto degree = block.GetOutDegree(id);
                  if (degree != 0) {
                    auto edges = block.GetOutEdges(id);
                    auto init_offset = block.GetInitOffset(id);
                    for (VertexID i = 0; i < degree; i++) {
                      if (!bitmap->GetBit(init_offset + i)) {
                        auto dst = edges[i];
                        edge_func(id, dst);
                      }
                    }
                  }
                }
              };
              tasks.push_back(task);
              b = e;
            }
            runner_->SubmitSync(tasks);
            size_num = 0;
          } else {
            auto task = [&edge_func, &size_num, this, sub_block_meta]() {
              auto id = sub_block_meta.begin_id;
              auto& block = graphs_->at(0);
              auto num_edges = sub_block_meta.num_edges;
              auto edges = block.GetAllEdges(sub_block_meta.id);
              auto bitmap = block.GetDelBitmap(sub_block_meta.id);
              uint32_t degree = block.GetOutDegree(id);
              while (degree == 0 && id < sub_block_meta.end_id) {
                id++;
                degree = block.GetOutDegree(id);
              }
              for (EdgeIndex i = 0; i < num_edges; i++) {
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
                LOGF_FATAL("Error executing edge parallel do! {} != {}", id,
                           sub_block_meta.id);
              }
              std::lock_guard<std::mutex> lock(mtx_);
              size_num -= 1;
              cv_.notify_all();
            };
            runner_->SubmitAsync(task);
            //            LOGF_INFO("submit task {}", bid);
          }
        }
        std::unique_lock<std::mutex> lock(mtx_);
        if (size_num != 0) {
          cv_.wait(lock, [&size_num]() { return size_num == 0; });
        }
      } else {
        // edge block is loaded!
        common::TaskPackage tasks;
        auto sub_ids = state_->GetSubBlockIDs(static_gid_);
        if (mode_ == common::Static) {
          assert(sub_ids.size() == 1);
          auto sub_block_meta = block_meta.sub_blocks.at(sub_ids.at(0));
          auto task_size =
              (sub_block_meta.num_vertices + parallelism_ - 1) / parallelism_;
          uint32_t b = sub_block_meta.begin_id, e = sub_block_meta.begin_id;
          for (int i = 0; i < parallelism_; i++) {
            e += task_size;
            if (e >= sub_block_meta.end_id) e = sub_block_meta.end_id;
            auto task = [&edge_func, this, sub_block_meta, b, e]() {
              auto& block = graphs_->at(0);
              auto bitmap = block.GetDelBitmap(sub_block_meta.id);
              for (VertexID id = b; id < e; id++) {
                auto degree = block.GetOutDegree(id);
                if (degree != 0) {
                  auto edges = block.GetOutEdges(id);
                  auto init_offset = block.GetInitOffset(id);
                  for (VertexID i = 0; i < degree; i++) {
                    if (!bitmap->GetBit(init_offset + i)) {
                      edge_func(id, edges[i]);
                    }
                  }
                }
              }
            };
            tasks.push_back(task);
            b = e;
          }
          runner_->SubmitSync(tasks);
        } else {
          for (int i = 0; i < sub_ids.size(); i++) {
            auto id = sub_ids.at(i);
            auto sub_block_meta = block_meta.sub_blocks.at(id);
            auto task = [&edge_func, this, sub_block_meta]() {
              auto id = sub_block_meta.begin_id;
              auto& block = graphs_->at(0);
              auto num_edges = sub_block_meta.num_edges;
              auto edges = block.GetAllEdges(sub_block_meta.id);
              auto bitmap = block.GetDelBitmap(sub_block_meta.id);
              auto degree = block.GetOutDegree(id);
              while (degree == 0 && id < sub_block_meta.end_id) {
                id++;
                degree = block.GetOutDegree(id);
              }
              for (EdgeIndex i = 0; i < num_edges; i++) {
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
      }
      Sync(use_readdata_only_);
      LOG_DEBUG("ParallelEdgeMutateDo is done");
      return;
    }
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
          for (EdgeIndex i = 0; i < num_edges; i++) {
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
            LOGF_FATAL("Error executing edge parallel do! {} != {}", id,
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
          for (EdgeIndex i = 0; i < num_edges; i++) {
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
    if (mode_ == common::Static) {
      auto load = state_->IsEdgesLoaded(static_gid_);
      auto block_meta = meta_->blocks.at(0);
      if (!load) {
        int size_num = state_->GetSubBlockNum(static_gid_);
        scheduler::ReadMessage read;
        read.graph_id = static_gid_;
        hub_->get_reader_queue()->Push(read);
        auto queue = buffer_->GetQueue();
        while (true) {
          auto bid = queue->PopOrWait();
          if (bid == MAX_VERTEX_ID) break;
          auto sub_block_meta = block_meta.sub_blocks.at(bid);
          auto task = [&edge_del_func, &size_num, this, sub_block_meta]() {
            auto id = sub_block_meta.begin_id;
            auto& block = graphs_->at(0);
            auto num_edges = sub_block_meta.num_edges;
            auto edges = block.GetAllEdges(sub_block_meta.id);
            auto bitmap = block.GetDelBitmap(sub_block_meta.id);
            uint32_t degree = block.GetOutDegree(id);
            while (degree == 0 && id < sub_block_meta.end_id) {
              id++;
              degree = block.GetOutDegree(id);
            }
            for (EdgeIndex i = 0; i < num_edges; i++) {
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
          LOGF_INFO("submit task {}", bid);
        }
        std::unique_lock<std::mutex> lock(mtx_);
        if (size_num != 0) {
          cv_.wait(lock, [&size_num]() { return size_num == 0; });
        }
      } else {
        common::TaskPackage tasks;
        auto sub_ids = state_->GetSubBlockIDs(static_gid_);
        for (int i = 0; i < sub_ids.size(); i++) {
          auto id = sub_ids.at(i);
          auto sub_block_meta = block_meta.sub_blocks.at(id);
          auto task = [&edge_del_func, this, sub_block_meta]() {
            auto id = sub_block_meta.begin_id;
            auto& block = graphs_->at(0);
            auto num_edges = sub_block_meta.num_edges;
            auto edges = block.GetAllEdges(sub_block_meta.id);
            auto bitmap = block.GetDelBitmap(sub_block_meta.id);
            auto degree = block.GetOutDegree(id);
            while (degree == 0 && id < sub_block_meta.end_id) {
              id++;
              degree = block.GetOutDegree(id);
            }
            for (EdgeIndex i = 0; i < num_edges; i++) {
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
      return;
    }
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
          for (EdgeIndex i = 0; i < num_edges; i++) {
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
          for (EdgeIndex i = 0; i < num_edges; i++) {
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

  void InitVertexActive(VertexID id) {
    auto block_id = GetBlockID(id);
    auto idx = id - meta_->blocks.at(block_id).begin_id;
    actives_.at(block_id).SetBit(idx);
  }

  void SetVertexActive(VertexID id) {
    auto block_id = GetBlockID(id);
    auto idx = id - meta_->blocks.at(block_id).begin_id;
    next_actives_.at(block_id).SetBit(idx);
  }

  void SyncSubGraphActive() {
    std::swap(actives_.at(current_gid_), next_actives_.at(current_gid_));
    next_actives_.at(current_gid_).Clear();
  }

  size_t GetActiveNum() {
    auto& bitmap = actives_.at(current_gid_);
    //    LOGF_INFO("Gid: {}, active num: {}", current_gid_, bitmap.Count());
    return bitmap.Count();
  }

  void Sync(bool read_only = false) {
    if (use_data_) {
      if (!read_only) {
        memcpy(read_, write_, sizeof(VertexData) * meta_->num_vertices);
      }
    }
  }

  // Read and Write function.

  VertexData Read(VertexID id) { return read_[id]; }

  void Write(VertexID id, VertexData vdata) { write_[id] = vdata; }

  void WriteActive(VertexID id, VertexData vdata) {
    write_[id] = vdata;
    active++;
  }

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

  void WriteOneBuffer(VertexID id, VertexData vdata) {
    active++;
    read_[id] = vdata;
  }

  VertexIndex GetIndexByID(VertexID id) {
    auto block_id = GetBlockID(id);
    return graphs_->at(block_id).GetVertexIndex(id);
  }

  VertexID GetNeiMinId(VertexID id) {
    auto block_id = GetBlockID(id);
    return graphs_->at(block_id).GetNeiMinId(id);
  }

  VertexDegree GetOutDegree(VertexID id) {
    auto block_id = GetBlockID(id);
    return graphs_->at(block_id).GetOutDegree(id);
  }

  VertexID* GetOutEdges(VertexID id) {
    auto block_id = GetBlockID(id);
    return graphs_->at(block_id).GetOutEdges(id);
  }

  void DeleteEdge(VertexID id, EdgeIndex idx) {
    auto block_id = GetBlockID(id);
    graphs_->at(block_id).DeleteEdge(id, idx);
  }

  void DeleteEdgeByVertex(VertexID id, EdgeIndex idx) {
    auto block_id = GetBlockID(id);
    graphs_->at(block_id).DeleteEdgeByVertex(id, idx);
  }

  bool IsEdgeDelete(VertexID id, EdgeIndex idx) {
    auto block_id = GetBlockID(id);
    return graphs_->at(block_id).IsEdgeDelete(id, idx);
  }

  void SetRound(int round) override { round_ = round; }

  void SetCurrentGid(GraphID gid) override {
    if (mode_ != common::Normal) {
      static_gid_ = gid;
      current_gid_ = 0;
    } else {
      current_gid_ = gid;
    }
  }

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

  void LogCurrentGraphInfo() { graphs_->at(current_gid_).LogGraphInfo(); }

  void LogCurrentGraphDelInfo() { graphs_->at(current_gid_).LogDelGraphInfo(); }

  size_t GetSubGraphNumEdges() {
    if (mode_ != common::Normal) {
      auto ids = state_->GetSubBlockIDs(static_gid_);
      return graphs_->at(0).GetNumEdges(ids);
    }
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
  scheduler::GraphState* state_ = nullptr;

  common::TaskRunner* runner_;

  data_structures::graph::MutableBlockCSRGraph* graph_;

  int round_ = 0;

  size_t active = 0;

  std::vector<common::Bitmap> actives_;
  std::vector<common::Bitmap> next_actives_;

  common::Bitmap active_;
  common::Bitmap active_next_;

  // block ids in one sub_graph, -1 means the subgraph is all loaded.
  scheduler::EdgeBuffer2* buffer_;
  scheduler::MessageHub* hub_;

  bool data_init_ = false;
  VertexData* read_ = nullptr;
  VertexData* write_ = nullptr;

  GraphID current_gid_;
  GraphID static_gid_;
  std::vector<data_structures::graph::MutableBlockCSRGraph>* graphs_;

  common::ModeType mode_;

  std::mutex mtx_;
  std::condition_variable cv_;

  // configs
  uint32_t parallelism_;
  uint32_t task_package_factor_;
  common::ApplicationType app_type_;
  bool use_readdata_only_ = false;
  bool use_data_ = true;
  // TODO: add UpdateStore as a member here.
};

}  // namespace sics::graph::core::apis

#endif  // GRAPH_SYSTEMS_PLANAR_APP_BASE_OP_H
