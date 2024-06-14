#ifndef GRAPH_SYSTEMS_CORE_APIS_PLANAR_APP_OP_H_
#define GRAPH_SYSTEMS_CORE_APIS_PLANAR_APP_OP_H_

#include <condition_variable>
#include <functional>
#include <mutex>
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
#include "data_structures/graph/mutable_block_csr_graph.h"
#include "data_structures/serializable.h"
#include "io/mutable_csr_reader.h"
#include "io/mutable_csr_writer.h"
#include "scheduler/edge_buffer.h"
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
template <typename VertexData>
class PlanarAppOpBase : public PIE {
  using GraphID = common::GraphID;
  using BlockID = common::BlockID;
  using VertexID = common::VertexID;
  using VertexIndex = common::VertexIndex;
  using EdgeIndex = common::EdgeIndex;
  using EdgeIndexS = common::EdgeIndexS;
  using VertexDegree = common::VertexDegree;
  using CSRBlockGraph = data_structures::graph::MutableBlockCSRGraph;

 public:
  PlanarAppOpBase() = default;
  PlanarAppOpBase(const std::string& root_path)
      : metadata_(root_path),
        parallelism_(common::Configurations::Get()->parallelism),
        task_package_factor_(
            common::Configurations::Get()->task_package_factor),
        app_type_(common::Configurations::Get()->application) {
    graphs_.resize(metadata_.num_blocks);
    for (int i = 0; i < metadata_.num_blocks; i++) {
      graphs_.at(i).Init(root_path, &metadata_.blocks.at(i));
    }
    edge_buffer_.Init(&metadata_, &graphs_);
    loader_.Init(root_path, &hub, &metadata_, &edge_buffer_, &graphs_);
    executer_.Init(&hub);

    use_readdata_only_ = app_type_ == common::Coloring;
    LOGF_INFO("vertex data sync: {}", !use_readdata_only_);
    LOGF_INFO("use read data only: {}", use_readdata_only_);
    Init();
  }
  // TODO: add UpdateStore as a parameter, so that PEval, IncEval and Assemble
  //  can access global messages in it.
  //  PlanarAppOpBase()
  //      : parallelism_(common::Configurations::Get()->parallelism),
  //        task_package_factor_(
  //            common::Configurations::Get()->task_package_factor),
  //        app_type_(common::Configurations::Get()->application) {
  //    use_readdata_only_ = app_type_ == common::Coloring;
  //    LOGF_INFO("vertex data sync: {}", !use_readdata_only_);
  //    LOGF_INFO("use read data only: {}", use_readdata_only_);
  //  }

  void Init() {
    active_edge_blocks_.resize(metadata_.num_blocks);
    for (uint32_t i = 0; i < metadata_.num_blocks; i++) {
      active_edge_blocks_.at(i).Init(metadata_.blocks.at(i).num_sub_blocks);
    }
    read_data_ = new VertexData[metadata_.num_vertices];
    write_data_ = new VertexData[metadata_.num_vertices];

    loader_.Start();
  }

  ~PlanarAppOpBase() override {
    delete[] read_data_;
    delete[] write_data_;

    loader_.StopAndJoin();
  };

  virtual void SetRound(int round) { round_ = round; }

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
      LOGF_INFO("Subgraph {} task finished", gid);
    }
    SyncVertexState(use_readdata_only_);
    LOG_INFO("ParallelVertexDo is done");
  }

  // Users will use edges of block, so we need to load edge block first.
  void ParallelVertexDoWithEdges(
      const std::function<void(VertexID)>& vertex_func) {
    LOG_INFO("ParallelVertexDoWithEdges begins!");
    for (uint32_t gid = 0; gid < metadata_.num_blocks; gid++) {
      current_gid = gid;
      active_edge_blocks_.at(gid).Fill();  // Now active all blocks.
      // First, edge block current in memory.
      common::TaskPackage tasks;
      auto blocks_in_memory = edge_buffer_.GetBlocksInMemory(gid);
      auto blocks_to_read = edge_buffer_.GetBlocksToRead(gid);
      auto size_sum = blocks_in_memory.size() + blocks_to_read.size();
      for (unsigned int sub_block_id : blocks_in_memory) {
        auto sub_block = metadata_.blocks.at(gid).sub_blocks.at(sub_block_id);
        tasks.push_back([this, &size_sum, &vertex_func, sub_block]() {
          auto begin_id = sub_block.begin_id;
          auto end_id = sub_block.end_id;
          for (VertexID id = begin_id; id < end_id; id++) {
            vertex_func(id);
          }
          edge_buffer_.FinishOneEdgeBlock(current_gid, sub_block.id);
          std::lock_guard<std::mutex> lock(mtx_);
          size_sum -= 1;
          cv_.notify_all();
        });
      }
      // TODO: async submit!
      executer_.GetTaskRunner()->SubmitAsync(tasks);
      // Secondly, Wait for io load left edg block.
      // TODO: Block when no edge block is ready!
      if (blocks_to_read.size() != 0) {
        loader_.SetToReadBlocksID(gid, &blocks_to_read);  // Trigger load.
        while (true) {
          tasks.clear();
          // TODO: use edge_buffer to block.
          auto resp = hub.get_response_queue()->PopOrWait();
          scheduler::ReadMessage read_resp;
          resp.Get(&read_resp);
          if (read_resp.terminated) break;
          auto num_read = read_resp.num_edge_blocks;
          auto read_edge_block_id = GetLoadBlocksID(num_read);
          for (int j = 0; j < read_edge_block_id.size(); j++) {
            auto sub_block_id = read_edge_block_id[j];
            auto sub_block =
                metadata_.blocks.at(gid).sub_blocks.at(sub_block_id);
            tasks.push_back([this, &size_sum, &vertex_func, sub_block]() {
              auto begin_id = sub_block.begin_id;
              auto end_id = sub_block.end_id;
              for (VertexID id = begin_id; id < end_id; id++) {
                vertex_func(id);
              }
              edge_buffer_.FinishOneEdgeBlock(current_gid, sub_block.id);
              std::lock_guard<std::mutex> lock(mtx_);
              size_sum -= 1;
              cv_.notify_all();
            });
          }
          executer_.GetTaskRunner()->SubmitAsync(tasks);
          LOGF_INFO("submit edge blocks: {}", GetIds(read_edge_block_id));
          // Decide if release edge block for load other block.
        }
      }

      // Block when any edge_block has not finished.
      std::unique_lock<std::mutex> lock(mtx_);
      if (size_sum != 0) {
        cv_.wait(lock, [&size_sum]() { return size_sum == 0; });
      }
      LOGF_INFO("SubGraph: {} finish!", gid);
    }
    edge_buffer_.Reset();
    LOG_INFO("ParallelVertexDoWithEdges is done!");
  }

  std::string GetIds(std::vector<BlockID>& ids) {
    std::string res = "";
    for (auto id : ids) {
      res += std::to_string(id) + " ";
    }
    return res;
  }

  void ParallelEdgeDo2(
      const std::function<void(VertexID, VertexID)>& edge_func) {}

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

  // Log functions.
  void LogVertexState() {
    for (VertexID id = 0; id < metadata_.num_vertices; id++) {
      LOGF_INFO("Vertex: {}, read: {} write: {}", id, read_data_[id],
                write_data_[id]);
    }
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

  std::vector<BlockID> GetLoadBlocksID(size_t num) {
    std::vector<BlockID> res;
    while (num--) {
      auto entry = edge_buffer_.PopOneEdgeBlock();
      res.push_back(entry.second);
    }
    return res;
  }

  void ReleaseEdgeBlock(BlockID bid) {
    edge_buffer_.ReleaseBuffer(current_gid, bid);
  }

 protected:
  common::BlockingQueue<VertexID>* active_queue_;

  int round_ = 0;

  // Active for vertex in each round.
  common::Bitmap active_;
  common::Bitmap active_next_;

  // graph_manager ------
  data_structures::TwoDMetadata metadata_;

  // Vertex states.
  VertexData* read_data_ = nullptr;
  VertexData* write_data_ = nullptr;

  common::GraphID active_gid_ = 0;
  std::vector<common::Bitmap> active_edge_blocks_;

  // Graph index and edges structure. Init at beginning.
  GraphID current_gid;
  std::vector<CSRBlockGraph> graphs_;
  // Buffer size management.
  scheduler::EdgeBuffer edge_buffer_;
  // --------

  scheduler::MessageHub hub;

  components::LoaderOp loader_;
  components::ExecutorOp executer_;

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

#endif  // GRAPH_SYSTEMS_CORE_APIS_PLANAR_APP_OP_H_
