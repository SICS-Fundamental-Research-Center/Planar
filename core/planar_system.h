#include <chrono>
#include <iostream>
#include <memory>

#include "apis/planar_app_base.h"
#include "common/blocking_queue.h"
#include "common/types.h"
#include "components/discharger.h"
#include "components/executor.h"
#include "components/loader.h"
#include "components/loader_op2.h"
#include "data_structures/graph/mutable_block_csr_graph.h"
#include "data_structures/graph_metadata.h"
#include "io/csr_edge_block_reader.h"
#include "io/mutable_csr_reader.h"
#include "io/mutable_csr_writer.h"
#include "scheduler/edge_buffer2.h"
#include "scheduler/scheduler.h"
#include "update_stores/bsp_update_store.h"

namespace sics::graph::core::planar_system {

template <typename AppType>
class Planar {
  using GraphID = common::GraphID;
  using VertexID = common::VertexID;
  using VertexData = typename AppType::VertexData;
  using EdgeData = typename AppType::EdgeData;

 public:
  Planar() = default;
  Planar(const std::string& root_path)
      : meta_(root_path),
        scheduler_(std::make_unique<scheduler::Scheduler>(root_path)) {
    update_store_ =
        std::make_unique<update_stores::BspUpdateStore<VertexData, EdgeData>>(
            root_path, scheduler_->GetVertexNumber());

    graphs_.resize(meta_.num_blocks);
    for (int i = 0; i < meta_.num_blocks; i++) {
      graphs_.at(i).Init(root_path, &meta_.blocks.at(i));
    }

    edge_buffer.Init(&meta_, &graphs_);
    // components for reader, writer and executor
    loader2_.Init(root_path, scheduler_->GetMessageHub(), &meta_, &edge_buffer,
                  &graphs_, &queue_);
    executer_ =
        std::make_unique<components::Executor>(scheduler_->GetMessageHub());

    // set scheduler info
    scheduler_->Init(update_store_.get(), executer_->GetTaskRunner(), &app_,
                     &meta_);

    app_.AppInit(executer_->GetTaskRunner(), update_store_.get());
  }

  ~Planar() = default;

  void Start() {
    LOG_INFO("start components!");
    start_time_ = std::chrono::system_clock::now();
    loader2_.Start();
    executer_->Start();
    scheduler_->Start();

    scheduler_->Stop();
    Stop();
    end_time_ = std::chrono::system_clock::now();
    LOGF_INFO(" =========== whole Runtime: {} s ===========",
              std::chrono::duration<double>(end_time_ - start_time_).count());
  }

  void Stop() {
    loader2_.StopAndJoin();
    executer_->StopAndJoin();
  }

 private:
  data_structures::TwoDMetadata meta_;

  std::unique_ptr<scheduler::Scheduler> scheduler_;
  std::unique_ptr<update_stores::BspUpdateStore<VertexData, EdgeData>>
      update_store_;

  AppType app_;

  scheduler::EdgeBuffer2 edge_buffer;
  common::BlockingQueue<common::BlockID> queue_;

  components::LoaderOp2 loader2_;

  std::unique_ptr<components::Loader<io::MutableCSRReader>> loader_;
  std::unique_ptr<components::Executor> executer_;

  std::vector<data_structures::graph::MutableBlockCSRGraph> graphs_;

  // time
  std::chrono::time_point<std::chrono::system_clock> start_time_;
  std::chrono::time_point<std::chrono::system_clock> end_time_;
};

}  // namespace sics::graph::core::planar_system
