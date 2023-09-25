#include <chrono>
#include <iostream>
#include <memory>

#include "apis/planar_app_base.h"
#include "common/types.h"
#include "components/discharger.h"
#include "components/executor.h"
#include "components/loader.h"
#include "io/mutable_csr_reader.h"
#include "io/mutable_csr_writer.h"
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
      : scheduler_(std::make_unique<scheduler::Scheduler>(root_path)) {
    update_store_ =
        std::make_unique<update_stores::BspUpdateStore<VertexData, EdgeData>>(
            root_path, scheduler_->GetVertexNumber());

    // components for reader, writer and executor
    loader_ = std::make_unique<components::Loader<io::MutableCSRReader>>(
        root_path, scheduler_->GetMessageHub());
    discharger_ =
        std::make_unique<components::Discharger<io::MutableCSRWriter>>(
            root_path, scheduler_->GetMessageHub());
    executer_ =
        std::make_unique<components::Executor>(scheduler_->GetMessageHub());

    // set scheduler info
    scheduler_->Init(update_store_.get(), executer_->GetTaskRunner(), &app_);

    app_.AppInit(executer_->GetTaskRunner(), update_store_.get());
  }

  ~Planar() = default;

  void Start() {
    LOG_INFO("start components!");
    start_time_ = std::chrono::system_clock::now();
    loader_->Start();
    discharger_->Start();
    executer_->Start();
    scheduler_->Start();

    scheduler_->Stop();
    Stop();
    end_time_ = std::chrono::system_clock::now();
    LOGF_INFO(" =========== Hole Runtime: {} s ===========",
             std::chrono::duration<double>(end_time_ - start_time_).count());
  }

  void Stop() {
    loader_->StopAndJoin();
    discharger_->StopAndJoin();
    executer_->StopAndJoin();
  }

 private:
  std::unique_ptr<scheduler::Scheduler> scheduler_;
  std::unique_ptr<update_stores::BspUpdateStore<VertexData, EdgeData>>
      update_store_;

  AppType app_;

  std::unique_ptr<components::Loader<io::MutableCSRReader>> loader_;
  std::unique_ptr<components::Discharger<io::MutableCSRWriter>> discharger_;
  std::unique_ptr<components::Executor> executer_;

  // time
  std::chrono::time_point<std::chrono::system_clock> start_time_;
  std::chrono::time_point<std::chrono::system_clock> end_time_;
};

}  // namespace sics::graph::core::planar_system
