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
            scheduler_->GetVertexNumber());

    // components for reader, writer and executor
    loader_ = std::make_unique<components::Loader<io::MutableCSRReader>>(
        root_path, scheduler_->GetMessageHub());
    discharger_ =
        std::make_unique<components::Discharger<io::MutableCSRWriter>>(
            root_path, scheduler_->GetMessageHub());
    executer_ =
        std::make_unique<components::Executor>(scheduler_->GetMessageHub());

    // set scheduler info
    scheduler_->Init(update_store_.get(), &app_);
  };

  ~Planar() = default;

  void Start() {}

  void Stop() {}

 private:
  std::unique_ptr<scheduler::Scheduler> scheduler_;
  std::unique_ptr<update_stores::BspUpdateStore<VertexData, EdgeData>>
      update_store_;

  AppType app_;

  std::unique_ptr<components::Loader<io::MutableCSRReader>> loader_;
  std::unique_ptr<components::Discharger<io::MutableCSRWriter>> discharger_;
  std::unique_ptr<components::Executor> executer_;
};

}  // namespace sics::graph::core::planar_system
