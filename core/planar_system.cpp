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
  Planar() {
    scheduler_ = std::make_unique<scheduler::Scheduler>();

    update_store_ =
        std::make_unique<update_stores::BspUpdateStore<VertexData, EdgeData>>();

    app_ = std::make_unique<AppType>();

    // components for reader, writer and executor
    loader_ = std::make_unique<components::Loader<io::MutableCSRReader>>(
        scheduler_->GetMessageHub());
    discharger_ =
        std::make_unique<components::Discharger<io::MutableCSRWriter>>(
            scheduler_->GetMessageHub());
    executer_ =
        std::make_unique<components::Executor>(scheduler_->GetMessageHub());
  };

  ~Planar() = default;

  void Start() {}

  void Stop() {}

 private:
  // TODO: dose this need unique_ptr
  std::unique_ptr<scheduler::Scheduler> scheduler_;
  std::unique_ptr<update_stores::BspUpdateStore<VertexData, EdgeData>>
      update_store_;

  std::unique_ptr<AppType> app_;

  std::unique_ptr<components::Loader<io::MutableCSRReader>> loader_;
  std::unique_ptr<components::Discharger<io::MutableCSRWriter>> discharger_;
  std::unique_ptr<components::Executor> executer_;
};

}  // namespace sics::graph::core::planar_system
