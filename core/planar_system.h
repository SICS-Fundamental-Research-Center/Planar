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
  // TODO: add init info
  Planar(const std::string& root_path, AppType&& app)
      : scheduler_(std::make_unique<
                   update_stores::BspUpdateStore<VertexData, EdgeData>>(
            root_path)),
        app_(app) {
    //    app_ = std::make_unique<AppType>();

    update_store_ =
        std::make_unique<update_stores::BspUpdateStore<VertexData, EdgeData>>(
            scheduler_->GetVertexNumber());

    // components for reader, writer and executor
    loader_ = std::make_unique<components::Loader<io::MutableCSRReader>>(
        scheduler_->GetMessageHub());
    discharger_ =
        std::make_unique<components::Discharger<io::MutableCSRWriter>>(
            scheduler_->GetMessageHub());
    executer_ =
        std::make_unique<components::Executor>(scheduler_->GetMessageHub());

    // set scheduler info
    scheduler_->Init(update_store_.get(), executer_->get_task_runner(),
                     app_->get());
  };

  ~Planar() = default;

  void Start() {}

  void Stop() {}

 private:
  // TODO: dose this need unique_ptr
  data_structures::GraphMetadata graph_metadata_;

  std::unique_ptr<scheduler::Scheduler> scheduler_;
  std::unique_ptr<update_stores::BspUpdateStore<VertexData, EdgeData>>
      update_store_;

  AppType* app_;

  std::unique_ptr<components::Loader<io::MutableCSRReader>> loader_;
  std::unique_ptr<components::Discharger<io::MutableCSRWriter>> discharger_;
  std::unique_ptr<components::Executor> executer_;
};

}  // namespace sics::graph::core::planar_system
