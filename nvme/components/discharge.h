#ifndef GRAPH_SYSTEMS_NVME_COMPONENTS_DISCHARGE_H_
#define GRAPH_SYSTEMS_NVME_COMPONENTS_DISCHARGE_H_

#include <memory>
#include <thread>
#include <type_traits>

#include "core/components/component.h"
#include "core/io/reader_writer.h"
#include "core/scheduler/message_hub.h"
#include "core/util/logging.h"
#include "nvme/components/component.h"

namespace sics::graph::nvme::components {

// An adapter class that adapts a WriterInterface to Discharger that works
// against a MessageHub.
template <typename WriterType>
class Discharger : public Component {
 protected:
  static_assert(std::is_base_of<core::io::Writer, WriterType>::value,
                "WriterType must be a subclass of Writer");

 public:
  Discharger(const std::string& root_path, core::scheduler::MessageHub* hub)
      : writer_(root_path),
        writer_q_(hub->get_writer_queue()),
        response_q_(hub->get_response_queue()) {}

  ~Discharger() final = default;

  void Start() override {
    thread_ = std::make_unique<std::thread>([this]() {
      while (true) {
        core::scheduler::WriteMessage message = writer_q_->PopOrWait();
        if (message.terminated) {
          LOG_INFO("*** Discharger is signaled termination ***");
          break;
        }

        LOGF_INFO("Discharger starts writing subgraph {}", message.graph_id);
        writer_.Write(&message);
        LOGF_INFO("Discharger completes writing subgraph {}", message.graph_id);
        response_q_->Push(core::scheduler::Message(message));
      }
    });
  }

  void StopAndJoin() override {
    core::scheduler::WriteMessage message;
    message.terminated = true;
    writer_q_->Push(message);
    thread_->join();
  }

 private:
  WriterType writer_;

  core::scheduler::WriterQueue* writer_q_;
  core::scheduler::ResponseQueue* response_q_;

  std::unique_ptr<std::thread> thread_;
};

}  // namespace sics::graph::nvme::components

#endif  // GRAPH_SYSTEMS_NVME_COMPONENTS_DISCHARGE_H_
