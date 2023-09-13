#ifndef GRAPH_SYSTEMS_DISCHARGER_H
#define GRAPH_SYSTEMS_DISCHARGER_H

#include <memory>
#include <thread>
#include <type_traits>

#include "components/component.h"
#include "io/reader_writer.h"
#include "scheduler/message_hub.h"
#include "util/logging.h"

namespace sics::graph::core::components {

// An adapter class that adapts a WriterInterface to Discharger that works
// against a MessageHub.
template <typename WriterType>
class Discharger : public Component {
 protected:
  static_assert(std::is_base_of<io::Writer, WriterType>::value,
                "WriterType must be a subclass of Writer");

 public:
  Discharger(const std::string& root_path, scheduler::MessageHub* hub)
      : writer_(root_path),
        writer_q_(hub->get_writer_queue()),
        response_q_(hub->get_response_queue()) {}

  ~Discharger() final = default;

  void Start() override {
    thread_ = std::make_unique<std::thread>([this]() {
      while (true) {
        scheduler::WriteMessage message = writer_q_->PopOrWait();
        if (message.terminated) {
          LOG_INFO("*** Discharger is signaled termination ***");
          break;
        }

        LOGF_INFO("Discharger starts writing subgraph {}", message.graph_id);
        writer_.Write(&message);
        LOGF_INFO("Discharger completes writing subgraph {}", message.graph_id);
        response_q_->Push(scheduler::Message(message));
      }
    });
  }

  void StopAndJoin() override {
    scheduler::WriteMessage message;
    message.terminated = true;
    writer_q_->Push(message);
    thread_->join();
  }

 private:
  WriterType writer_;

  scheduler::WriterQueue* writer_q_;
  scheduler::ResponseQueue* response_q_;

  std::unique_ptr<std::thread> thread_;
};

}  // namespace sics::graph::core::components

#endif  // GRAPH_SYSTEMS_DISCHARGER_H
