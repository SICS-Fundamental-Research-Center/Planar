#ifndef GRAPH_SYSTEMS_LOADER_H
#define GRAPH_SYSTEMS_LOADER_H

#include "common/component.h"
#include "io/reader_writer.h"
#include "scheduler/message_hub.h"
#include "util/logging.h"
#include <memory>
#include <thread>
#include <type_traits>

namespace sics::graph::core::io {

// An adapter class that adapts a ReaderInterface to Loader that works
// against a MessageHub.
template<typename ReaderType>
class Loader : public common::Component {
 protected:
  static_assert(std::is_base_of<Reader, ReaderType>::value,
                "ReaderType must be a subclass of Reader");

 public:
  Loader(scheduler::MessageHub* hub)
      : reader_q_(hub->get_reader_queue()),
        response_q_(hub->get_response_queue()) {}

  ~Loader() final = default;

  void Start() override {
    thread_ = std::make_unique<std::thread>([this]() {
      while (true) {
        scheduler::ReadMessage message = reader_q_->PopOrWait();
        if (message.terminated) {
          LOG_INFO("*** Loader is signaled termination ***");
          break;
        }

        LOGF_INFO("Loader starts reading subgraph {}", message.graph_id);
        reader_->Read(&message);
        LOGF_INFO("Loader completes reading subgraph {}", message.graph_id);
        response_q_->Push(scheduler::Message(message));
      }
    });
  }

  void StopAndJoin() override {
    scheduler::ReadMessage message;
    message.terminated = true;
    reader_q_->Push(message);
    thread_->join();
  }

 private:
  ReaderType reader_;

  scheduler::ReaderQueue* reader_q_;
  scheduler::ResponseQueue* response_q_;

  std::unique_ptr<std::thread> thread_;
};

}

#endif  // GRAPH_SYSTEMS_LOADER_H
