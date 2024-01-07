#ifndef GRAPH_SYSTEMS_NVME_COMPONENTS_LOADER_H_
#define GRAPH_SYSTEMS_NVME_COMPONENTS_LOADER_H_

#include <memory>
#include <thread>

#include "core/io/reader_writer.h"
#include "core/scheduler/message_hub.h"
#include "nvme/components/component.h"

namespace sics::graph::nvme::components {

// An adapter class that adapts a ReaderInterface to Loader that works
// against a MessageHub.
template <typename ReaderType>
class Loader : public Component {
 protected:
  static_assert(std::is_base_of<core::io::Reader, ReaderType>::value,
                "ReaderType must be a subclass of Reader");

 public:
  Loader(const std::string& root_path, core::scheduler::MessageHub* hub)
      : reader_(root_path),
        reader_q_(hub->get_reader_queue()),
        response_q_(hub->get_response_queue()) {}

  ~Loader() final = default;

  void Start() override {
    thread_ = std::make_unique<std::thread>([this]() {
      while (true) {
        core::scheduler::ReadMessage message = reader_q_->PopOrWait();
        if (message.terminated) {
          LOGF_INFO("Read size all: {}", reader_.SizeOfReadNow());
          LOG_INFO("*** Loader is signaled termination ***");
          break;
        }

        LOGF_INFO("Loader starts reading subgraph {}", message.graph_id);
        reader_.Read(&message);
        LOGF_INFO("Loader completes reading subgraph {}", message.graph_id);
        response_q_->Push(core::scheduler::Message(message));
      }
    });
  }

  void StopAndJoin() override {
    core::scheduler::ReadMessage message;
    message.terminated = true;
    reader_q_->Push(message);
    thread_->join();
  }

  ReaderType* GetReader() { return &reader_; }

 private:
  ReaderType reader_;

  core::scheduler::ReaderQueue* reader_q_;
  core::scheduler::ResponseQueue* response_q_;

  std::unique_ptr<std::thread> thread_;
};

}  // namespace sics::graph::nvme::components

#endif  // GRAPH_SYSTEMS_NVME_COMPONENTS_LOADER_H_
