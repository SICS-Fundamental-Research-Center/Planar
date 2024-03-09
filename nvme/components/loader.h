#ifndef GRAPH_SYSTEMS_NVME_COMPONENTS_LOADER_H_
#define GRAPH_SYSTEMS_NVME_COMPONENTS_LOADER_H_

#include <memory>
#include <thread>

#include "core/util/logging.h"
#include "nvme/common/multithreading/occupied_task_runner.h"
#include "nvme/components/component.h"
#include "nvme/data_structures/graph/pram_block.h"
#include "nvme/io/reader_writer.h"
#include "nvme/scheduler/message_hub.h"

namespace sics::graph::nvme::components {

using BlockCSRGraphUInt32 = data_structures::graph::BlockCSRGraphUInt32;
using BlockCSRGraphUInt16 = data_structures::graph::BlockCSRGraphUInt16;

// An adapter class that adapts a ReaderInterface to Loader that works
// against a MessageHub.
template <typename ReaderType>
class Loader : public Component {
 protected:
  static_assert(std::is_base_of<io::Reader, ReaderType>::value,
                "ReaderType must be a subclass of Reader");

 public:
  explicit Loader(const std::string& root_path, scheduler::MessageHub* hub)
      : reader_(root_path),
        reader_q_(hub->get_reader_queue()),
        response_q_(hub->get_response_queue()) {}

  ~Loader() final = default;

  void Start() override {
    thread_ = std::make_unique<std::thread>([this]() {
      LOG_INFO("*** Loader starts ***");
      while (true) {
        scheduler::ReadMessage message = reader_q_->PopOrWait();
        if (message.terminated) {
          LOGF_INFO("Read size all: {}", reader_.SizeOfReadNow());
          // LOG_INFO("*** Loader is signaled termination ***");
          break;
        }

        // LOGF_INFO("Loader starts reading block {}", message.graph_id);
        // First read from disk.
        std::unique_ptr<data_structures::graph::SerializedPramBlockCSRGraph>
            serialized = std::make_unique<
                data_structures::graph::SerializedPramBlockCSRGraph>();
        message.serialized = serialized.get();
        reader_.Read(&message);
        // Then deserialize.
        message.graph->Deserialize(
            occupied_pool_, std::unique_ptr<core::data_structures::Serialized>(
                                serialized.release()));
        LOGF_INFO("Loader completes reading block {}", message.graph_id);
        response_q_->Push(scheduler::Message(message));
      }
    });
  }

  void StopAndJoin() override {
    scheduler::ReadMessage message;
    message.terminated = true;
    reader_q_->Push(message);
    thread_->join();
    LOG_INFO("*** Loader stops ***");
  }

  ReaderType* GetReader() { return &reader_; }

 private:
  ReaderType reader_;
  common::OccupiedPool occupied_pool_;
  scheduler::ReaderQueue* reader_q_;
  scheduler::ResponseQueue* response_q_;

  std::unique_ptr<std::thread> thread_;
};

}  // namespace sics::graph::nvme::components

#endif  // GRAPH_SYSTEMS_NVME_COMPONENTS_LOADER_H_
