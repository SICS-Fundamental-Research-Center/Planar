#ifndef MINICLEAN_MODULES_LOADER_H_
#define MINICLEAN_MODULES_LOADER_H_

#include <memory>
#include <thread>

#include "core/util/logging.h"
#include "miniclean/io/miniclean_graph_reader.h"
#include "miniclean/messages/message.h"
#include "miniclean/messages/message_hub.h"

namespace sics::graph::miniclean::modules {

class Loader {
 private:
  using Reader = sics::graph::miniclean::io::MiniCleanGraphReader;
  using ReadMessage = sics::graph::miniclean::messages::ReadMessage;
  using Message = sics::graph::miniclean::messages::Message;
  using MessageHub = sics::graph::miniclean::messages::MessageHub;
  using ReaderQueue = sics::graph::miniclean::messages::ReaderQueue;
  using ResponseQueue = sics::graph::miniclean::messages::ResponseQueue;

 public:
  Loader(const std::string& root_path, MessageHub* hub)
      : reader_(root_path),
        reader_q_(hub->get_reader_queue()),
        response_q_(hub->get_response_queue()) {}

  ~Loader() = default;

  void Start() {
    thread_ = std::make_unique<std::thread>([this]() {
      while (true) {
        ReadMessage message = reader_q_->PopOrWait();
        if (message.terminated) {
          LOGF_INFO("Read size all: {}", reader_.SizeOfReadNow());
          LOG_INFO("*** Loader is signaled termination ***");
          break;
        }

        LOGF_INFO("Loader starts reading subgraph {}", message.graph_id);
        reader_.Read(&message);
        LOGF_INFO("Loader completes reading subgraph {}", message.graph_id);
        response_q_->Push(Message(message));
      }
    });
  }

  void StopAndJoin() {
    ReadMessage message;
    message.terminated = true;
    reader_q_->Push(message);
    thread_->join();
  }

  Reader* GetReader() { return &reader_; }

 private:
  Reader reader_;

  ReaderQueue* reader_q_;
  ResponseQueue* response_q_;

  std::unique_ptr<std::thread> thread_;
};

}  // namespace sics::graph::miniclean::modules

#endif  // GMINICLEAN_MODULES_LOADER_H_
