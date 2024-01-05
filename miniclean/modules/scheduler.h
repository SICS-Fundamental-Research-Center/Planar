#ifndef MINICLEAN_MODULES_SCHEDULER_H_
#define MINICLEAN_MODULES_SCHEDULER_H_

#include <memory>
#include <string>
#include <thread>

#include "core/util/logging.h"
#include "miniclean/common/miniclean_config.h"
#include "miniclean/common/types.h"
#include "miniclean/messages/message.h"
#include "miniclean/messages/message_hub.h"
#include "miniclean/modules/io_manager.h"

namespace sics::graph::miniclean::modules {

class Scheduler {
 private:
  using MiniCleanConfig = sics::graph::miniclean::common::MiniCleanConfig;
  using MessageHub = sics::graph::miniclean::messages::MessageHub;
  using Message = sics::graph::miniclean::messages::Message;
  using ReadMessage = sics::graph::miniclean::messages::ReadMessage;
  using ExecuteMessage = sics::graph::miniclean::messages::ExecuteMessage;
  using WriteMessage = sics::graph::miniclean::messages::WriteMessage;
  using GraphID = sics::graph::miniclean::common::GraphID;
  using IOManager = sics::graph::miniclean::modules::IOManager;

 public:
  Scheduler(const std::string& data_home, const std::string& graph_home);

  void Start();
  void Stop() { thread_->join(); }

  GraphID GetNextReadGraphInCurrentRound();

 private:
  bool ReadMessageResponseAndExecute(const ReadMessage& message);
  bool ExecuteMessageResponseAndWrite(const ExecuteMessage& message);
  bool WriteMessageResponseAndCheckTerminate(const WriteMessage& message);

  IOManager io_manager_;
  MessageHub message_hub_;

  size_t available_memory_size_;
  size_t current_round_ = 0;

  std::unique_ptr<std::thread> thread_;
};
}  // namespace sics::graph::miniclean::modules

#endif  // MINICLEAN_MODULES_SCHEDULER_H_