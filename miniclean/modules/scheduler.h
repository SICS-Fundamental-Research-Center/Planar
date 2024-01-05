#ifndef MINICLEAN_MODULES_SCHEDULER_H_
#define MINICLEAN_MODULES_SCHEDULER_H_

#include <memory>
#include <thread>

#include "core/util/logging.h"
#include "miniclean/common/miniclean_config.h"
#include "miniclean/common/types.h"
#include "miniclean/messages/message.h"

namespace sics::graph::miniclean::modules {

class Scheduler {
 private:
  using MiniCleanConfig = sics::graph::miniclean::common::MiniCleanConfig;
  using Message = sics::graph::miniclean::messages::Message;
  using ReadMessage = sics::graph::miniclean::messages::ReadMessage;
  using ExecuteMessage = sics::graph::miniclean::messages::ExecuteMessage;
  using WriteMessage = sics::graph::miniclean::messages::WriteMessage;
  using GraphID = sics::graph::miniclean::common::GraphID;

 public:
  Scheduler();

  void Start();
  GraphID GetNextReadGraphInCurrentRound();

 private:
  bool ReadMessageResponseAndExecute(const ReadMessage& message);
  bool ExecuteMessageResponseAndWrite(const ExecuteMessage& message);
  bool WriteMessageResponseAndCheckTerminate(const WriteMessage& message);

  size_t available_memory_size_;
  std::unique_ptr<std::thread> thread_;
};
}  // namespace sics::graph::miniclean::modules

#endif  // MINICLEAN_MODULES_SCHEDULER_H_