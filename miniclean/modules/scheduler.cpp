#include "miniclean/modules/scheduler.h"

namespace sics::graph::miniclean::modules {

using GraphID = sics::graph::miniclean::common::GraphID;

Scheduler::Scheduler() {
  // Initialize config.
  available_memory_size_ = MiniCleanConfig::Get()->memory_size_;
}

void Scheduler::Start() {
  thread_ = std::make_unique<std::thread>([this]() {
    LOG_INFO(" ========== Launched MiniClean scheduler.  ========== ");
    LOGF_INFO("Available memory size: {} MB", available_memory_size_);
    ReadMessage first_read_message;
  });
}

// TODO (bai-wenchao): Decide which graph to read next.
GraphID Scheduler::GetNextReadGraphInCurrentRound() { 
  
}
}  // namespace sics::graph::miniclean::modules