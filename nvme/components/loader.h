#ifndef GRAPH_SYSTEMS_NVME_COMPONENTS_LOADER_H_
#define GRAPH_SYSTEMS_NVME_COMPONENTS_LOADER_H_

#include <memory>
#include <thread>

#include "core/util/logging.h"
#include "nvme/common/multithreading/occupied_task_runner.h"
#include "nvme/components/component.h"
#include "nvme/data_structures/graph/block_csr_graph.h"
#include "nvme/data_structures/memory_buffer.h"
#include "nvme/io/io_uring_reader.h"
#include "nvme/io/reader_writer.h"
#include "nvme/scheduler/message_hub.h"

namespace sics::graph::nvme::components {

// An adapter class that adapts a ReaderInterface to Loader that works
// against a MessageHub.
class Loader : public Component {
  using BlockID = core::common::BlockID;

 public:
  Loader() = default;
  explicit Loader(const std::string& root_path, scheduler::MessageHub* hub,
                  data_structures::EdgeBuffer* buffer)
      : reader_q_(hub->get_reader_queue()),
        response_q_(hub->get_response_queue()) {
    reader_.Init(root_path, buffer);
    buffer_ = buffer;
  }

  void Init(const std::string& root_path, scheduler::MessageHub* hub,
            data_structures::EdgeBuffer* buffer) {
    reader_q_ = hub->get_reader_queue();
    response_q_ = hub->get_response_queue();
    reader_.Init(root_path, buffer);
    buffer_ = buffer;
  }

  ~Loader() final = default;

  int SubmitReadRequest(int begin) {
    if (begin >= to_read_blocks_id_.size()) return begin;
    std::vector<BlockID> reqs;
    while (true) {
      if (queue_ >= QD || begin >= to_read_blocks_id_.size()) break;
      auto bid = to_read_blocks_id_.at(begin);
      if (buffer_->IsBufferNotEnough(bid)) {
        //        buffer_->SetBufferBlock();
        LOG_INFO("Buffer space is not enough, waiting for space release!");
        break;
      }
      buffer_->ApplyBuffer( bid);
      reqs.push_back(bid);
      begin++;
      queue_++;
    }
    reader_.Read(reqs);
    return begin;
  }

  size_t CheckIOEntry() {
    auto loaded = reader_.GetBlockReady();
    receive_ += loaded;
    return loaded;
  }

  void Start() override {
    queue_ = 0;
    receive_ = 0;
    send_ = 0;
    thread_ = std::make_unique<std::thread>([this]() {
      LOG_INFO("*** Capacity Loader starts ***");
      while (true) {
        auto running = buffer_->CheckIfWaitForLoading();
        if (!running) break;
        to_read_blocks_id_ = buffer_->GetBlocksToRead();
        int begin = 0;
        while (receive_ < to_read_blocks_id_.size()) {
          begin = SubmitReadRequest(begin);
          auto load = CheckIOEntry();
          LOGF_INFO("Load {} subBlocks finish!", load);
          buffer_->SendToExecutor();
        }
        receive_ = 0;
        queue_ = 0;
        send_ = 0;
      }
    });
  }

  void StopAndJoin() override {
    buffer_->StopLoader();
    thread_->join();
    LOG_INFO("*** Loader stops ***");
  }

  io::CSREdgeBlockReader* GetReader() { return &reader_; }

 private:
  io::CSREdgeBlockReader reader_;
  common::OccupiedPool occupied_pool_;
  scheduler::ReaderQueue* reader_q_;
  scheduler::ResponseQueue* response_q_;

  std::unique_ptr<std::thread> thread_;

  std::vector<BlockID> to_read_blocks_id_;
  size_t queue_;
  size_t receive_;
  size_t send_;
  data_structures::EdgeBuffer* buffer_;

};

}  // namespace sics::graph::nvme::components

#endif  // GRAPH_SYSTEMS_NVME_COMPONENTS_LOADER_H_
