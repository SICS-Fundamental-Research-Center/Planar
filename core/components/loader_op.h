#ifndef GRAPH_SYSTEMS_CORE_COMPONENTS_LOADER_OP_H_
#define GRAPH_SYSTEMS_CORE_COMPONENTS_LOADER_OP_H_

#include <memory>
#include <thread>
#include <type_traits>
#include <utility>

#include "components/component.h"
#include "data_structures/graph/mutable_block_csr_graph.h"
#include "io/csr_edge_block_reader.h"
#include "io/mutable_csr_reader.h"
#include "io/reader_writer.h"
#include "scheduler/edge_buffer.h"
#include "scheduler/message_hub.h"
#include "util/logging.h"

namespace sics::graph::core::components {

// An adapter class that adapts a ReaderInterface to Loader that works
// against a MessageHub.
class LoaderOp {
 public:
  LoaderOp() = default;
  LoaderOp(const std::string& root_path, scheduler::MessageHub* hub)
      : reader_q_(hub->get_reader_queue()),
        response_q_(hub->get_response_queue()) {}

  ~LoaderOp() = default;

  void Init(const std::string& root_path, scheduler::MessageHub* hub,
            data_structures::TwoDMetadata* metadata,
            scheduler::EdgeBuffer* buffer,
            std::vector<data_structures::graph::MutableBlockCSRGraph>* graphs) {
    reader_.Init(root_path, metadata, buffer, graphs);
    reader_q_ = hub->get_reader_queue();
    response_q_ = hub->get_response_queue();
    buffer_ = buffer;
    graphs_ = graphs;
  }

  int SubmitReadRequest(int begin) {
    if (begin >= to_read_blocks_id_->size()) return begin;
    std::vector<common::BlockID> reqs;
    while (true) {
      if (queue_ >= QD || begin >= to_read_blocks_id_->size()) break;
      auto bid = to_read_blocks_id_->at(begin);
      if (buffer_->IsBufferNotEnough(current_gid_, bid)) {
        //        buffer_->SetBufferBlock();
        break;
      }
      buffer_->ApplyBuffer(current_gid_, bid);
      reqs.push_back(bid);
      begin++;
      queue_++;
    }
    reader_.Read(current_gid_, reqs);
    return begin;
  }

  size_t CheckIOEntry() {
    auto loaded = reader_.GetBlockReady();
    receive_ += loaded;
    queue_ -= loaded;
    return loaded;
  }

  void Start() {
    queue_ = 0;
    receive_ = 0;
    send_ = 0;
    thread_ = std::make_unique<std::thread>([this]() {
      while (true) {
        scheduler::ReadMessage message = reader_q_->PopOrWait();
        if (message.terminated) break;
        int begin = 0;
        while (receive_ < to_read_blocks_id_->size()) {
          // Send QD requests per Read operation.
          begin = SubmitReadRequest(begin);
          auto load = CheckIOEntry();
          if (load) {
            // Check loaded blocks and give back to scheduler(app).
            scheduler::ReadMessage response;
            response.num_edge_blocks = load;
            response_q_->Push(scheduler::Message(response));
          }
          // TODO: Judge if to block for buffer release.
        }
        // One subgraph finished, terminate the executor for next one.
        scheduler::ReadMessage m_end;
        m_end.terminated = true;
        response_q_->Push(scheduler::Message(m_end));

        receive_ = 0;
        queue_ = 0;
        send_;
      }
    });
  }

  void StopAndJoin() {
    scheduler::ReadMessage message;
    message.terminated = true;
    reader_q_->Push(message);
    thread_->join();
  }

  io::CSREdgeBlockReader* GetReader() { return &reader_; }

  std::vector<common::BlockID> GetReadBlocks() {
    return reader_.GetReadBlocks();
  }

  // Set to_read_blocks_id for current subgraph.
  void SetToReadBlocksID(common::GraphID gid,
                         std::vector<common::VertexID>* to_read_blocks_id) {
    current_gid_ = gid;
    to_read_blocks_id_ = to_read_blocks_id;
    scheduler::ReadMessage message;
    message.graph_id = gid;
    reader_q_->Push(message);
  }

  // Get the edge block address.
  common::VertexID* GetBlockAddr(common::GraphID gid, common::BlockID bid) {
    return reader_.GetBlockAddr(gid, bid);
  }

  // Release the edge block address when no need.
  void ReleaseBlockAddr(common::GraphID gid, common::BlockID bid) {
    reader_.ReleaseBlockAddr(gid, bid);
  }

 private:
  io::CSREdgeBlockReader reader_;

  common::GraphID current_gid_;
  std::vector<common::VertexID>* to_read_blocks_id_;

  bool block_ = false;

  size_t queue_;
  size_t receive_;
  size_t send_;

  scheduler::ReaderQueue* reader_q_;
  scheduler::ResponseQueue* response_q_;

  std::unique_ptr<std::thread> thread_;

  scheduler::EdgeBuffer* buffer_;
  std::vector<data_structures::graph::MutableBlockCSRGraph>* graphs_;
};

}  // namespace sics::graph::core::components

#endif  // GRAPH_SYSTEMS_CORE_COMPONENTS_LOADER_OP_H_
