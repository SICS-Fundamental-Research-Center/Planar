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
    reader_.Init(root_path, metadata);
    reader_q_ = hub->get_reader_queue();
    response_q_ = hub->get_response_queue();
    buffer_ = buffer;
    graphs_ = graphs;
  }

  void Start() {
    thread_ = std::make_unique<std::thread>([this]() {
      while (true) {
        scheduler::ReadMessage message = reader_q_->PopOrWait();
        for (int i = 0; i < to_read_blocks_id_->size(); i++) {
          // Decide number of edge blocks to read, at least DEPTH.
          std::vector<common::BlockID> reads_;
          for (int j = 0; j < QD; j++) {
            // Block when buffer is not available.
            buffer_->ApplyBuffer(current_gid_, to_read_blocks_id_->at(i));
          }
          // Send read message to reader.
          reader_.Read(current_gid_, reads_);
          // Check loaded blocks and give back to scheduler(app).
          reader_.GetBlockReady();
          scheduler::ReadMessage response;
          response.num_edge_blocks = 0;
          response_q_->Push(scheduler::Message(response));
        }

        while (true) {
        }

        if (message.terminated) {
          LOGF_INFO("Read size all: {}", reader_.SizeOfReadNow());
          LOG_INFO("*** Loader is signaled termination ***");
          break;
        }

        LOGF_INFO("Loader starts reading subgraph {}", message.graph_id);
        // Get batch read edge blocks.

        //        reader_.Read();
        LOGF_INFO("Loader completes reading subgraph {}", message.graph_id);
        response_q_->Push(scheduler::Message(message));
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

  scheduler::ReaderQueue* reader_q_;
  scheduler::ResponseQueue* response_q_;

  std::unique_ptr<std::thread> thread_;

  scheduler::EdgeBuffer* buffer_;
  std::vector<data_structures::graph::MutableBlockCSRGraph>* graphs_;
};

}  // namespace sics::graph::core::components

#endif  // GRAPH_SYSTEMS_CORE_COMPONENTS_LOADER_OP_H_
