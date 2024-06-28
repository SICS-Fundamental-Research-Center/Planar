#ifndef GRAPH_SYSTEMS_LOADER_OP2_H
#define GRAPH_SYSTEMS_LOADER_OP2_H

#include <memory>
#include <thread>
#include <type_traits>
#include <utility>

#include "common/types.h"
#include "components/component.h"
#include "data_structures/graph/mutable_block_csr_graph.h"
#include "io/csr_edge_block_reader2.h"
#include "io/mutable_csr_reader.h"
#include "io/reader_writer.h"
#include "scheduler/edge_buffer2.h"
#include "scheduler/graph_state.h"
#include "scheduler/message_hub.h"
#include "util/logging.h"

namespace sics::graph::core::components {

// An adapter class that adapts a ReaderInterface to Loader that works
// against a MessageHub.
class LoaderOp2 {
 public:
  LoaderOp2() = default;
  LoaderOp2(const std::string& root_path, scheduler::MessageHub* hub)
      : reader_q_(hub->get_reader_queue()),
        response_q_(hub->get_response_queue()) {}

  ~LoaderOp2() = default;

  void Init(const std::string& root_path, scheduler::MessageHub* hub,
            data_structures::TwoDMetadata* metadata,
            scheduler::EdgeBuffer2* buffer,
            std::vector<data_structures::graph::MutableBlockCSRGraph>* graphs) {
    meta_ = metadata;
    reader_.Init(root_path, metadata, buffer, graphs);
    reader_q_ = hub->get_reader_queue();
    response_q_ = hub->get_response_queue();
    buffer_ = buffer;
    graphs_ = graphs;
  }

  int SubmitReadRequest(int begin) {
    if (begin >= to_read_blocks_id_.size()) return begin;
    std::vector<common::BlockID> reqs;
    while (true) {
      if (queue_ >= QD || begin >= to_read_blocks_id_.size()) break;
      auto bid = to_read_blocks_id_.at(begin);
      if (buffer_->IsBufferNotEnough(current_gid_, bid)) {
        //        buffer_->SetBufferBlock();
        LOG_INFO("Buffer space is not enough, waiting for space release!");
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
        GetReadSubBlocksIds(message.graph_id);
        current_gid_ = message.graph_id;
        int begin = 0;
        while (receive_ < to_read_blocks_id_.size()) {
          // Send QD requests per Read operation.
          begin = SubmitReadRequest(begin);
          auto load = CheckIOEntry();
          // TODO: Judge if to block for buffer release.
        }
        buffer_->Push(4294967295);
        // One subgraph finished, terminate the executor for next one.
        //        scheduler::ReadMessage read_finish;
        //        read_finish.graph_id = current_gid_;
        //        response_q_->Push(scheduler::Message(read_finish));
        if (common::Configurations::Get()->mode == common::Static) {
          state_->SetEdgeLoaded(current_gid_);
        } else {
          graphs_->at(current_gid_).SetEdgeLoaded(true);
        }

        LOGF_INFO("Loading subgraph {} finish", current_gid_);

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

  io::CSREdgeBlockReader2* GetReader() { return &reader_; }

  std::vector<common::BlockID> GetReadBlocks() {
    return reader_.GetReadBlocks();
  }

  // Set to_read_blocks_id for current subgraph.
  //  void SetToReadBlocksID(common::GraphID gid,
  //                         std::vector<common::VertexID>* to_read_blocks_id) {
  //    current_gid_ = gid;
  //    to_read_blocks_id_ = to_read_blocks_id;
  //    scheduler::ReadMessage message;
  //    message.graph_id = gid;
  //    reader_q_->Push(message);
  //  }

  // Get the edge block address.
  common::VertexID* GetBlockAddr(common::GraphID gid, common::BlockID bid) {
    return reader_.GetBlockAddr(gid, bid);
  }

  // Release the edge block address when no need.
  void ReleaseBlockAddr(common::GraphID gid, common::BlockID bid) {
    reader_.ReleaseBlockAddr(gid, bid);
  }

  void GetReadSubBlocksIds(common::GraphID gid) {
    to_read_blocks_id_.clear();
    if (common::Configurations::Get()->mode == common::Static) {
      auto ids = state_->GetSubBlockIDs(gid);
      for (auto id : ids) {
        to_read_blocks_id_.push_back(id);
      }
    } else {
      auto num_sub_blocks = meta_->blocks.at(gid).num_sub_blocks;
      for (common::BlockID i = 0; i < num_sub_blocks; i++) {
        to_read_blocks_id_.push_back(i);
      }
    }
  }

  void SetStatePtr(scheduler::GraphState* state) { state_ = state; }

 private:
  io::CSREdgeBlockReader2 reader_;

  data_structures::TwoDMetadata* meta_;

  common::GraphID current_gid_;
  std::vector<common::VertexID> to_read_blocks_id_;

  bool block_ = false;

  size_t queue_;
  size_t receive_;
  size_t send_;

  scheduler::ReaderQueue* reader_q_;
  scheduler::ResponseQueue* response_q_;

  std::unique_ptr<std::thread> thread_;

  scheduler::GraphState* state_;
  scheduler::EdgeBuffer2* buffer_;
  std::vector<data_structures::graph::MutableBlockCSRGraph>* graphs_;
};

}  // namespace sics::graph::core::components

#endif  // GRAPH_SYSTEMS_LOADER_OP2_H
