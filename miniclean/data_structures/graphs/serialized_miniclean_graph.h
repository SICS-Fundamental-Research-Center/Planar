#ifndef MINICLEAN_DATA_STRUCTURES_GRAPHS_SERIALIZED_MINICLEAN_GRAPH_H_
#define MINICLEAN_DATA_STRUCTURES_GRAPHS_SERIALIZED_MINICLEAN_GRAPH_H_

#include <list>
#include <vector>

#include "core/data_structures/buffer.h"
#include "core/data_structures/serialized.h"

class SerializedMiniCleanGraph
    : public sics::graph::core::data_structures::Serialized {
 private:
  using OwnedBuffer = sics::graph::core::data_structures::OwnedBuffer;

 public:
  SerializedMiniCleanGraph() = default;

  bool HasNext() const override { return miniclean_graph_buffers_.size() > 0; }

  // Reader call this function to push buffers into miniclean_graph_buffers_.
  void ReceiveBuffers(std::vector<OwnedBuffer>&& buffers) override {
    miniclean_graph_buffers_.emplace_back(std::move(buffers));
  };

  const std::list<std::vector<OwnedBuffer>>& GetMiniCleanGraphBuffers() {
    return miniclean_graph_buffers_;
  }

 protected:
  // Writer call this function to write the first buffer back to disk.
  std::vector<OwnedBuffer> PopNextImpl() override {
    std::vector<OwnedBuffer> buffers =
        std::move(miniclean_graph_buffers_.front());
    miniclean_graph_buffers_.pop_front();
    return buffers;
  }

  // `miniclean_graph_buffers_` stores the serialized graph data for miniclean
  // application. Each item of `miniclean_graph_buffers_` corresponds to vector
  // of buffers for different subgraph data files:
  //   - item 0: CSR format graph data
  //   - item 1: Bitmap (is-in-graph)
  //   - other items: vertex attributes
  std::list<std::vector<OwnedBuffer>> miniclean_graph_buffers_;
};

#endif  // MINICLEAN_DATA_STRUCTURES_GRAPHS_SERIALIZED_MINICLEAN_GRAPH_H_