#ifndef GRAPH_SYSTEMS_CORE_IO_MUTABLE_CSR_READER_H_
#define GRAPH_SYSTEMS_CORE_IO_MUTABLE_CSR_READER_H_

#include <filesystem>
#include <fstream>
#include <list>
#include <memory>
#include <string>
#include <utility>

#include "common/blocking_queue.h"
#include "common/config.h"
#include "data_structures/buffer.h"
#include "data_structures/graph/serialized_mutable_csr_graph.h"
#include "data_structures/graph_metadata.h"
#include "data_structures/serialized.h"
#include "io/reader_writer.h"

namespace sics::graph::core::io {

// @DESCRIPTION Class to read data from ssd to memory
// @EXAMPLE
//  ReadMessage read_message;
//  auto serialized_csr = std::make_unique<SerializedMutableCSRGraph>();
//  read_message.response_serialized = serialized_csr.get();
//  read_message.graph_id = 0;
//  csr_reader.Read(&read_message, nullptr);
class MutableCSRReader : public Reader {
 private:
  using OwnedBuffer = sics::graph::core::data_structures::OwnedBuffer;
  using Serialized = sics::graph::core::data_structures::Serialized;

 public:
  MutableCSRReader(const std::string& root_path) : root_path_(root_path) {}
  void Init(const std::string& root_path) {
    // copy the root path
    root_path_ = root_path;
  }

  void Read(ReadMessage* message,
            common::TaskRunner* runner = nullptr) override;

  size_t SizeOfReadNow() override { return read_size_; }

  // TODO: use queue to replace vector?
  std::vector<common::VertexID> GetReadBlocks() {
    std::lock_guard<std::mutex> lock(mtx_);
    std::vector<common::VertexID> read_blocks = read_blocks_;
    read_blocks_.clear();
    return read_blocks;
  }

 private:
  void ReadMetaInfoFromBin(const std::string& path,
                           common::VertexCount num_vertices,
                           std::vector<OwnedBuffer>* buffers);

  void ReadSingleBufferFromBin(const std::string& path,
                               std::vector<OwnedBuffer>* buffers);

 private:
  std::string root_path_;
  size_t read_size_ = 0;  // use MB

  std::vector<common::VertexID> read_blocks_;
  std::mutex mtx_;
};

}  // namespace sics::graph::core::io

#endif  // GRAPH_SYSTEMS_CORE_IO_MUTABLE_CSR_READER_H_
