#ifndef GRAPH_SYSTEMS_CORE_IO_PRAM_BLOCK_READER_H_
#define GRAPH_SYSTEMS_CORE_IO_PRAM_BLOCK_READER_H_

#include <filesystem>
#include <fstream>
#include <list>
#include <memory>
#include <string>
#include <utility>

#include "core/common/config.h"
#include "core/data_structures/buffer.h"
#include "core/data_structures/graph_metadata.h"
#include "core/data_structures/serialized.h"
#include "nvme/data_structures/graph/serialized_pram_block_csr.h"
#include "nvme/io/reader_writer.h"

namespace sics::graph::nvme::io {

// @DESCRIPTION Class to read data from ssd to memory
// @EXAMPLE
//  ReadMessage read_message;
//  auto serialized_csr = std::make_unique<SerializedMutableCSRGraph>();
//  read_message.response_serialized = serialized_csr.get();
//  read_message.graph_id = 0;
//  csr_reader.Read(&read_message, nullptr);
class PramBlockReader : public Reader {
 private:
  using OwnedBuffer = sics::graph::core::data_structures::OwnedBuffer;
  using Serialized = sics::graph::core::data_structures::Serialized;

 public:
  explicit PramBlockReader(const std::string& root_path)
      : root_path_(root_path) {}

  void Read(ReadMessage* message,
            core::common::TaskRunner* runner = nullptr) override;

  size_t SizeOfReadNow() override { return read_size_; }

 private:
  void ReadBlockInfo(const std::string& path,
                     core::common::VertexCount num_vertices,
                     std::vector<OwnedBuffer>* buffers);

  void ReadNeighborInfo(const std::string& path,
                        std::vector<OwnedBuffer>* buffers);

 private:
  const std::string root_path_;
  size_t read_size_ = 0;  // use MB
};

}  // namespace sics::graph::nvme::io

#endif  // GRAPH_SYSTEMS_CORE_IO_PRAM_BLOCK_READER_H_
