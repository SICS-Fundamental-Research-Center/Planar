#ifndef CORE_IO_CSR_READER_H_
#define CORE_IO_CSR_READER_H_

#include <yaml-cpp/yaml.h>

#include <filesystem>
#include <fstream>
#include <iostream>
#include <list>
#include <memory>
#include <string>
#include <utility>

#include "data_structures/buffer.h"
#include "data_structures/graph_metadata.h"
#include "data_structures/serialized.h"
#include "io/reader_writer.h"

namespace sics::graph::core::io {

// @DESCRIPTION Class to read data from ssd to memory
// @EXAMPLE
//  ReadMessage read_message;
//  auto serialized_csr = std::make_unique<SerializedImmutableCSRGraph>();
//  read_message.response_serialized = serialized_csr.get();
//  read_message.graph_id = 0;
//  csr_reader.Read(&read_message, nullptr);
class CSRReader : public Reader {
 private:
  using OwnedBuffer = sics::graph::core::data_structures::OwnedBuffer;
  using Serialized = sics::graph::core::data_structures::Serialized;

 public:
  CSRReader(const std::string& root_path) : root_path_(root_path) {}

  void Read(ReadMessage* message,
            common::TaskRunner* runner = nullptr) override;

  void ReadFromBin(const std::string& path, Serialized* serialized_graph);

 private:
  const std::string root_path_;
};

}  // namespace sics::graph::core::io

#endif  // CORE_IO_CSR_READER_H
