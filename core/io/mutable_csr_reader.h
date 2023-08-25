#ifndef GRAPH_SYSTEMS_CORE_IO_MUTABLE_CSR_READER_H_
#define GRAPH_SYSTEMS_CORE_IO_MUTABLE_CSR_READER_H_

#include <filesystem>
#include <fstream>
#include <list>
#include <memory>
#include <string>
#include <utility>

#include "data_structures/buffer.h"
#include "data_structures/graph_metadata.h"
#include "data_structures/serialized.h"
#include "io/reader_writer.h"

namespace sics::graph::core::io {

class MutableCSRReader : public Reader {
 private:
  using OwnedBuffer = sics::graph::core::data_structures::OwnedBuffer;
  using Serialized = sics::graph::core::data_structures::Serialized;

 public:
  MutableCSRReader(const std::string& root_path) : root_path_(root_path) {}

  void Read(ReadMessage* message,
            common::TaskRunner* runner = nullptr) override;

  void ReadFromBin(const std::string& path, Serialized* serialized_graph);

 private:
  const std::string root_path_;
};

}  // namespace sics::graph::core::io

#endif  // GRAPH_SYSTEMS_CORE_IO_MUTABLE_CSR_READER_H_
