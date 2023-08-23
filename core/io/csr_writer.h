#ifndef CORE_IO_CSR_WRITER_H_
#define CORE_IO_CSR_WRITER_H_

#include <fstream>
#include <list>
#include <string>
#include <utility>

#include "data_structures/buffer.h"
#include "data_structures/graph_metadata.h"
#include "data_structures/serialized.h"
#include "io/reader_writer.h"
#include "scheduler/message.h"

namespace sics::graph::core::io {

class CSRWriter : public Writer {
 private:
  using OwnedBUffer = data_structures::OwnedBuffer;
  using Serialized = data_structures::Serialized;

 public:
  CSRWriter(const std::string& root_path) : root_path_(root_path) {}

  void Write(WriteMessage* message,
             common::TaskRunner* runner = nullptr) override;

  void WriteToBin(const std::string& path, Serialized* serialized_graph);

 private:
  const std::string root_path_;
};

}  // namespace sics::graph::core::io

#endif  // CORE_IO_CSR_WRITER_H_
