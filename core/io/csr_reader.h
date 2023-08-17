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
#include <sstream>

#include "data_structures/buffer.h"
#include "data_structures/serialized.h"
#include "io/reader_writer.h"
#include "scheduler/message.h"

namespace sics::graph::core::io {

// Class to read data from ssd to memory
// Example:
//  Reader reader;
//  SerializedImmutableCSR* serialized_immutable_csr =
//      new SerializedImmutableCSR();
//  reader.ReadSubgraph(PATH, serialized_immutable_csr);
class CSRReader {
 public:
  using OwnedBuffer = sics::graph::core::data_structures::OwnedBuffer;
  using Serialized = sics::graph::core::data_structures::Serialized;
  using ReadMessage = sics::graph::core::scheduler::ReadMessage;

  CSRReader() {}

  // read csr of a certain subgraph from ssd
  // workdir structure:
  //  - dir:{work_dir_}
  //    - dir:0
  //      - file:0_data.bin
  //      - file:0_inedge_label.bin
  //      - file:0_outedge_label.bin
  //      - file:0_vertex_label.bin
  //      - file:0_attr.bin
  //    - dir:1
  //      - file:1_data.bin
  //      - file:1_inedge_label.bin
  //      - file:1_outedge_label.bin
  //      - file:1_vertex_label.bin
  //      - file:1_attr.bin
  //    - file:csr_global.yaml
  void Read(ReadMessage* message,
            common::TaskRunner* runner = nullptr);

 private:
  // read label files
  void ReadLabel(const std::string& path, Serialized* dst_object);

  // read data file
  void ReadBinFile(const std::string& path, Serialized* dst_object);
};

}  // namespace sics::graph::core::io

#endif  // CORE_IO_CSR_READER_H_
