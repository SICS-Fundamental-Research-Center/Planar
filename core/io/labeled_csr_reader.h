#ifndef CORE_IO_LABELED_CSR_READER_H_
#define CORE_IO_LABELED_CSR_READER_H_

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

// @DESCRIPTION Class to read data from ssd to memory
// @EXAMPLE
// LabeledCSRReader reader(subgraph_1_path);
// SerializedImmutableCSRGraph* serialized_immutable_csr =
//     new SerializedImmutableCSRGraph();
// ReadMessage* read_message = new ReadMessage();
// read_message->graph_id = 1;
// read_message->response_serialized = serialized_immutable_csr;
// reader.Read(read_message);
class LabeledCSRReader : public Reader {
 public:
  using OwnedBuffer = sics::graph::core::data_structures::OwnedBuffer;
  using Serialized = sics::graph::core::data_structures::Serialized;
  using ReadMessage = sics::graph::core::scheduler::ReadMessage;

  explicit LabeledCSRReader(const std::string& root_path) : root_path_(root_path) {}

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
            common::TaskRunner* runner = nullptr) override;

 private:
  // read label files
  void ReadLabel(const std::string& subgraph_folder_path, Serialized* dst_object);

  // read data file
  void ReadBinFile(const std::string& path, Serialized* dst_object);

 private:
  const std::string root_path_;
};

}  // namespace sics::graph::core::io

#endif  // CORE_IO_LABELED_CSR_READER_H_
