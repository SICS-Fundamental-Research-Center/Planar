#ifndef MINICLEAN_IO_MIINCLEAN_CSR_READER_H_
#define MINICLEAN_IO_MINICLEAN_CSR_READER_H_

#include <filesystem>
#include <fstream>
#include <iostream>
#include <list>
#include <vector>
#include <memory>
#include <sstream>
#include <string>
#include <utility>

#include <yaml-cpp/yaml.h>

#include "core/common/multithreading/task_runner.h"
#include "core/data_structures/buffer.h"
#include "core/data_structures/serialized.h"
#include "core/io/reader_writer.h"
#include "core/scheduler/message.h"

namespace sics::graph::miniclean::io {

// @DESCRIPTION Class to read data from ssd to memory
// @EXAMPLE
// LabeledCSRReader reader(subgraph_1_path);
// SerializedImmutableCSRGraph* serialized_immutable_csr =
//     new SerializedImmutableCSRGraph();
// ReadMessage* read_message = new ReadMessage();
// read_message->graph_id = 1;
// read_message->response_serialized = serialized_immutable_csr;
// reader.Read(read_message);
class MiniCleanCSRReader : public sics::graph::core::io::Reader {
 private:
  using OwnedBuffer = sics::graph::core::data_structures::OwnedBuffer;
  using ReadMessage = sics::graph::core::scheduler::ReadMessage;
  using Serialized = sics::graph::core::data_structures::Serialized;
  using TaskRunner = sics::graph::core::common::TaskRunner;

 public:
  explicit MiniCleanCSRReader(const std::string& root_path)
      : root_path_(root_path) {}

  // TODO: add a function to read attribute file.
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
  void Read(ReadMessage* message, TaskRunner* runner = nullptr) override;

 private:
  // read label files
  void ReadLabel(const std::string& subgraph_folder_path,
                 Serialized* dst_object);

  // read data file
  void ReadBinFile(const std::string& path, Serialized* dst_object);

 private:
  const std::string root_path_;
};

}  // namespace sics::graph::miniclean::io

#endif  // CORE_IO_LABELED_CSR_READER_H_
