#ifndef MINICLEAN_IO_MIINCLEAN_CSR_READER_H_
#define MINICLEAN_IO_MINICLEAN_CSR_READER_H_

#include <yaml-cpp/yaml.h>

#include <filesystem>
#include <fstream>
#include <iostream>
#include <list>
#include <memory>
#include <sstream>
#include <string>
#include <utility>
#include <vector>

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

  // read csr of a certain subgraph from ssd
  // workdir structure:
  //  - dir:{root_path_}
  //    - dir: attribute
  //      - file: vertex_attribute_offset.bin
  //      - file: vertex_attribute.bin
  //    - dir: elabel
  //      - file: out_elabel.bin
  //    - dir: graph
  //      - dir: graphs
  //        - file: {subgraph_id}.bin
  //      - file: meta.yaml
  //    - dir: vlabel
  //      - file: vlabels.bin
  void Read(ReadMessage* message, TaskRunner* runner = nullptr) override;

 private:
  // read data file
  void ReadBinFile(const std::string& path, Serialized* dst_object);

 private:
  const std::string root_path_;
};

}  // namespace sics::graph::miniclean::io

#endif  // CORE_IO_LABELED_CSR_READER_H_
