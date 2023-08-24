#include "io/mutable_csr_reader.h"

#include "data_structures/graph/serialized_mutable_csr_graph.h"

namespace sics::graph::core::io {

using SerializedMuatbleCSRGraph =
    data_structures::graph::SerializedMutableCSRGraph;

void MutableCSRReader::Read(scheduler::ReadMessage* message,
                            common::TaskRunner* /* runner */) {
  // Init path.
  std::string file_path =
      root_path_ + "graphs/" + std::to_string(message->graph_id) + ".bin";
  std::string label_path =
      root_path_ + "graphs/" + std::to_string(message->graph_id) + "label.bin";

  auto graph_serialized = std::make_unique<SerializedMuatbleCSRGraph>();

  ReadFromBin(file_path, graph_serialized.get());
  ReadFromBin(label_path, graph_serialized.get());
}

void MutableCSRReader::ReadFromBin(const std::string& path,
                                   Serialized* serialized_graph) {
  std::ifstream file(path, std::ios::binary);
  if (!file) {
    LOG_FATAL("Error opening bin file: ", path.c_str());
  }

  file.seekg(0, std::ios::end);
  size_t file_size = file.tellg();
  file.seekg(0, std::ios::beg);

  std::list<OwnedBuffer> buffers;
  buffers.emplace_back(file_size);

  file.read(reinterpret_cast<char*>(buffers.back().Get()), file_size);
  if (!file) {
    LOG_FATAL("Error reading file: ", path.c_str());
  }

  serialized_graph->ReceiveBuffers(std::move(buffers));
}

}  // namespace sics::graph::core::io
