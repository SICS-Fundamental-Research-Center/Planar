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
      root_path_ + "label/" + std::to_string(message->graph_id) + ".bin";

  Serialized* graph_serialized = new SerializedMuatbleCSRGraph();

  ReadMetaInfoFromBin(file_path, message->num_vertices, graph_serialized);
  ReadLabelInfoFromBin(label_path, graph_serialized);

  message->response_serialized = graph_serialized;
}

void MutableCSRReader::ReadMetaInfoFromBin(const std::string& path,
                                           common::VertexCount num_vertices,
                                           Serialized* serialized_graph) {
  std::ifstream file(path, std::ios::binary);
  if (!file) {
    LOG_FATAL("Error opening bin file: ", path.c_str());
  }

  file.seekg(0, std::ios::end);
  size_t file_size = file.tellg();
  file.seekg(0, std::ios::beg);
  size_t meta_size = num_vertices * sizeof(common::VertexID) * 3;
  size_t edge_size = file_size - meta_size;

  // split the buffer into two parts
  std::vector<OwnedBuffer> buffers;
  buffers.emplace_back(meta_size);
  buffers.emplace_back(edge_size);

  file.read((char*)(buffers.at(0).Get()), meta_size);
  if (!file) {
    LOG_FATAL("Error reading file meta info: ", path.c_str());
  }
  file.read((char*)(buffers.at(1).Get()), edge_size);
  if (!file) {
    LOG_FATAL("Error reading file edge info: ", path.c_str());
  }
  serialized_graph->ReceiveBuffers(std::move(buffers));
}

void MutableCSRReader::ReadLabelInfoFromBin(const std::string& path,
                                            Serialized* serialized_graph) {
  std::ifstream file(path, std::ios::binary);
  if (!file) {
    LOG_FATAL("Error opening bin file: ", path.c_str());
  }

  file.seekg(0, std::ios::end);
  size_t file_size = file.tellg();
  file.seekg(0, std::ios::beg);

  std::vector<OwnedBuffer> buffers;
  buffers.emplace_back(file_size);

  file.read(reinterpret_cast<char*>(buffers.back().Get()), file_size);
  if (!file) {
    LOG_FATAL("Error reading file: ", path.c_str());
  }

  serialized_graph->ReceiveBuffers(std::move(buffers));
}

}  // namespace sics::graph::core::io
