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
  if (common::Configurations::Get()->edge_mutate && message->round != 0) {
    file_path += ".new";
    label_path += ".new";
  }

  Serialized* graph_serialized = new SerializedMuatbleCSRGraph();
  std::vector<OwnedBuffer> buffers;
  ReadMetaInfoFromBin(file_path, message->num_vertices, &buffers);
  ReadLabelInfoFromBin(label_path, &buffers);

  graph_serialized->ReceiveBuffers(std::move(buffers));

  message->response_serialized = graph_serialized;
}

void MutableCSRReader::ReadMetaInfoFromBin(const std::string& path,
                                           common::VertexCount num_vertices,
                                           std::vector<OwnedBuffer>* buffers) {
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
  buffers->emplace_back(meta_size);
  buffers->emplace_back(edge_size);

  file.read((char*)(buffers->at(0).Get()), meta_size);
  if (!file) {
    LOG_FATAL("Error reading file meta info: ", path.c_str());
  }
  file.read((char*)(buffers->at(1).Get()), edge_size);
  if (!file) {
    LOG_FATAL("Error reading file edge info: ", path.c_str());
  }
}

void MutableCSRReader::ReadLabelInfoFromBin(const std::string& path,
                                            std::vector<OwnedBuffer>* buffers) {
  std::ifstream file(path, std::ios::binary);
  if (!file) {
    LOG_FATAL("Error opening bin file: ", path.c_str());
  }

  file.seekg(0, std::ios::end);
  size_t file_size = file.tellg();
  file.seekg(0, std::ios::beg);

  //  buffers->emplace_back(OwnedBuffer(file_size));
  buffers->emplace_back(file_size);

  file.read((char*)(buffers->at(2).Get()), file_size);
  if (!file) {
    LOG_FATAL("Error reading file: ", path.c_str());
  }
}

}  // namespace sics::graph::core::io
