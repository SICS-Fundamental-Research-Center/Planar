#include "nvme/io/pram_block_reader.h"

namespace sics::graph::nvme::io {
using SerializedPramBlockCSRGraph =
    data_structures::graph::SerializedPramBlockCSRGraph;

void PramBlockReader::Read(ReadMessage* message,
                           core::common::TaskRunner* /* runner */) {
  // Init path.
  std::string path = "";
  if (message->changed) {
    path =
        root_path_ + "blocks/" + std::to_string(message->graph_id) + ".bin.new";
  } else {
    path = root_path_ + "blocks/" + std::to_string(message->graph_id) + ".bin";
  }

  // Read block info.
  Serialized* block_serialized = message->serialized;
  std::vector<OwnedBuffer> buffers;
  ReadBlockInfo(path, message->num_vertices, &buffers);

  block_serialized->ReceiveBuffers(std::move(buffers));
  message->bytes_read = read_size_;
  read_size_ = 0;
}

void PramBlockReader::ReadBlockInfo(const std::string& path,
                                    core::common::VertexCount num_vertices,
                                    std::vector<OwnedBuffer>* buffers) {
  std::ifstream file(path, std::ios::binary);
  if (!file) {
    LOG_FATAL("Error opening bin file: ", path.c_str());
  }

  file.seekg(0, std::ios::end);
  size_t file_size = file.tellg();
  file.seekg(0, std::ios::beg);

  read_size_ += (file_size >> 20);

  size_t meta_size = num_vertices * sizeof(core::common::VertexID) +
                     num_vertices * sizeof(core::common::EdgeIndex);
  size_t edge_size = file_size - meta_size;

  // split the buffer into two parts
  buffers->emplace_back(meta_size);
  file.read((char*)(buffers->back().Get()), meta_size);
  if (!file) {
    LOG_FATAL("Error reading file meta info: ", path.c_str());
  }

  buffers->emplace_back(edge_size);
  file.read((char*)(buffers->back().Get()), edge_size);
  if (!file) {
    LOG_FATAL("Error reading file edge info: ", path.c_str());
  }
  file.close();
}

void PramBlockReader::ReadNeighborInfo(const std::string& path,
                                       std::vector<OwnedBuffer>* buffers) {
  std::ifstream file(path, std::ios::binary);
  if (!file) {
    LOG_FATAL("Error opening bin file: ", path.c_str());
  }

  file.seekg(0, std::ios::end);
  size_t file_size = file.tellg();
  file.seekg(0, std::ios::beg);

  buffers->emplace_back(file_size);
  file.read((char*)(buffers->back().Get()), file_size);
  if (!file) {
    LOG_FATAL("Error reading file edge info: ", path.c_str());
  }
  file.close();
}

}  // namespace sics::graph::nvme::io