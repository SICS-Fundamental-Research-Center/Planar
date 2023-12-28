#include "io/pram_block_reader.h"

namespace sics::graph::core::io {
using SerializedPramBlockCSRGraph =
    data_structures::graph::SerializedPramBlockCSRGraph;

void PramBlockReader::Read(scheduler::ReadMessage* message,
                           common::TaskRunner* /* runner */) {
  // Init path.
  std::string path = root_path_ + std::to_string(message->graph_id) + ".bin";
}

void PramBlockReader::ReadBlockInfo(const std::string& path,
                                    common::VertexCount num_vertices,
                                    std::vector<OwnedBuffer>* buffers) {
  std::ifstream file(path, std::ios::binary);
  if (!file) {
    LOG_FATAL("Error opening bin file: ", path.c_str());
  }

  file.seekg(0, std::ios::end);
  size_t file_size = file.tellg();
  file.seekg(0, std::ios::beg);

  read_size_ += (file_size >> 20);

  size_t meta_size = num_vertices * sizeof(common::VertexID) * 2 +
                     num_vertices * sizeof(common::EdgeIndex);
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

}  // namespace sics::graph::core::io