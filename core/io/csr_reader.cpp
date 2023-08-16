#include "io/csr_reader.h"

namespace sics::graph::core::io {

void CSRReader::Read(const GraphID gid, Serialized* dst_object) {
  if (gid > graph_metadata_.get_num_subgraphs()) {
    throw std::runtime_error("not exists gid: " + gid);
  }

  std::string file_path = root_path_ + "graphs/" + std::to_string(gid) + ".bin";
  std::ifstream file(file_path, std::ios::binary);

  file.seekg(0, std::ios::end);
  size_t file_size = file.tellg();
  file.seekg(0, std::ios::beg);

  // Allocate memory to store file data using smart pointers (unique_ptr).
  std::unique_ptr<uint8_t[]> data = std::make_unique<uint8_t[]>(file_size);

  // create list of owned buffer
  std::list<OwnedBuffer> file_buffers;
  file_buffers.emplace_back(file_size);

  // Read the file data.
  file.read(reinterpret_cast<char*>(file_buffers.back().Get()), file_size);
  if (!file) throw std::runtime_error("Error reading file: " + file_path);

  // give it to serialized object
  dst_object->ReceiveBuffers(std::move(file_buffers));
  file.close();
}

}  // namespace sics::graph::core::io
