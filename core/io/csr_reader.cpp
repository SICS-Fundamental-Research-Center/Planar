#include "io/csr_reader.h"

namespace sics::graph::core::io {

void CSRReader::Read(scheduler::ReadMessage* message,
                     common::TaskRunner* /* runner */) {
  // Init path.
  std::string file_path =
      root_path_ + "graphs/" + std::to_string(message->graph_id) + ".bin";
  std::ifstream file(file_path, std::ios::binary);

  file.seekg(0, std::ios::end);
  size_t file_size = file.tellg();
  file.seekg(0, std::ios::beg);

  // Create list of owned buffer
  std::list<OwnedBuffer> file_buffers;
  file_buffers.emplace_back(file_size);

  // Read the file data.
  file.read(reinterpret_cast<char*>(file_buffers.back().Get()), file_size);
  if (!file) throw std::runtime_error("Error reading file: " + file_path);

  // Give it to serialized object
  message->response_serialized->ReceiveBuffers(std::move(file_buffers));
  file.close();
}

}  // namespace sics::graph::core::io
