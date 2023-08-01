#include "io/basic_writer.h"

namespace sics::graph::core::io {
void BasicWriter::WriteSubgraph(const std::string& path, Serialized* src_object, int write_type) {
  if (write_type == 0) {
    WriteCSR(path, src_object);
  }
}

void BasicWriter::WriteCSR(const std::string& path, Serialized* src_object) {
  std::filesystem::path dir(path);
  std::string subgraph_id_str = dir.filename().string();
  std::string data_file_path =
      path + "/" + subgraph_id_str + "_data.bin";

  try {
    WriteBinFile(data_file_path, src_object);
  } catch (const std::exception& e) {
    std::cerr << e.what() << std::endl;
    exit(EXIT_FAILURE);
  }
}

void BasicWriter::WriteBinFile(const std::string& path, Serialized* src_object) {
  // Get the list of OwnedBuffers from Serialized.
  std::list<OwnedBuffer> buffers = src_object->PopNext();

  if (buffers.empty()) {
      throw std::runtime_error("No data available in the Serialized object.");
  }

  // Get the first buffer from the list.
  // temporarily only one buffer
  OwnedBuffer& firstBuffer = buffers.front();

  // Open the binary file for writing.
  std::ofstream file(path, std::ios::binary);
  if (!file) {
      throw std::runtime_error("Error opening bin file for writing: " + path);
  }

  // Write the first buffer to the file.
  file.write(reinterpret_cast<const char*>(firstBuffer.Get()), firstBuffer.GetSize());
  if (!file) {
      throw std::runtime_error("Error writing to file: " + path);
  }
}
}  // namespace sics::graph::core::io
