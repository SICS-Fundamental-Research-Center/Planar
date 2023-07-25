#include "io/reader.h"

namespace sics::graph::core::io {

void Reader::ReadSubgraph(const std::string& path, Serialized* dst_object, int read_type = 0) {
  if (read_type == 0) {
    ReadCsr(path, dst_object);
  }
}


void Reader::ReadCsr(const std::string& path, Serialized* dst_object) {
  std::filesystem::path dir(path);
  std::string subgraph_id_str = dir.filename().string();
  std::string data_file_path =
      path + "/" + subgraph_id_str + "_data.bin";
  // leave for reading other files like attribute

  // read files
  try {
    ReadBinFile(data_file_path, dst_object);
  } catch (const std::exception& e) {
    std::cerr << e.what() << std::endl;
    exit(EXIT_FAILURE);
  }
}

void Reader::ReadBinFile(std::string data_file_path, Serialized* dst_object) {
  std::ifstream file(data_file_path, std::ios::binary);
  if (!file) {
    throw std::runtime_error("Error opening bin file: " + data_file_path);
  }

  // Get the file size.
  file.seekg(0, std::ios::end);
  size_t fileSize = file.tellg();
  file.seekg(0, std::ios::beg);

  // Allocate memory to store file data using smart pointers (unique_ptr).
  std::unique_ptr<uint8_t[]> data = std::make_unique<uint8_t[]>(fileSize);

  // Read the file data.
  file.read(reinterpret_cast<char*>(data.get()), fileSize);
  if (!file) {
    throw std::runtime_error("Error reading file: " + data_file_path);
  }

  // Move the data to an OwnedBuffer object.
  std::list<OwnedBuffer> file_buffers;
  file_buffers.emplace_back(fileSize);
  file_buffers.back().SetPointer(data.release());
  dst_object->ReceiveBuffers(std::move(file_buffers));
}
}  // namespace sics::graph::core::io
