#include "io/labeled_csr_reader.h"

namespace sics::graph::core::io {

void LabeledCSRReader::Read(ReadMessage* message, common::TaskRunner* runner) {
  std::ostringstream ss;
  ss << message->graph_id;
  std::string subgraph_id_str = ss.str();
  std::string subgraph_folder_path = root_path_ + "/" + subgraph_id_str + "/" + subgraph_id_str;
  std::string data_file_path = subgraph_folder_path + "_data.bin";
  Serialized* dst_object = message->response_serialized;

  // read files
  try {
    ReadBinFile(data_file_path, dst_object);
  } catch (const std::exception& e) {
    std::cerr << e.what() << std::endl;
    exit(EXIT_FAILURE);
  }

  // read label files
  ReadLabel(subgraph_folder_path, dst_object);

  dst_object->SetComplete();
}

void LabeledCSRReader::ReadLabel(const std::string& subgraph_folder_path, Serialized* dst_object) {
  std::string inedge_data_file_path = subgraph_folder_path + "_inedge_label.bin";
  std::string outedge_data_file_path = subgraph_folder_path + "_outedge_label.bin";
  std::string vertex_data_file_path = subgraph_folder_path + "_vertex_label.bin";

  // read files
  try {
    ReadBinFile(inedge_data_file_path, dst_object);
    ReadBinFile(outedge_data_file_path, dst_object);
    ReadBinFile(vertex_data_file_path, dst_object);
  } catch (const std::exception& e) {
    std::cerr << e.what() << std::endl;
    exit(EXIT_FAILURE);
  }
}

void LabeledCSRReader::ReadBinFile(const std::string& path, Serialized* dst_object) {
  std::ifstream file(path, std::ios::binary);
  if (!file) {
    throw std::runtime_error("Error opening bin file: " + path);
  }

  // Get the file size.
  file.seekg(0, std::ios::end);
  size_t fileSize = file.tellg();
  file.seekg(0, std::ios::beg);

  // Allocate memory to store file data using smart pointers (unique_ptr).
  std::unique_ptr<uint8_t[]> data = std::make_unique<uint8_t[]>(fileSize);

  // create list of owned buffer
  std::list<OwnedBuffer> file_buffers;
  file_buffers.emplace_back(fileSize);

  // Read the file data.
  file.read(reinterpret_cast<char*>(file_buffers.back().Get()), fileSize);
  if (!file) {
    throw std::runtime_error("Error reading file: " + path);
  }

  // give it to serialized object
  dst_object->ReceiveBuffers(std::move(file_buffers));
}

}  // namespace sics::graph::core::io
