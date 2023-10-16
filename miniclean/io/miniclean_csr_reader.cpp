#include "miniclean/io/miniclean_csr_reader.h"

namespace xyz::graph::miniclean::io {

void MiniCleanCSRReader::Read(ReadMessage* message, TaskRunner* /* runner */) {
  std::ostringstream ss;
  ss << message->graph_id;
  std::string subgraph_id_str = ss.str();
  std::string subgraph_folder_path =
      root_path_ + "/" + subgraph_id_str + "/" + subgraph_id_str;
  std::string data_file_path = subgraph_folder_path + "_data.bin";
  Serialized* dst_object = message->response_serialized;

  // read files
  try {
    ReadBinFile(data_file_path, dst_object);
  } catch (const std::exception& e) {
    LOG_FATAL(e.what());
  }

  // read label files
  ReadLabel(subgraph_folder_path, dst_object);

  dst_object->SetComplete();
}

void MiniCleanCSRReader::ReadLabel(const std::string& subgraph_folder_path,
                                   Serialized* dst_object) {
  // const std::string inedge_data_file_path =
  //     subgraph_folder_path + "_inedge_label.bin";
  const std::string outedge_data_file_path =
      subgraph_folder_path + "_outedge_label.bin";
  const std::string vertex_data_file_path =
      subgraph_folder_path + "_vertex_label.bin";
  const std::string vertex_attribute_file_path =
      subgraph_folder_path + "_vertex_attr.bin";

  // read files
  try {
    // ReadBinFile(inedge_data_file_path, dst_object);
    ReadBinFile(outedge_data_file_path, dst_object);
    ReadBinFile(vertex_data_file_path, dst_object);
    ReadBinFile(vertex_attribute_file_path, dst_object);
  } catch (const std::exception& e) {
    LOG_FATAL(e.what());
  }
}

void MiniCleanCSRReader::ReadBinFile(const std::string& path,
                                     Serialized* dst_object) {
  std::ifstream file(path, std::ios::binary);
  if (!file) {
    LOG_FATAL("Error opening bin file: ", path.c_str());
  }

  // Get the file size.
  file.seekg(0, std::ios::end);
  size_t fileSize = file.tellg();
  file.seekg(0, std::ios::beg);

  // create list of owned buffer
  std::vector<OwnedBuffer> file_buffers;
  file_buffers.emplace_back(fileSize);

  // Read the file data.
  file.read(reinterpret_cast<char*>(file_buffers.back().Get()), fileSize);
  if (!file) {
    LOG_FATAL("Error reading file: ", path.c_str());
  }

  // give it to serialized object
  dst_object->ReceiveBuffers(std::move(file_buffers));
}

}  // namespace xyz::graph::miniclean::io
