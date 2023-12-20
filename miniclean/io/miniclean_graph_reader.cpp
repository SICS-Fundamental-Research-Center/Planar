#include "miniclean/io/miniclean_graph_reader.h"

namespace sics::graph::miniclean::io {

void MiniCleanGraphReader::Read(ReadMessage* message,
                                TaskRunner* /* runner */) {
  // Init path.
  std::string graph_home = root_path_ + "partition_result/graphs/";
  std::string csr_path =
      graph_home + std::to_string(message->graph_id) + ".bin";
  std::string is_in_graph_bitmap_path =
      root_path_ + "partition_result/bitmap/is_in_graph/" +
      std::to_string(message->graph_id) + ".bin";

  // Serialized object.
  Serialized* serialized_object = message->response_serialized;

  // Load CSR.
  LoadBinFileAsBuffer(csr_path, serialized_object);

  // Load is_in_graph bitmap.
  LoadBinFileAsBuffer(is_in_graph_bitmap_path, serialized_object);

  // Load attributes.
  // 1. parse graph meta information
  std::string meta_path = root_path_ + "partition_result/meta.yaml";
  YAML::Node node = YAML::LoadFile(meta_path);
  MiniCleanGraphMetadata graph_metadata = node.as<MiniCleanGraphMetadata>();
  // 2. get attribute path from meta information
  GraphID gid = message->graph_id;
  std::filesystem::path original_dir = std::filesystem::current_path();
  std::filesystem::current_path(root_path_ + "partition_result/");
  for (const auto& attr_path :
       graph_metadata.subgraphs[gid].vattr_id_to_file_path) {
    // Determine whether the attribute is empty.
    if (attr_path.empty()) {
      std::vector<OwnedBuffer> file_buffers;
      serialized_object->ReceiveBuffers(std::move(file_buffers));
      continue;
    }
    std::string canonical_attr_path =
        std::filesystem::canonical(attr_path).string();
    LoadBinFileAsBuffer(canonical_attr_path, serialized_object);
  }
  std::filesystem::current_path(original_dir);

  // Finish the reading.
  serialized_object->SetComplete();
}

void MiniCleanGraphReader::LoadBinFileAsBuffer(const std::string& bin_path,
                                               Serialized* serialized_object) {
  std::ifstream file(bin_path, std::ios::binary);
  if (!file) {
    LOG_FATAL("Error opening bin file: ", bin_path.c_str());
  }

  // Compute the file size.
  file.seekg(0, std::ios::end);
  size_t fileSize = file.tellg();
  file.seekg(0, std::ios::beg);

  // Initialize buffer.
  std::vector<OwnedBuffer> file_buffers;
  file_buffers.emplace_back(fileSize);

  // Read the file data.
  file.read(reinterpret_cast<char*>(file_buffers.back().Get()), fileSize);
  if (!file) {
    LOG_FATAL("Error reading file: ", bin_path.c_str());
  }

  // give it to serialized object
  serialized_object->ReceiveBuffers(std::move(file_buffers));
}
}  // namespace sics::graph::miniclean::io