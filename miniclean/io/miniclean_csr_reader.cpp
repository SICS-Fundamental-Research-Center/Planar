#include "miniclean/io/miniclean_csr_reader.h"

namespace sics::graph::miniclean::io {

void MiniCleanCSRReader::Read(ReadMessage* message, TaskRunner* /* runner */) {
  std::ostringstream ss;
  ss << message->graph_id;
  std::string subgraph_id_str = ss.str();

  std::string subgraph_file_path =
      root_path_ + "/graph/graphs/" + subgraph_id_str + ".bin";
  std::string vlabel_file_path = root_path_ + "/vlabel/vlabels.bin";
  std::string vlabel_range_path = root_path_ + "/vlabel/vlabel_ranges.bin";
  std::string elabel_file_path = root_path_ + "/elabel/out_elabel.bin";
  std::string vattr_file_path = root_path_ + "/attribute/vertex_attribute.bin";
  std::string vattr_offset_path =
      root_path_ + "/attribute/vertex_attribute_offset.bin";

  Serialized* dst_object = message->response_serialized;

  // read files
  try {
    ReadBinFile(subgraph_file_path, dst_object);
    ReadBinFile(vlabel_file_path, dst_object);
    ReadBinFile(vlabel_range_path, dst_object);
    ReadBinFile(elabel_file_path, dst_object);
    ReadBinFile(vattr_file_path, dst_object);
    ReadBinFile(vattr_offset_path, dst_object);
  } catch (const std::exception& e) {
    LOG_FATAL(e.what());
  }

  dst_object->SetComplete();
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

}  // namespace sics::graph::miniclean::io
