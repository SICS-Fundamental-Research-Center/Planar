#include "io/mutable_csr_reader.h"

namespace sics::graph::core::io {

using SerializedMuatbleCSRGraph =
    data_structures::graph::SerializedMutableCSRGraph;

void MutableCSRReader::Read(scheduler::ReadMessage* message,
                            common::TaskRunner* /* runner */) {
  // Init path.
  std::string file_path =
      root_path_ + "graphs/" + std::to_string(message->graph_id) + ".bin";
  std::string label_path =
      root_path_ + "label/" + std::to_string(message->graph_id) + ".bin";
  if (common::Configurations::Get()->edge_mutate && message->round != 0) {
    file_path += ".new";
  }
  if (message->round != 0) {
    label_path += ".new";
  }
  std::string in_graph_bitmap_path = root_path_ + "bitmap/is_in_graph/" +
                                     std::to_string(message->graph_id) + ".bin";
  std::string src_bitmap_path = root_path_ + "bitmap/src_map/" +
                                std::to_string(message->graph_id) + ".bin";
  std::string index_path = "";
  if (core::common::Configurations::Get()->partition_type ==
      common::PartitionType::EdgeCut) {
    index_path = root_path_ + "index/global_index.bin";
  } else {
    index_path =
        root_path_ + "index/" + std::to_string(message->graph_id) + ".bin";
  }

  Serialized* graph_serialized = message->response_serialized;
  std::vector<OwnedBuffer> buffers;
  buffers.reserve(6);
  ReadMetaInfoFromBin(file_path, message->num_vertices, &buffers);
  ReadSingleBufferFromBin(label_path, &buffers);
  if (!common::Configurations::Get()->radical) {
    ReadSingleBufferFromBin(in_graph_bitmap_path, &buffers);
    ReadSingleBufferFromBin(src_bitmap_path, &buffers);
    ReadSingleBufferFromBin(index_path, &buffers);
  }
  graph_serialized->ReceiveBuffers(std::move(buffers));
}

void MutableCSRReader::ReadMetaInfoFromBin(const std::string& path,
                                           common::VertexCount num_vertices,
                                           std::vector<OwnedBuffer>* buffers) {
  std::ifstream file(path, std::ios::binary);
  if (!file) {
    LOG_FATAL("Error opening bin file: ", path.c_str());
  }

  //  file.seekg(0, std::ios::end);
  //  size_t file_size = file.tellg();
  //  file.seekg(0, std::ios::beg);
  //  read_size_ += (file_size >> 20);

  size_t index_size = num_vertices * sizeof(common::VertexID);
  size_t degree_size = num_vertices * sizeof(common::VertexID);
  size_t offset_size = num_vertices * sizeof(common::EdgeIndex);
  size_t meta_size = 0;

  file.seekg(0, std::ios::end);
  size_t file_size = file.tellg();
  if (common::Configurations::Get()->radical) {
    file.seekg(index_size, std::ios::beg);
    read_size_ += ((file_size - index_size) >> 20);
    meta_size = degree_size + offset_size;
  } else {
    file.seekg(0, std::ios::beg);
    read_size_ += (file_size >> 20);
    meta_size = index_size + degree_size + offset_size;
  }

  size_t edge_size = file_size - index_size - degree_size - offset_size;

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

void MutableCSRReader::ReadSingleBufferFromBin(
    const std::string& path, std::vector<OwnedBuffer>* buffers) {
  std::ifstream file(path, std::ios::binary);
  if (!file) {
    LOG_FATAL("Error opening bin file: ", path.c_str());
  }

  file.seekg(0, std::ios::end);
  size_t file_size = file.tellg();
  file.seekg(0, std::ios::beg);

  read_size_ += (file_size >> 20);

  //  buffers->emplace_back(OwnedBuffer(file_size));
  buffers->emplace_back(file_size);

  file.read((char*)(buffers->back().Get()), file_size);
  if (!file) {
    LOG_FATAL("Error reading file: ", path.c_str());
  }
  file.close();
}

}  // namespace sics::graph::core::io
