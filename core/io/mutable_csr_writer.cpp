#include "io/mutable_csr_writer.h"

namespace sics::graph::core::io {

void MutableCSRWriter::Write(WriteMessage* message,
                             common::TaskRunner* /* runner */) {
  std::string file_path =
      root_path_ + "graphs/" + std::to_string(message->graph_id) + ".bin";
  std::string label_path =
      root_path_ + "label/" + std::to_string(message->graph_id) + ".bin";

  if (common::configs.edge_mutate) {
    file_path += ".new";
    label_path += ".new";
  }

  if (message->serialized->HasNext()) {
    auto a = message->serialized->PopNext();
    WriteMetaInfoToBin(file_path, a);
    WriteLabelInfoToBin(file_path, a);
  }
}

void MutableCSRWriter::WriteMetaInfoToBin(
    const std::string& path, const std::vector<OwnedBUffer>& buffers) {
  // TODO: create if file do not exist
  std::ofstream file(path, std::ios::binary);
  if (!file) {
    LOG_FATAL("Error opening bin file: ", path.c_str());
  }

  file.write((char*)(buffers.at(0).Get()), buffers.at(0).GetSize());
  if (!file) {
    LOG_FATAL("Error writing meta data file: ", path.c_str());
  }
  file.write((char*)buffers.at(1).Get(), buffers.at(1).GetSize());
  if (!file) {
    LOG_FATAL("Error writing label data file: ", path.c_str());
  }
  file.close();
}

void MutableCSRWriter::WriteLabelInfoToBin(
    const std::string& path, const std::vector<OwnedBUffer>& buffers) {
  std::ofstream label_file(path, std::ios::binary);
  if (!label_file) {
    LOG_INFO("Error opening label bin file: ", path.c_str());
  }

  label_file.write((char*)(buffers.at(2).Get()), buffers.at(2).GetSize());
  if (!label_file) {
    LOG_FATAL("Error writing label data file: ", path.c_str());
  }
  label_file.close();
}

}  // namespace sics::graph::core::io