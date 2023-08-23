#include "io/csr_writer.h"

namespace sics::graph::core::io {

void CSRWriter::Write(WriteMessage* message, common::TaskRunner* runner) {
  std::string file_path =
      root_path_ + "graphs/" + std::to_string(message->graph_id) + ".bin";
  std::string label_path =
      root_path_ + "graphs/" + std::to_string(message->graph_id) + "label.bin";

  if (message->serialized->HasNext()) {
    auto a = message->serialized->PopNext();
    WriteToBin(file_path, a.front());
    a.pop_front();
    WriteToBin(label_path, a.front());
    a.pop_front();
  }
}

void CSRWriter::WriteToBin(const std::string& path, const OwnedBUffer& buffer) {
  // TODO: create if file do not exist
  std::ofstream file(path, std::ios::binary);
  if (!file) {
    LOG_FATAL("Error opening bin file: ", path.c_str());
  }

  file.write(reinterpret_cast<char*>(buffer.Get()), buffer.GetSize());
  if (!file) {
    LOG_FATAL("Error writing file: ", path.c_str());
  }
}

}  // namespace sics::graph::core::io