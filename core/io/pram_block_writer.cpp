#include "io/pram_block_writer.h"

namespace sics::graph::core::io {

void PramBlockWriter::Write(WriteMessage* message,
                            common::TaskRunner* /* runner */) {
  std::string file_path =
      root_path_ + std::to_string(message->graph_id) + ".bin.new";
  if (message->serialized->HasNext()) {
    auto a = message->serialized->PopNext();
    WriteBlockInfo(file_path, a);
  }
}

void PramBlockWriter::WriteBlockInfo(const std::string& path,
                                     const std::vector<OwnedBUffer>& buffers) {
  std::ofstream file(path, std::ios::binary);
  if (!file) {
    LOG_FATAL("Error opening bin file: ", path.c_str());
  }

  file.write((char*) (buffers.at(0).Get()), buffers.at(0).GetSize());
  if (!file) {
    LOG_FATAL("Error writing meta data file: ", path.c_str());
  }
  file.write((char*) (buffers.at(1).Get()), buffers.at(1).GetSize());
  if (!file) {
    LOG_FATAL("Error writing label data file: ", path.c_str());
  }
  file.close();
}

}  // namespace sics::graph::core::io