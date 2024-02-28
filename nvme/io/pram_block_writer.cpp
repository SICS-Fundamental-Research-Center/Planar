#include "nvme/io/pram_block_writer.h"

namespace sics::graph::nvme::io {

void PramBlockWriter::Write(WriteMessage* message,
                            core::common::TaskRunner* /* runner */) {
  std::string file_path = "";
  if (message->changed) {
    file_path =
        root_path_ + "blocks/" + std::to_string(message->graph_id) + ".bin.new";
    if (message->serialized->HasNext()) {
      auto a = message->serialized->PopNext();
      WriteBlockInfo(file_path, a);
    }
  } else {
    // if not changed, just release the OwnedBuffer and return.
    if (message->serialized->HasNext()) {
      message->serialized->PopNext();
    }
  }
}

void PramBlockWriter::WriteBlockInfo(const std::string& path,
                                     const std::vector<OwnedBUffer>& buffers) {
  std::ofstream file(path, std::ios::binary);
  if (!file) {
    LOG_FATAL("Error opening bin file: ", path.c_str());
  }

  file.write((char*)(buffers.at(0).Get()), buffers.at(0).GetSize());
  if (!file) {
    LOG_FATAL("Error writing meta data file: ", path.c_str());
  }
  file.write((char*)(buffers.at(1).Get()), buffers.at(1).GetSize());
  if (!file) {
    LOG_FATAL("Error writing label data file: ", path.c_str());
  }
  file.close();
}

}  // namespace sics::graph::nvme::io