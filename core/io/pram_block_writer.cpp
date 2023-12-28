#include "io/pram_block_writer.h"

namespace sics::graph::core::io {

void PramBlockWriter::Write(WriteMessage* message,
                            common::TaskRunner* /* runner */) {
  std::string file_path =
      root_path_ + std::to_string(message->graph_id) + ".bin.new";
}

void PramBlockWriter::WriteBlockInfo(const std::string& path,
                                     const std::vector<OwnedBUffer>& buffers) {}

}  // namespace sics::graph::core::io