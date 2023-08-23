#include "io/csr_writer.h"

namespace sics::graph::core::io {

void CSRWriter::Write(WriteMessage* message, common::TaskRunner* runner) {
  std::string file_path =
      root_path_ + "graphs/" + std::to_string(message->graph_id) + ".bin";
  std::string label_path =
      root_path_ + "graphs/" + std::to_string(message->graph_id) + "label.bin";

  if (message->serialized->HasNext()) {
    auto a = message->serialized->PopNext();
    WriteToBin(file_path, message->serialized);
  }

  if (message->serialized->HasNext()) {
    WriteToBin(label_path, message->serialized);
  }
}

}  // namespace sics::graph::core::io