#ifndef MINICLEAN_IO_MINICLEAN_GRAPH_READER_H_
#define MINICLEAN_IO_MINICLEAN_GRAPH_READER_H_

#include <yaml-cpp/yaml.h>

#include <filesystem>
#include <fstream>
#include <string>

#include "core/common/multithreading/task_runner.h"
#include "core/data_structures/buffer.h"
#include "core/data_structures/serialized.h"
#include "core/io/reader_writer.h"
#include "core/scheduler/message.h"
#include "core/util/logging.h"
#include "miniclean/common/types.h"
#include "miniclean/data_structures/graphs/miniclean_graph_metadata.h"

namespace sics::graph::miniclean::io {

class MiniCleanGraphReader : public sics::graph::core::io::Reader {
 private:
  using ReadMessage = sics::graph::core::scheduler::ReadMessage;
  using TaskRunner = sics::graph::core::common::TaskRunner;
  using Serialized = sics::graph::core::data_structures::Serialized;
  using OwnedBuffer = sics::graph::core::data_structures::OwnedBuffer;
  using MiniCleanGraphMetadata =
      sics::graph::miniclean::data_structures::graphs::MiniCleanGraphMetadata;
  using GraphID = sics::graph::miniclean::common::GraphID;

 public:
  explicit MiniCleanGraphReader(const std::string& root_path)
      : root_path_(root_path), read_size_mb_(0) {};

  void Read(ReadMessage* message, TaskRunner* runner = nullptr) override;

  size_t SizeOfReadNow() override { return read_size_mb_; }

 private:
  void LoadBinFileAsBuffer(const std::string& bin_path,
                           Serialized* serialized_object);

  const std::string root_path_;
  size_t read_size_mb_;
};
}  // namespace sics::graph::miniclean::io

#endif  // MINICLEAN_IO_MINICLEAN_GRAPH_READER_H_