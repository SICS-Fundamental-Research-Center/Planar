#ifndef GRAPH_SYSTEMS_CORE_IO_PRAM_BLOCK_WRITER_H_
#define GRAPH_SYSTEMS_CORE_IO_PRAM_BLOCK_WRITER_H_

#include <fstream>
#include <list>
#include <memory>
#include <string>
#include <utility>

#include "core/common/config.h"
#include "core/common/types.h"
#include "core/data_structures/buffer.h"
#include "core/data_structures/graph_metadata.h"
#include "core/data_structures/serialized.h"
#include "nvme/io/reader_writer.h"
#include "nvme/scheduler/message.h"

namespace sics::graph::nvme::io {

class PramBlockWriter : public Writer {
 private:
  using OwnedBUffer = core::data_structures::OwnedBuffer;
  using Serialized = core::data_structures::Serialized;

 public:
  explicit PramBlockWriter(const std::string& root_path)
      : root_path_(root_path) {}

  void Write(WriteMessage* message,
             core::common::TaskRunner* runner = nullptr) override;

 private:
  void WriteBlockInfo(const std::string& path,
                      const std::vector<OwnedBUffer>& buffers);

 private:
  const std::string root_path_;
  size_t write_size_ = 0;
};

}  // namespace sics::graph::nvme::io

#endif  // GRAPH_SYSTEMS_CORE_IO_PRAM_BLOCK_WRITER_H_
