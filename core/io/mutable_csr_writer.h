#ifndef CORE_IO_MUTABLE_CSR_WRITER_H_
#define CORE_IO_MUTABLE_CSR_WRITER_H_

#include <fstream>
#include <list>
#include <memory>
#include <string>
#include <utility>

#include "common/config.h"
#include "common/types.h"
#include "data_structures/buffer.h"
#include "data_structures/graph_metadata.h"
#include "data_structures/serialized.h"
#include "io/reader_writer.h"
#include "scheduler/message.h"

namespace sics::graph::core::io {

class MutableCSRWriter : public Writer {
 private:
  using OwnedBUffer = data_structures::OwnedBuffer;
  using Serialized = data_structures::Serialized;

 public:
  MutableCSRWriter(const std::string& root_path) : root_path_(root_path) {}

  void Write(WriteMessage* message,
             common::TaskRunner* runner = nullptr) override;

 private:
  void WriteMetaInfoToBin(const std::string& path,
                          const std::vector<OwnedBUffer>& buffers);

  void WriteLabelInfoToBin(const std::string& path,
                           const std::vector<OwnedBUffer>& buffers);

 private:
  const std::string root_path_;
};

}  // namespace sics::graph::core::io

#endif  // CORE_IO_MUTABLE_CSR_WRITER_H_
