#ifndef CORE_IO_WRITER_H_
#define CORE_IO_WRITER_H_

#include <list>
#include <memory>
#include <string>
#include <utility>
#include <filesystem>
#include <fstream>
#include <iostream>

#include "data_structures/buffer.h"
#include "data_structures/serialized.h"
#include "yaml-cpp/yaml.h"

namespace sics::graph::core::io {

class Writer {
 public:
  using OwnedBuffer = sics::graph::core::data_structures::OwnedBuffer;
  using Serialized = sics::graph::core::data_structures::Serialized;

  Writer() {}

  // Write the subgraph to disk
  // path: path to the subgraph dictionary
  // src_object: where to get ownedbuffers
  // read type: 0: csr, other: other type in future version
  void WriteSubgraph(const std::string& path, Serialized* src_object, int write_type = 0);

 private:
  // write csr of a certain subgraph to ssd
  void WriteCSR(const std::string& path, Serialized* src_object);

  // write data file
  void WriteBinFile(const std::string& path, Serialized* src_object);
};
}  // namespace sics::graph::core::io

#endif  // CORE_IO_WRITER_H_
