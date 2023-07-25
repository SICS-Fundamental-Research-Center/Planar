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
  Writer() {}

  void WriteSubgraph(size_t subgraph_id);

  void WriteCsr(size_t subgraph_id);

  void WriteYaml(std::string yaml_file_path);

  void WriteBinFile(std::string data_file_path);

 protected:
  std::string path_edgelist_global_yaml_;
  std::string work_dir_;
};
}  // namespace sics::graph::core::io

#endif  // CORE_IO_WRITER_H_
