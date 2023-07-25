#ifndef CORE_IO_READER_H_
#define CORE_IO_READER_H_

#include "data_structures/buffer.h"
#include "data_structures/serialized.h"
#include "yaml-cpp/yaml.h"
#include <list>
#include <memory>
#include <string>
#include <utility>

#define CSR_GLOBLE_FILE_NAME "csr_global.yaml"

using sics::graph::core::data_structures::OwnedBuffer;
using sics::graph::core::data_structures::Serialized;

namespace sics::graph::core::io {

class Reader {
 public:
  explicit Reader(std::string path_edgelist_global_yaml);

  bool JudgeAdapt();

  void ReadSubgraph(size_t subgraph_id, bool enforce_adapt = false);

  Serialized* GetSerialized() { return serialized_.get(); }

  void SetPointer(Serialized* p) {
    serialized_ = std::unique_ptr<Serialized>(p);
  }

 public:
  void ReadCsr(size_t subgraph_id);

  void ReadYaml(std::string yaml_file_path);

  void ReadBinFile(std::string data_file_path);

  bool ReadEdgelistGlobalYaml();

 protected:
  std::string path_edgelist_global_yaml_;
  std::string work_dir_;
  std::unique_ptr<Serialized> serialized_;
};

}  // namespace sics::graph::core::io

#endif  // CORE_IO_READER_H_
