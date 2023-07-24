#ifndef CORE_IO_READER_H_
#define CORE_IO_READER_H_

#include <liburing.h>  // TODO(zhj): add io_uring
#include <yaml-cpp/yaml.h>  // TODO(zhj): add yaml-cpp
#include <folly/concurrency/ConcurrentQueue.h>  // TODO(zhj): add folly

#include <string>
#include <list>
#include <utility>

#include "data_structures/buffer.h"
#include "data_structures/serialized.h"

#define CSR_GLOBLE_FILE_NAME "csr_global.yaml"

using sics::graph::core::data_structures::OwnedBuffer OwnedBuffer;
using sics::graph::core::data_structures::Serialized Serialized;

namespace sics::graph::core::io {

class Reader {
 public:
  explicit Reader(std::string path_edgelist_global_yaml);

  bool JudgeAdapt();

  void ReadSubgraph(size_t subgraph_id, bool enforce_adapt = false);

  Serialized* GetSerialized() {
    return &serialized_;
  }

 private:
  void ReadCsr(size_t subgraph_id);

  void ReadYaml(std::string yaml_file_path, std::list<list<OwnedBuffer>>* buffer_list);

  void ReadBinFile(std::string data_file_path, struct io_uring ring, std::list<list<OwnedBuffer>>* buffer_list);

  bool ReadEdgelistGlobalYaml();

 protected:
  std::string path_edgelist_global_yaml_;
  std::string work_dir_;
  Serialized serialized_;
};

}  // namespace sics::graph::core::io

#endif  // CORE_IO_READER_H_
