#ifndef CORE_IO_READER_H_
#define CORE_IO_READER_H_

#include <string>
#include <list>
#include <utility>

#include <liburing.h>  // TODO(zhj): add io_uring
#include <folly/concurrency/ConcurrentQueue.h>  // TODO(zhj): add folly
#include <yaml-cpp/yaml.h>  // TODO(zhj): add yaml-cpp

#include "data_structures/buffer.h"
#include "data_structures/serializable.h"

#define CSR_GLOBLE_FILE_NAME "csr_global.yaml"

// TODO(zhj): maybe need to be deleted
using sics::graph::core::data_structures::Buffer Buffer;
using sics::graph::core::data_structures::Serialized Serialized;

namespace sics::graph::core::io {

class Reader {
 public:
  explicit Reader(std::string path_edgelist_global_yaml);

  bool judge_adapt();

  void read_subgraph(size_t subgraph_id, bool enforce_adapt = false);

  void read_csr(size_t subgraph_id);

 private:
  bool read_yaml(std::string yaml_file_path, std::list<list<Buffer>>* buffer_list);

  void read_bin_file(std::string data_file_path, struct io_uring ring, std::list<list<Buffer>>* buffer_list);

  bool read_edgelist_global_yaml();

 protected:
  std::string path_edgelist_global_yaml_;
  std::string work_dir_;
  Serialized serialized_;
};

}  // namespace sics::graph::core::io

#endif  // CORE_IO_READER_H_
