#ifndef CORE_IO_READER_H_
#define CORE_IO_READER_H_

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

#define CSR_GLOBLE_FILE_NAME "csr_global.yaml"

using sics::graph::core::data_structures::OwnedBuffer;
using sics::graph::core::data_structures::Serialized;

namespace sics::graph::core::io {

// Class to read data from ssd to memory
class Reader {
 public:
  // read subgraph from ssd to Serialized object
  // path: path to the subgraph dictionary
  // dst_object: where to move ownedbuffers
  // read type: 0: csr, other: other type in future version
  void ReadSubgraph(const std::string& path, Serialized* dst_object, int read_type = 0);

 public:
  // read csr of a certain subgraph from ssd
  // workdir structure:
  //  - dir:{work_dir_}
  //    - dir:0
  //      - file:0.yaml
  //      - file:0_data.bin
  //      - file:0_attr.bin
  //    - dir:1
  //      - file:1.yaml
  //      - file:1_data.bin
  //      - file:1_attr.bin
  //    - file:csr_global.yaml
  void ReadCsr(const std::string& path, Serialized* dst_object);

  // read data file
  void ReadBinFile(std::string data_file_path, Serialized* dst_object);

 protected:
  std::string path_edgelist_global_yaml_;
  std::string work_dir_;
};

}  // namespace sics::graph::core::io

#endif  // CORE_IO_READER_H_
