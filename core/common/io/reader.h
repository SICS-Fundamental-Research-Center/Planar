#ifndef CORE_COMMON_IO_READER_H_
#define CORE_COMMON_IO_READER_H_

#include <string>
#include <list>
#include <utility>

#include <liburing.h>  // TODO(zhj): add io_uring
#include <folly/concurrency/ConcurrentQueue.h>  // TODO(zhj): add folly

#include "data_structures/buffer.h"
#include "data_structures/serializable.h"
#include "yaml-cpp/yaml.h" // TODO(zhj): add yaml-cpp

#define CSR_GLOBLE_FILE_NAME "csr_global.yaml"

// TODO(zhj): maybe need to be deleted
#typedef sics::graph::core::data_structures::Buffer Buffer;
#typedef sics::graph::core::data_structures::Serialized Serialized;

namespace sics::graph::core::common::io {

// Class to read data from ssd to memory.
// First read config file of edgelist and judge
// whether it needs to be adapted into csr with io_adapter.
// If not, read data directly from ssd to memory.
// If yes, read data from ssd to memory with io_adapter
// and save yaml and bin files of csr.
// Edgelist config file can be anywhere and its format:
//  workdir: path of working dictionary
//  edgelist: files of edgelist
// TODO(zhj): don't know format details yet
// Example:
//  Reader reader("path.yaml");
class Reader {
 public:
  explicit Reader(std::string path_edgelist_global_yaml) : path_edgelist_global_yaml_(path_edgelist_global_yaml) {}

  // judge whether to use io_adapter or not
  // return true if need to adapt
  bool judge_adapt() {
    // TODO(zhj): look in work_dir_ for csr_global.yaml
    return true;
  }

  // read subgraph from ssd
  // if enforce_adapt is true, use io_adapter to adapt edgelist to csr
  void read_subgraph(size_t subgraph_id, bool enforce_adapt = false) {
    // TODO(zhj): read yaml file path_config_ and get workdir

    if (enforce_adapt || judge_adapt()) {
      // TODO(zhj): use io_adapter to adapt edgelist to csr
    }

    read_csr(subgraph_id);
  }

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
  void read_csr(size_t subgraph_id) {
    std::string global_file_path = work_dir_ + "/" + CSR_GLOBLE_FILE_NAME;

    // Initialize io_uring
    struct io_uring ring;
    io_uring_queue_init(/*...*/);  // TODO(zhj): add params

    std::string dir_path = work_dir_ + "/" + std::to_string(dir_num);
    std::string yaml_file_path = dir_path + "/" + std::to_string(dir_num) + ".yaml";
    std::string data_file_path = dir_path + "/" + std::to_string(dir_num) + "_data.bin";
    std::string attr_file_path = dir_path + "/" + std::to_string(dir_num) + "_attr.bin";

    // create temp storing buffer list
    std::list<std::list<Buffer>> buffer_list;

    // read yaml file yaml_file_path
    read_yaml(yaml_file_path, &buffer_list);
    // read data file
    read_bin_file(data_file_path, ring, &buffer_list);
    // read attr file
    read_bin_file(attr_file_path, ring, &buffer_list);

    // Clean up io_uring
    io_uring_queue_exit(/*...*/);  // TODO(zhj): add params

    // Add buffer_list to serialized_
    serialized_.ReceiveBuffers(std::move(buffer_list));
  }

  // read yaml file
  void read_yaml(std::string yaml_file_path, std::list<list<Buffer>>* buffer_list) {
    // read yaml file yaml_file_path
    YAML::Node yaml_file = YAML::LoadFile(yaml_file_path);

    // TODO(zhj): read yaml into buffer

    // Add the Buffer list to list
    buffer_list->push(std::move(file_buffers));
  }

  // read data file
  void read_bin_file(std::string data_file_path, struct io_uring ring, std::list<list<Buffer>>* buffer_list) {
    // TODO(zhj): recheck this code and answer the question "is io_uring actually speeding up the whole io?"
    FILE* file = fopen(data_file_path.c_str(), "rb");
    if (!file) {
      // Handle error...
      return;
    }

    // Get the file size.
    fseek(file, 0, SEEK_END);
    size_t fileSize = ftell(file);
    fseek(file, 0, SEEK_SET);

    uint8_t* data = new uint8_t[fileSize];

    // Create an async IO request to read the file.
    io_uring_sqe* sqe = io_uring_get_sqe(&ring);
    io_uring_prep_read(sqe, fileno(file), data, fileSize, 0);
    io_uring_submit(&ring);

    // Wait for the async IO request to complete.
    io_uring_wait_cqe(&ring, &cqe);

    // Move the data to a Buffer object.
    Buffer buffer(data, fileSize);

    std::list<Buffer> file_buffers;
    file_buffers.push_back(std::move(buffer));

    // Add the Buffer list to the list
    buffer_list->push(std::move(file_buffers));

    // Clean up
    fclose(file);
    delete[] data;
  }

 protected:
  std::string path_edgelist_global_yaml_;
  std::string work_dir_;
  Serialized serialized_;
};

}  // namespace sics::graph::core::common::io

#endif  // CORE_COMMON_IO_READER_H_



