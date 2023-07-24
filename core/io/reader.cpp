#include "reader.h"

namespace sics::graph::core::io {

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
Reader::Reader(std::string path_edgelist_global_yaml) : path_edgelist_global_yaml_(path_edgelist_global_yaml) {
  try {
    if (!read_edgelist_global_yaml()) {
        throw std::runtime_error("Error reading edgelist global YAML file: " + path_edgelist_global_yaml_);
    }
  } catch (const std::exception& e) {
    std::cerr << e.what() << std::endl;
  }
}

// judge whether to use io_adapter or not
// return true if need to adapt
bool Reader::JudgeAdapt() {
  std::string file_path = work_dir_ + "/" + CSR_GLOBLE_FILE_NAME;
  return std::filesystem::exists(file_path);
}

// read subgraph from ssd
// if enforce_adapt is true, use io_adapter to adapt edgelist to csr
void Reader::ReadSubgraph(size_t subgraph_id, bool enforce_adapt) {
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
void Reader::ReadCsr(size_t subgraph_id) {
  std::string global_file_path = work_dir_ + "/" + CSR_GLOBLE_FILE_NAME;

  // Initialize io_uring
  struct io_uring ring;
  io_uring_queue_init(&ring);

  std::string dir_path = work_dir_ + "/" + std::to_string(dir_num);
  std::string yaml_file_path = dir_path + "/" + std::to_string(dir_num) + ".yaml";
  std::string data_file_path = dir_path + "/" + std::to_string(dir_num) + "_data.bin";
  std::string attr_file_path = dir_path + "/" + std::to_string(dir_num) + "_attr.bin";

  // create temp storing buffer list
  std::list<std::list<OwnedBuffer>> buffer_list;

  // read files
  try {
    read_yaml(yaml_file_path, &buffer_list);
    read_bin_file(data_file_path, ring, &buffer_list);
    read_bin_file(attr_file_path, ring, &buffer_list);
  } catch(const std::exception& e) {
    std::cerr << e.what() << std::endl;
  }

  // Clean up io_uring
  io_uring_queue_exit(&ring);

  // Add buffer_list to serialized_
  serialized_.ReceiveBuffers(std::move(buffer_list));
}

// read yaml file
void Reader::ReadYaml(std::string yaml_file_path, std::list<list<OwnedBuffer>>* buffer_list) {
  // read yaml file yaml_file_path
  YAML::Node yaml_file = YAML::LoadFile(yaml_file_path);

  // Open the YAML file for reading
  std::ifstream yaml_file_stream(yaml_file_path);
  if (!yaml_file_stream) {
      throw std::runtime_error("Error opening yaml file: " + yaml_file_stream);
  }

  // Get the size of the YAML data
  size_t yaml_size = yaml_file.size();

  // Create an OwnedBuffer and list OwnedBuffer
  OwnedBuffer owned_buffer(yaml_size);
  std::list<OwnedBuffer> file_buffers;

  // Copy the YAML data into the OwnedBuffer's internal buffer
  owned_buffer.SetPointer(yaml_file.as<std::string>().c_str());

  // Add the Buffer list to list
  file_buffers.push_back(std::move(buffer));
  buffer_list->push_back(std::move(file_buffers));
}

// read data file
void Reader::ReadBinFile(std::string data_file_path, struct io_uring ring, std::list<list<OwnedBuffer>>* buffer_list) {
  FILE* file = fopen(data_file_path.c_str(), "rb");
  if (!file) {
    throw std::runtime_error("Error opening bin file: " + data_file_path);
  }

  // Get the file size.
  fseek(file, 0, SEEK_END);
  size_t fileSize = ftell(file);
  fseek(file, 0, SEEK_SET);

  // Create an async IO request to read the file.
  uint8_t* data = new uint8_t[fileSize];
  io_uring_sqe* sqe = io_uring_get_sqe(&ring);
  io_uring_prep_read(sqe, fileno(file), data, fileSize, 0);
  io_uring_submit(&ring);

  // Wait for the async IO request to complete.
  io_uring_cqe* cqe;
  io_uring_wait_cqe(&ring, &cqe);

  // Check if the async read was successful
  if (cqe->res < 0) {
    fclose(file);
    delete[] data;
    io_uring_cqe_seen(&ring, cqe);
    throw std::runtime_error("Error in async I/O read");
  }

  // Move the data to a OwnedBuffer object.
  Buffer OwnedBuffer(fileSize);
  OwnedBuffer.SetPointer(data);

  std::list<OwnedBuffer> file_buffers;
  file_buffers.push_back(std::move(buffer));
  buffer_list->push_back(std::move(file_buffers));

  // Clean up
  fclose(file);
  io_uring_cqe_seen(&ring, cqe);

// read edgelist global yaml file
// return true if success
// return false if failed or no work_dir_ in yaml file
bool Reader::ReadEdgelistGlobalYaml() {
  try {
    std::ifstream fin(path_edgelist_global_yaml_);
    if (!fin.is_open()) {
        return false;
    }
    YAML::Node yaml_node = YAML::Load(fin);

    // get work_dir
    if (yaml_node["work_dir"]) {
        work_dir_ = yaml_node["work_dir"].as<std::string>();
        return true;
    } else {
        return false;
    }
  } catch (const YAML::Exception& e) {
    // deal with yaml error
    std::cerr << "YAML Exception: " << e.what() << std::endl;
    return false;
  }
}

}  // namespace sics::graph::core::io

