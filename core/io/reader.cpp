#include "io/reader.h"
#include <filesystem>
#include <fstream>
#include <iostream>

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
Reader::Reader(std::string path_edgelist_global_yaml)
    : path_edgelist_global_yaml_(path_edgelist_global_yaml) {
  try {
    if (!ReadEdgelistGlobalYaml()) {
      throw std::runtime_error("Error reading edgelist global YAML file: " +
                               path_edgelist_global_yaml_);
    }
  } catch (const std::exception& e) {
    std::cerr << e.what() << std::endl;
    exit(EXIT_FAILURE);
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
  if (enforce_adapt || JudgeAdapt()) {
    // TODO(zhj): use io_adapter to adapt edgelist to csr
  }
  ReadCsr(subgraph_id);
}

// read csr of a certain subgraph from ssd
// workdir structure:dx
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

  std::string dir_path = work_dir_ + "/" + std::to_string(subgraph_id);
  std::string yaml_file_path =
      dir_path + "/" + std::to_string(subgraph_id) + ".yaml";
  std::string data_file_path =
      dir_path + "/" + std::to_string(subgraph_id) + "_data.bin";
  std::string attr_file_path =
      dir_path + "/" + std::to_string(subgraph_id) + "_attr.bin";

  // read files
  try {
    // ReadYaml(yaml_file_path);
    ReadBinFile(data_file_path);
    // ReadBinFile(attr_file_path);
  } catch (const std::exception& e) {
    std::cerr << e.what() << std::endl;
    exit(EXIT_FAILURE);
  }
}

// read yaml file
// not used
void Reader::ReadYaml(std::string yaml_file_path) {
  // read yaml file yaml_file_path
  YAML::Node yaml_file = YAML::LoadFile(yaml_file_path);

  // Convert the YAML data to a binary representation (assuming you want to
  // serialize it)
  std::stringstream serialized_yaml;
  serialized_yaml << yaml_file;

  // Get the size of the serialized YAML data
  size_t yaml_size = serialized_yaml.str().size();
  uint8_t* data = new uint8_t[yaml_size];

  // Create an OwnedBuffer and list OwnedBuffer
  // OwnedBuffer owned_buffer(yaml_size);
  std::list<OwnedBuffer> file_buffers;
  file_buffers.emplace_back(yaml_size);
  file_buffers.back().SetPointer(static_cast<uint8_t*>(
      static_cast<void*>(const_cast<char*>(serialized_yaml.str().c_str()))));

  serialized_->ReceiveBuffers(std::move(file_buffers));
  // LOG_INFO("in function ReadYaml");
}

// read data file
void Reader::ReadBinFile(std::string data_file_path) {
  FILE* file = fopen(data_file_path.c_str(), "rb");
  if (!file) {
    throw std::runtime_error("Error opening bin file: " + data_file_path);
  }

  // Get the file size.
  fseek(file, 0, SEEK_END);
  size_t fileSize = ftell(file);
  fseek(file, 0, SEEK_SET);

  // Allocate memory to store file data.
  uint8_t* data = new uint8_t[fileSize];

  // Read the file data.
  size_t bytesRead = fread(data, 1, fileSize, file);
  if (bytesRead != fileSize) {
    fclose(file);
    delete[] data;
    throw std::runtime_error("Error reading file: " + data_file_path);
  }

  // Move the data to a OwnedBuffer object.
  OwnedBuffer owned_buffer(fileSize);
  owned_buffer.SetPointer(data);

  std::list<OwnedBuffer> file_buffers;
  file_buffers.push_back(std::move(owned_buffer));
  serialized_->ReceiveBuffers(std::move(file_buffers));

  // Clean up
  fclose(file);
}

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
