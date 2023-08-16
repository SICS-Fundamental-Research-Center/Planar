#ifndef CORE_IO_CSR_READER_H_
#define CORE_IO_CSR_READER_H_

#include <filesystem>
#include <fstream>
#include <iostream>
#include <list>
#include <memory>
#include <string>
#include <utility>

#include <yaml-cpp/yaml.h>

#include "data_structures/buffer.h"
#include "data_structures/graph_metadata.h"
#include "data_structures/serialized.h"

namespace sics::graph::core::io {

// Class to read data from ssd to memory
// Example:
//  Reader reader;
//  SerializedImmutableCSR* serialized_immutable_csr =
//      new SerializedImmutableCSR();
//  reader.ReadSubgraph(PATH, serialized_immutable_csr);
class CSRReader {
 public:
  using GraphID = sics::graph::core::common::GraphID;
  using VertexID = sics::graph::core::common::VertexID;
  using OwnedBuffer = sics::graph::core::data_structures::OwnedBuffer;
  using Serialized = sics::graph::core::data_structures::Serialized;
  using GraphMetadata = sics::graph::core::data_structures::GraphMetadata;

  CSRReader(const std::string& root_path) : root_path_(root_path){};

  void SetGraphMetadata(const GraphMetadata& graph_metadata) {
    graph_metadata_ = graph_metadata;
  }

  // @DESCRIPTION read subgraph from ssd to Serialized object
  // @PARAMETER
  // gid: graph id,
  // dst_object: where to move ownedbuffers
  void Read(const GraphID gid, Serialized* dst_object);

 private:
  GraphMetadata graph_metadata_;
  const std::string root_path_;
};

}  // namespace sics::graph::core::io

#endif  // CORE_IO_CSR_READER_H
