#ifndef xyz_GRAPH_SYSTEMS_TOOLS_PARTITIONER_H_
#define xyz_GRAPH_SYSTEMS_TOOLS_PARTITIONER_H_

#include "tools/common/types.h"

#include <string>

namespace xyz::graph::tools::partitioner {

// This class defines the Partitioner interface, which is used to partition a
// graph into several subgraphs.
class PartitionerBase {
 private:
  using StoreStrategy = xyz::graph::tools::common::StoreStrategy;

 public:
  PartitionerBase(const std::string& input_path,
                  const std::string& output_path,
                  StoreStrategy store_strategy)
      : input_path_(input_path),
        output_path_(output_path),
        store_strategy_(store_strategy) {}

  // This function will submit the partitioning task.
  virtual void RunPartitioner() = 0;

 protected:
  const std::string input_path_;
  const std::string output_path_;
  const StoreStrategy store_strategy_;
};

}  // namespace xyz::graph::tools::partitioner

#endif  // xyz_GRAPH_SYSTEMS_TOOLS_PARTITIONER_H_
