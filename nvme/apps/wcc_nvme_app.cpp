#include "nvme/apps/wcc_nvme_app.h"

namespace sics::graph::nvme::apps {

DEFINE_string(i, "/testfile", "graph files root path");
DEFINE_uint32(p, 1, "parallelism");
DEFINE_bool(in_memory, false, "in memory mode");
DEFINE_uint32(memory_size, 64, "memory size (GB)");
DEFINE_uint32(limits, 0, "subgrah limits for pre read");
DEFINE_bool(no_short_cut, false, "no short cut");

int main(int argc, char** argv) {
  gflags::ParseCommandLineFlags(&argc, &argv, true);

  core::common::Configurations::GetMutable()->root_path = FLAGS_i;
  core::common::Configurations::GetMutable()->parallelism = FLAGS_p;
  core::common::Configurations::GetMutable()->in_memory = FLAGS_in_memory;
  core::common::Configurations::GetMutable()->limits = FLAGS_limits;
  // wcc nvme specific configurations
  core::common::Configurations::GetMutable()->is_block_mode = true;

  WCCNvmeApp app;
  app.Compute();
  return 0;
}

}  // namespace sics::graph::nvme::apps