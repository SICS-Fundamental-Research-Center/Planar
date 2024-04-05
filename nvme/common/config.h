#ifndef GRAPH_SYSTEMS_NVME_COMMON_CONFIG_H_
#define GRAPH_SYSTEMS_NVME_COMMON_CONFIG_H_

#include <chrono>
#include <string>

namespace sics::graph::nvme::common {

std::chrono::time_point<std::chrono::system_clock> start_time_in_memory;
std::chrono::time_point<std::chrono::system_clock> end_time_in_memory;

}  // namespace sics::graph::nvme::common

#endif  // GRAPH_SYSTEMS_NVME_COMMON_CONFIG_H_
