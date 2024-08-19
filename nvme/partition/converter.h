#ifndef GRAPH_SYSTEMS_CONVERTER_H
#define GRAPH_SYSTEMS_CONVERTER_H

#include <filesystem>
#include <fstream>
#include <iostream>
#include <string>
#include <vector>

#include "core/common/types.h"

namespace sics::graph::nvme::partition {

using sics::graph::core::common::BlockID;
using sics::graph::core::common::EdgeIndex;
using sics::graph::core::common::GraphID;
using sics::graph::core::common::VertexID;

namespace fs = std::filesystem;

void ConvertEdgelistToBin() {}

}

#endif  // GRAPH_SYSTEMS_CONVERTER_H
