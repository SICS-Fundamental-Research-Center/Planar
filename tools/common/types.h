#ifndef SICS_GRAPH_SYSTEMS_TOOLS_TOOLS_COMMON_TYPES_H_
#define SICS_GRAPH_SYSTEMS_TOOLS_TOOLS_COMMON_TYPES_H_

#include <string>

namespace sics::graph::tools::common {

enum StoreStrategy {
  kUnconstrained,  // default
  kIncomingOnly,
  kOutgoingOnly,
  kUndefinedStrategy
};

static inline StoreStrategy StoreStrategy2Enum(const std::string& s) {
  if (s == "incoming_only")
    return kIncomingOnly;
  else if (s == "outgoing_only")
    return kOutgoingOnly;
  else if (s == "unconstrained")
    return kUnconstrained;
  return kUndefinedStrategy;
};

enum ConvertMode {
  kEdgelistCSV2EdgelistBin,
  kEdgelistCSV2CSRBin,
  kEdgelistBin2CSRBin,
  kUndefinedMode
};

static inline ConvertMode ConvertMode2Enum(const std::string& s) {
  if (s == "edgelistcsv2edgelistbin")
    return kEdgelistCSV2EdgelistBin;
  else if (s == "edgelistbin2csrbin")
    return kEdgelistBin2CSRBin;
  else if (s == "edgelistcsv2bin")
    return kEdgelistCSV2CSRBin;
  return kUndefinedMode;
};

}  // namespace tools

#endif  // SICS_GRAPH_SYSTEMS_TOOLS_TOOLS_COMMON_TYPES_H_
