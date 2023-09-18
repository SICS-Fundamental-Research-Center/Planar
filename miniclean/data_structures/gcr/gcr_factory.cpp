#include "miniclean/data_structures/gcr/gcr_factory.h"

namespace sics::graph::miniclean::data_structures::gcr {

using PathRule = sics::graph::miniclean::data_structures::gcr::PathRule;
using GCR = sics::graph::miniclean::data_structures::gcr::refactor::GCR;

std::vector<GCR>& GCRFactory::InitializeGCRs(const GCR& gcr) {
  // TODO
}

std::vector<GCR>& GCRFactory::MergeAndCompleteGCRs(const GCR& gcr,
                                                   const PathRule& path_rule) {
  // TODO
}

}  // namespace sics::graph::miniclean::data_structures::gcr