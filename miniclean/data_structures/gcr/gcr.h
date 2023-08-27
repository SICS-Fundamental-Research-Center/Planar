#ifndef MINICLEAN_DATA_STRUCTURES_GCR_GCR_H_
#define MINICLEAN_DATA_STRUCTURES_GCR_GCR_H_

#include <memory>
#include <vector>

#include "core/common/multithreading/task_runner.h"
#include "miniclean/common/types.h"
#include "miniclean/data_structures/gcr/predicate.h"

namespace sics::graph::miniclean::data_structures::gcr {

class GCR {
 private:
  using DualPattern = sics::graph::miniclean::common::DualPattern;
  using GCRPredicate =
      sics::graph::miniclean::data_structures::gcr::GCRPredicate;
  using TaskRunner = sics::graph::core::common::TaskRunner;
  using VertexID = sics::graph::miniclean::common::VertexID;

 public:
  // Two GCRs are joinable if
  //   (1) Their labels of star centers are the same.
  //   (2) The composed dual pattern would not carry more than `n` predicates.
  //       (n is the maximum number of predicates allowed in a GCR.)
  void Joinable(const GCR& gcr);

  // Compose two GCRs into a new GCR.
  // To compose:
  //    GCR1[x0, y0](X1 -> p1) and
  //    GCR2[x0, y0](X2 -> p2)
  // We first compose their dual patterns (only star centers are shared).
  // Then we merge the preconditions of two GCR and get:
  //    GCR3[x0, y0]({X1, X2} -> p1) and
  //    GCR4[x0, y0]({X1, X2} -> p2)
  void Compose(const GCR& gcr);

  // Compute the local support and local match of the GCR.
  // If support is local than the threshold, the GCR would be pruned.
  void Verify();

 public:
  DualPattern dual_pattern;
  std::vector<GCRPredicate> preconditions;
  std::vector<GCRPredicate> conquences;

  size_t local_support;
  size_t local_match;

  std::vector<VertexID> activated_left_centers;
  std::vector<VertexID> activated_right_centers;
};

}  // namespace sics::graph::miniclean::data_structures::gcr

#endif  // MINICLEAN_DATA_STRUCTURES_GCR_GCR_H_