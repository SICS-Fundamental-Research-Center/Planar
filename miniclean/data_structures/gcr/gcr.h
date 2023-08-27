#ifndef MINICLEAN_DATA_STRUCTURES_GCR_GCR_H_
#define MINICLEAN_DATA_STRUCTURES_GCR_GCR_H_

#include <list>
#include <memory>

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
  using PathInstanceID = sics::graph::miniclean::common::PathInstanceID;

 public:
  // Two GCRs are joinable if
  //   (1) Their labels of star centers are the same.
  //   (2) The composed dual pattern would not carry more than `n` predicates.
  //       (n is the maximum number of predicates allowed in a GCR.)
  void IsJoinableWith(const GCR& gcr) const;

  // Compose two GCRs into a new GCR.
  // To compose:
  //    GCR1[x0, y0](X1 -> p1) and
  //    GCR2[x0, y0](X2 -> p2)
  // We first compose their dual patterns (only star centers are shared).
  // Then we merge the preconditions of two GCR and get:
  //    GCR3[x0, y0]({X1, X2} -> p1) and
  //    GCR4[x0, y0]({X1, X2} -> p2)
  GCR ComposeWith(const GCR& gcr) const;

  // Compute and update the local support (Match) and local match (Verify).
  // If the local support is lower than the threshold, the GCR is pruned.
  // In this function:
  //   (1) We first match instances that satisfy the dual pattern and
  //       preconditions and update the local support. If the local support is
  //       lower than the threshold, the GCR is pruned, and `false` is returned.
  //   (2) Then we verify whether the instances satisfy the conquences and
  //       update the local match. Finally `true` is returned.
  bool MatchAndVerify();

 public:
  DualPattern dual_pattern;
  std::list<GCRPredicate> preconditions;
  GCRPredicate conquences;

  size_t local_support;
  size_t local_match;

  // Each GCR maintains a list of left star instances and right star instances.
  // Each star instance is a list of path instances.
  std::list<std::list<PathInstanceID>> left_star_instances_;
  std::list<std::list<PathInstanceID>> right_star_instances_;

};

}  // namespace sics::graph::miniclean::data_structures::gcr

#endif  // MINICLEAN_DATA_STRUCTURES_GCR_GCR_H_