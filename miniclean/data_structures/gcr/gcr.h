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
  GCR() = default;
  GCR(const DualPattern& dual_pattern) : dual_pattern_(dual_pattern) {}
  GCR(const GCR& other_gcr)
      : dual_pattern_(other_gcr.get_dual_pattern()),
        preconditions_(other_gcr.get_preconditions()),
        consequence_(other_gcr.get_consequence()),
        local_support(other_gcr.get_local_support()),
        local_match(other_gcr.get_local_match()),
        left_star_instances_(other_gcr.get_left_star_instances()),
        right_star_instances_(other_gcr.get_right_star_instances()) {}

  // Two GCRs are joinable if
  //   (1) Their labels of star centers are the same.
  //   (2) The composed dual pattern would not carry more than `n` predicates.
  //       (n is the maximum number of predicates allowed in a GCR.)
  bool IsJoinableWith(const GCR& gcr) const;

  // Compose two GCRs into a new GCR.
  // To compose:
  //    GCR1[x0, y0](X1 -> p1) and
  //    GCR2[x0, y0](X2 -> p2)
  // We first compose their dual patterns (only star centers are shared).
  // Then we merge the preconditions of two GCR and get:
  //    GCR3[x0, y0]({X1, X2} -> p1) and
  //    GCR4[x0, y0]({X1, X2} -> p2)
  GCR ComposeWith(const GCR& gcr) const;

  // Determine whether this GCR should expand to next level by verifying its
  // local support and local match. If the local support is lower than the
  // threshold, the GCR is pruned.
  // In this function:
  //   (1) We first match instances that satisfy the dual pattern and
  //       preconditions and update the local support. If the local support is
  //       lower than the threshold, the GCR is pruned, and `false` is returned.
  //   (2) Then we verify whether the instances satisfy the conquences and
  //       update the local match. Finally `true` is returned.
  bool ShouldExpandByVerification();

  DualPattern get_dual_pattern() const { return dual_pattern_; }
  std::list<GCRPredicate*> get_preconditions() const { return preconditions_; }
  GCRPredicate* get_consequence() const { return consequence_; }

  size_t get_local_support() const { return local_support; }
  size_t get_local_match() const { return local_match; }

  std::list<std::list<PathInstanceID>> get_left_star_instances() const {
    return left_star_instances_;
  }

  std::list<std::list<PathInstanceID>> get_right_star_instances() const {
    return right_star_instances_;
  }

  void set_dual_pattern(const DualPattern& dual_pattern) {
    dual_pattern_ = dual_pattern;
  }

  void set_preconditions(const std::list<GCRPredicate*>& preconditions) {
    preconditions_ = preconditions;
  }

  void set_consequence(GCRPredicate* consequence) {
    consequence_ = consequence;
  }

  void set_local_support(size_t local_support) {
    local_support = local_support;
  }

  void set_local_match(size_t local_match) { local_match = local_match; }

  void AddPreconditionToBack(GCRPredicate* precondition) {
    preconditions_.push_back(precondition);
  }

  void RemoveLastPrecondition() { preconditions_.pop_back(); }

  void AddLeftStarInstanceToBack(
      const std::list<PathInstanceID>& left_star_instance) {
    left_star_instances_.push_back(left_star_instance);
  }

  void AddRightStarInstanceToBack(
      const std::list<PathInstanceID>& right_star_instance) {
    right_star_instances_.push_back(right_star_instance);
  }

 private:
  DualPattern dual_pattern_;
  std::list<GCRPredicate*> preconditions_;
  GCRPredicate* consequence_;

  size_t local_support;
  size_t local_match;

  // Each GCR maintains a list of left star instances and right star instances.
  // Each star instance is a list of path instances.
  std::list<std::list<PathInstanceID>> left_star_instances_;
  std::list<std::list<PathInstanceID>> right_star_instances_;
};

}  // namespace sics::graph::miniclean::data_structures::gcr

#endif  // MINICLEAN_DATA_STRUCTURES_GCR_GCR_H_