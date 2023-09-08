#include "tools/common/data_structures.h"

#include <cstddef>   // For std::ptrdiff_t
#include <iterator>  // For std::forward_iterator_tag

#include "core/common/bitmap.h"
#include "core/common/multithreading/thread_pool.h"
#include "core/util/atomic.h"
#include "core/util/logging.h"

namespace sics::graph::tools::common {

using sics::graph::core::common::Bitmap;
using sics::graph::core::common::GraphID;
using sics::graph::core::common::TaskPackage;
using sics::graph::core::common::VertexID;
using sics::graph::core::common::VertexLabel;
using sics::graph::core::util::atomic::WriteAdd;
using sics::graph::core::util::atomic::WriteMax;
using sics::graph::core::util::atomic::WriteMin;
using Bitmap = sics::graph::core::common::Bitmap;
using TaskPackage = sics::graph::core::common::TaskPackage;

void Edges::SortBySrc() {
  std::sort(std::begin(*this), std::end(*this),
            [](const Edge& l, const Edge& r) {
              if (l.src != r.src) {
                return l.src < r.src;
              }
              if (l.dst != r.dst) {
                return l.dst < r.dst;
              }
            });
}

void Edges::SortByDst() {
  std::sort(std::begin(*this), std::end(*this),
            [](const Edge& l, const Edge& r) {
              if (l.dst != r.dst) {
                return l.dst < r.dst;
              }
              if (l.src != r.src) {
                return l.src < r.src;
              }
            });
}

VertexID Edges::GetVertexWithMaximumDegree() {
  auto parallelism = std::thread::hardware_concurrency();
  auto thread_pool = sics::graph::core::common::ThreadPool(parallelism);
  auto task_package = TaskPackage();
  std::mutex mtx;

  auto aligned_max_vid = ((edgelist_metadata_.max_vid >> 6) << 6) + 64;
  VertexID outdegree_by_vid[aligned_max_vid] = {0};
  VertexID max_outdegree = 0, vid_with_maximum_degree = 0;

  for (unsigned int i = 0; i < parallelism; i++) {
    auto task = std::bind([&, i]() {
      for (VertexID j = i; j < edgelist_metadata_.num_edges; j += parallelism) {
        auto src = get_src_by_index(j);
        sics::graph::core::util::atomic::WriteAdd(outdegree_by_vid + src,
                                                  (VertexID)1);
        std::lock_guard<std::mutex> lck(mtx);
        if (sics::graph::core::util::atomic::WriteMax(&max_outdegree,
                                                      outdegree_by_vid[src])) {
          vid_with_maximum_degree = src;
        }
      }
    });
    task_package.push_back(task);
  }
  thread_pool.SubmitSync(task_package);
  task_package.clear();

  return vid_with_maximum_degree;
}

Edges::Iterator Edges::SearchVertex(VertexID vid) {
  return std::lower_bound(
      this->begin(), this->end(), vid,
      [](const auto& l, VertexID vid) { return l.src < vid; });
}

}  // namespace sics::graph::tools::common