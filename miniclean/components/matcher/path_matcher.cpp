#include "miniclean/components/matcher/path_matcher.h"

#include <fstream>
#include <memory>
#include <sstream>
#include <string>

#include "core/common/multithreading/task.h"
#include "core/common/multithreading/thread_pool.h"
#include "core/data_structures/graph/serialized_immutable_csr_graph.h"
#include "core/util/logging.h"
#include "core/scheduler/message.h"
#include "miniclean/io/miniclean_csr_reader.h"

namespace sics::graph::miniclean::components::matcher {

using MiniCleanCSRReader = sics::graph::miniclean::io::MiniCleanCSRReader;
using ReadMessage = sics::graph::core::scheduler::ReadMessage;
using SerializedImmutableCSRGraph =
    sics::graph::core::data_structures::graph::SerializedImmutableCSRGraph;
using Task = sics::graph::core::common::Task;
using TaskPackage = sics::graph::core::common::TaskPackage;
using ThreadPool = sics::graph::core::common::ThreadPool;

void PathMatcher::LoadGraph(const std::string& data_path) {
  // Prepare reader.
  MiniCleanCSRReader reader(data_path);

  // Initialize Serialized object.
  std::unique_ptr<SerializedImmutableCSRGraph> serialized_graph =
      std::make_unique<SerializedImmutableCSRGraph>();

  // Initialize ReadMessage object.
  ReadMessage read_message;
  read_message.graph_id = miniclean_csr_graph_->get_metadata().gid;
  read_message.response_serialized = serialized_graph.get();

  // Read a subgraph.
  reader.Read(&read_message, nullptr);

  // Deserialize subgraph.
  ThreadPool thread_pool(1);
  miniclean_csr_graph_->Deserialize(thread_pool, std::move(serialized_graph));
}

void PathMatcher::LoadPatterns(const std::string& pattern_path) {
  std::ifstream pattern_file(pattern_path);
  if (!pattern_file) {
    LOG_FATAL("Error opening pattern file: ", pattern_path.c_str());
  }

  VertexLabel max_label_id = 1;

  std::string line;
  while (std::getline(pattern_file, line)) {
    // if the line is empty or the first char is `#`, continue.
    if (line.empty() || line[0] == '#') continue;

    std::vector<VertexLabel> pattern;
    std::stringstream ss(line);
    std::string label;

    while (std::getline(ss, label, ',')) {
      VertexLabel label_id = static_cast<VertexLabel>(std::stoi(label));
      max_label_id = std::max(max_label_id, label_id);
      pattern.push_back(label_id);
    }

    path_patterns_.push_back(pattern);
  }

  num_label_ = max_label_id;

  // Initialize `vertex_label_to_pattern_id`.
  vertex_label_to_pattern_id.resize(num_label_);
  for (size_t i = 0; i < path_patterns_.size(); i++) {
    VertexLabel start_label = path_patterns_[i][0];
    vertex_label_to_pattern_id[start_label - 1].push_back(i);
  }
}

void PathMatcher::BuildCandidateSet() {
  // Reserve memory space for candidates_.
  candidates_.reserve(num_label_);
  // Initialize candidates_.
  for (VertexLabel i = 0; i < num_label_; i++) {
    candidates_.emplace_back();
  }
  VertexLabel* vertex_label = miniclean_csr_graph_->get_vertex_label();
  for (VertexID i = 0; i < miniclean_csr_graph_->get_num_vertices() * 2;
       i += 2) {
    for (VertexLabel j = 0; j < num_label_; j++) {
      if (vertex_label[i + 1] == j + 1)
        candidates_[j].push_back(vertex_label[i]);
    }
  }
}

void PathMatcher::GroupTasks(
    size_t num_tasks, std::vector<std::vector<VertexID>>* partial_result_pool,
    TaskPackage* task_package) {
  // Compute the task size (i.e., the number of task unit)
  VertexID vertex_number = miniclean_csr_graph_->get_num_vertices();
  auto task_size = vertex_number / num_tasks + 1;

  for (size_t i = 0; i < num_tasks; i++) {
    Task task = [&, this, i, task_size, vertex_number, partial_result_pool]() {
      // Initialize result pool for current task.
      std::vector<std::list<std::vector<VertexID>>> local_matched_results;
      local_matched_results.resize(path_patterns_.size());
      // Group tasks from vertex `task_size * i`
      // to vertex `task_size * (i + 1)`.
      auto t1 = std::chrono::system_clock::now();
      for (VertexID j = task_size * i; j < task_size * (i + 1); j++) {
        if (j >= vertex_number) break;
        VertexLabel label = miniclean_csr_graph_->get_vertex_label()[j * 2 + 1];
        std::vector<size_t> patterns = vertex_label_to_pattern_id[label - 1];
        for (size_t k = 0; k < patterns.size(); k++) {
          std::vector<VertexID> init_candidate = {j};
          PathMatchRecur(path_patterns_[patterns[k]], 0, init_candidate,
                         &(*partial_result_pool)[i],
                         &local_matched_results[patterns[k]]);
          // Recover the partial results.
          (*partial_result_pool)[i].clear();
        }
      }
      auto t2 = std::chrono::system_clock::now();
      // Merge the local matched results to the global matched results.
      {
        std::lock_guard<std::mutex> lock(mtx_);
        for (size_t j = 0; j < path_patterns_.size(); j++) {
          matched_results_[j].splice(matched_results_[j].end(),
                                     local_matched_results[j]);
        }
      }
      auto t3 = std::chrono::system_clock::now();
      // Update the execution and write back time.
      {
        std::lock_guard<std::mutex> lock(dur_mtx_);
        auto exe_time =
            std::chrono::duration_cast<std::chrono::microseconds>(t2 - t1)
                .count() /
            (double)CLOCKS_PER_SEC;
        auto write_back =
            std::chrono::duration_cast<std::chrono::microseconds>(t3 - t2)
                .count() /
            (double)CLOCKS_PER_SEC;
        exe_t0 = std::min(exe_t0, exe_time);
        write_back_t0 = std::min(write_back_t0, write_back);
        exe_t1 = std::max(exe_t1, exe_time);
        write_back_t1 = std::max(write_back_t1, write_back);
      }
    };
    task_package->push_back(task);
  }
}

void PathMatcher::PathMatching(unsigned int parallelism,
                               unsigned int num_tasks) {
  LOG_INFO("Initializing auxiliary data structures...");
  // Initialize matched results.
  matched_results_.resize(path_patterns_.size());

  // Initialize thread pool.
  ThreadPool thread_pool(parallelism);
  // Initialize partial results pool.
  std::vector<std::vector<VertexID>> partial_result_pool(num_tasks);
  // Initialize matched results pool.
  std::vector<std::vector<std::vector<std::vector<VertexID>>>>
      matched_result_pool(num_tasks);
  for (auto& result : matched_result_pool) {
    result.resize(path_patterns_.size());
  }
  // Initialize task package.
  TaskPackage task_package;
  task_package.reserve(num_tasks);

  // Group tasks.
  LOG_INFO("Grouping tasks...");
  GroupTasks(num_tasks, &partial_result_pool, &task_package);

  // Submit tasks.
  LOG_INFO("Submitting tasks...");
  thread_pool.SubmitSync(task_package);
  task_package.clear();

  // Print the length of matched results.
  for (size_t i = 0; i < matched_results_.size(); i++) {
    LOG_INFO("Pattern: ", i, ", Matched results: ", matched_results_[i].size());
  }

  // Print the execution time and write back time.
  LOG_INFO("Execution time: ", exe_t1 - exe_t0, " seconds.");
  LOG_INFO("Write back time: ", write_back_t1 - write_back_t0, " seconds.");
}

void PathMatcher::PathMatchRecur(const std::vector<VertexLabel>& path_pattern,
                                 size_t match_position,
                                 const std::vector<VertexID>& candidates,
                                 std::vector<VertexID>* partial_results,
                                 std::list<std::vector<VertexID>>* results) {
  // Return condition.
  if (match_position == path_pattern.size()) {
    results->push_back(*partial_results);
    return;
  }

  for (VertexID candidate : candidates) {
    // Scan the out-edges of the candidate.
    VertexID cand_out_degree =
        miniclean_csr_graph_->GetOutDegreeBasePointer()[candidate];
    VertexID cand_out_offset =
        miniclean_csr_graph_->GetOutOffsetBasePointer()[candidate];
    std::vector<VertexID> next_candidates;

    for (size_t i = 0; i < cand_out_degree; i++) {
      VertexID out_edge_id =
          miniclean_csr_graph_
              ->GetOutgoingEdgesBasePointer()[cand_out_offset + i];
      VertexLabel out_edge_label =
          miniclean_csr_graph_->get_vertex_label()[out_edge_id * 2 + 1];
      bool continue_flag = false;
      // Check whether cycle exists.
      for (VertexID vid : *partial_results) {
        if (vid == out_edge_id) {
          continue_flag = true;
          break;
        }
      }
      if (continue_flag) continue;
      // Check whether the label matches.
      if (match_position + 1 < path_pattern.size() &&
          out_edge_label == path_pattern[match_position + 1]) {
        next_candidates.push_back(out_edge_id);
      }
    }
    partial_results->push_back(candidate);
    PathMatchRecur(path_pattern, match_position + 1, next_candidates,
                   partial_results, results);
    partial_results->pop_back();
  }
}

}  // namespace sics::graph::miniclean::components::matcher