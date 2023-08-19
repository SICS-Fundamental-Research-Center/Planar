#ifndef SICS_GRAPH_SYSTEMS_TOOLS_UTIL_SORT_H_
#define SICS_GRAPH_SYSTEMS_TOOLS_UTIL_SORT_H_

namespace sics::graph::tools::util {

template <typename T>
int Part(T* r, int lower_bound, int upper_bound) {
  int i = lower_bound, j = upper_bound;
  T pivot = r[lower_bound];
  while (i < j) {
    while (i < j && r[j] > pivot) j--;
    if (i < j) std::swap(r[i++], r[j]);
    while (i < j && r[i] <= pivot) i++;
    if (i < j) std::swap(r[i], r[j--]);
  }
  return i;
}

template <typename T>
void QuickSort(T* r, int lower_bound, int upper_bound) {
  int mid;
  if (lower_bound < upper_bound) {
    mid = Part(r, lower_bound, upper_bound);
    QuickSort(r, lower_bound, mid - 1);
    QuickSort(r, mid + 1, upper_bound);
  }
}

}  // namespace sics::graph::tools::util

#endif  // SICS_GRAPH_SYSTEMS_TOOLS_UTIL_SORT_H_