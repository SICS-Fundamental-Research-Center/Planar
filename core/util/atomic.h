#ifndef GRAPH_SYSTEMS_ATOMIC_H
#define GRAPH_SYSTEMS_ATOMIC_H

#include <assert.h>
#include <stdlib.h>

namespace sics::graph::core::util {

namespace atomic {

// @DESCRIPTION
//
//  CAS is an atomic instruction used in multithreading to achieve
//  synchronization.  It compares the contents of a memory location with a given
//  value and, only if they are the same, modifies the contents of that memory
//  location to a new given value.
template <class ET>
inline bool CAS(ET* ptr, ET oldv, ET newv) const {
  if (sizeof(ET) == 8) {
    return __sync_bool_compare_and_swap((long*)ptr, *((long*)&oldv),
                                        *((long*)&newv));
  } else if (sizeof(ET) == 4) {
    return __sync_bool_compare_and_swap((int*)ptr, *((int*)&oldv),
                                        *((int*)&newv));
  } else if (sizeof(ET) == 2) {
    return __sync_bool_compare_and_swap((unsigned short*)ptr,
                                        *((unsigned short*)&oldv),
                                        *((unsigned short*)&newv));
  } else {
    assert(false);
  }
}

template <class ET>
inline bool WriteMin(ET* a, ET b) const {
  ET c;
  bool r = 0;
  do c = *a;
  while (c > b && !(r = CAS(a, c, b)));
  return r;
}

template <class ET>
inline bool WriteMax(ET* a, ET b) const {
  ET c;
  bool r = 0;
  do c = *a;
  while (c < b && !(r = CAS(a, c, b)));
  return r;
}

template <class ET>
inline void WriteAdd(ET* a, ET b) {
  volatile ET newV, oldV;
  do {
    oldV = *a;
    newV = oldV + b;
  } while (!CAS(a, oldV, newV));
}

}  // namespace atomic
}  // namespace sics::graph::core::util

#endif  // GRAPH_SYSTEMS_ATOMIC_H