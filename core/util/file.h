#ifndef CORE_UTIL_FILE_H_
#define CORE_UTIL_FILE_H_

#include <filesystem>
#include <fstream>

namespace sics::graph::core::util {
namespace file {

// @DESCRIPTION
//
// Function MakeDirectory() create the DIRECTORY(path), if they do not already
// "Exist()".
inline void MakeDirectory(const std::string& path) {
  std::filesystem::create_directory(path);
}

inline bool Exist(const std::string& path) {
  std::ifstream f(path.c_str());
  return f.good();
}

}  // namespace file
}  // namespace sics::graph::core::util

#endif  // CORE_UTIL_FILE_H_