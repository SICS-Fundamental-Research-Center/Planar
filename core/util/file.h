#ifndef CORE_UTIL_FILE_H_
#define CORE_UTIL_FILE_H_

#include <sys/stat.h>
#include <sys/types.h>

namespace sics::graph::core::util {
namespace file {

// @DESCRIPTION
//
// Function MakeDirectory() create the DIRECTORY(path), if they do not already
// "Exist()".
void MakeDirectory(const std::string& path) {
  std::string dir = path;
  int len = dir.size();
  if (dir[len - 1] != '/') {
    dir[len] = '/';
    len++;
  }
  std::string temp;
  for (int i = 1; i < len; i++) {
    if (dir[i] == '/') {
      temp = dir.substr(0, i);
      if (access(temp.c_str(), 0) != 0)
        if (mkdir(temp.c_str(), 0777) != 0) VLOG(1) << "failed operaiton.";
    }
  }
}

bool Exist(const std::string& path) {
  struct stat buffer;
  return (stat(path.c_str(), &buffer) == 0);
}

}  // namespace file
}  // namespace sics::graph::core::util

#endif  // CORE_UTIL_FILE_H_