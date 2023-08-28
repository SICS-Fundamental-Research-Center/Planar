#include <gflags/gflags.h>

#include "core/apps/wcc_app.h"
#include "core/common/types.h"

int main(int argc, char** argv) {
  gflags::ParseCommandLineFlags(&argc, &argv, true);
  // TODO: flags setting to static variables in types.h

  // TODO: planar system code

  // run
  return 0;
}