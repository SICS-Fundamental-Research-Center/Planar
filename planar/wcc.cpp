#include <gflags/gflags.h>

#include "core/apps/wcc_app.h"
#include "core/common/types.h"
#include "core/planar_system.h"

using namespace sics::graph::core;

int main(int argc, char** argv) {
  gflags::ParseCommandLineFlags(&argc, &argv, true);
  // TODO: flags setting to static variables in types.h

  // TODO: planar system code
  planar_system::Planar<apps::WCCApp> system(common::kDefaultRootPath);

  // run
  system.Start();
  return 0;
}