#include <gflags/gflags.h>

#include "core/apps/wcc_app.h"
#include "core/common/types.h"
#include "core/planar_system.h"

int main(int argc, char** argv) {
  gflags::ParseCommandLineFlags(&argc, &argv, true);
  // TODO: flags setting to static variables in types.h

  // TODO: planar system code
  sics::graph::core::apps::WCCApp app;
  sics::graph::core::planar_system::Planar<sics::graph::core::apps::WCCApp>
      system;

  // run
  return 0;
}