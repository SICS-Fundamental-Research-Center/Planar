#ifndef MINICLEAN_MINICLEAN_APP_BASE_H_
#define MINICLEAN_MINICLEAN_APP_BASE_H_

namespace sics::graph::miniclean {

// Base class for MiniClean applications.
// Classes that inherit from this class can be holded by `ExecuteMessage`.
// TODO (bai-wenchao): add virtual functions if MiniClean applications can be
// further abstracted.
class MiniCleanAppBase {};

}  // namespace sics::graph::miniclean

#endif  // MINICLEAN_MINICLEAN_APP_BASE_H_