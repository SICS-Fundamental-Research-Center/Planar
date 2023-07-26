#ifndef CORE_DATA_STRUCTURES_SERIALIZED_H_
#define CORE_DATA_STRUCTURES_SERIALIZED_H_

#include <list>

#include "data_structures/buffer.h"

namespace sics::graph::core::data_structures {

class Serialized {
 public:
  // `HasNext()` is called when `Writer` writes `Serialized` class back to disk.
  //
  // e.g. in `Writer::Write()`:
  //  while (serialized.HasNext) {
  //    std::list<OwnedBuffer> buffers = serialized.PopNext();
  //
  //    /* Write buffers to disks */
  //
  //  }
  virtual bool HasNext() const = 0;

  // `ReceiveBuffers()` is called when `Reader` reads `Serialized` from disk.
  //
  // The reason to use `std::list<OwnedBuffer>` instead of `OwnedBuffer` is to
  // make it compatible with parallel reading in which case multiple
  // `OwnedBuffer`s are read from the same data file at the same time.
  //
  // `SetComplete()` should be called when all buffers are received.
  //
  // e.g. in `Reader::ReadBinFile(const std::string& path, Serialized* dst)`:
  //  while (/* folder.HasUnreadFile() */) {
  //    std::list<OwnedBuffer> file_buffers;
  //    /* Read data from disk to file_buffers */
  //    dst->ReceiveBuffers(std::move(file_buffers));
  //  }
  virtual void ReceiveBuffers(std::list<OwnedBuffer>&& buffers) = 0;
  
  // `PopNext()` is called when `Writer` writes `Serialized` class back to disk.
  // It returns a list of `OwnedBuffer`s that are ready to be written to disk.
  std::list<OwnedBuffer> PopNext() {
    is_complete_ = false;
    return PopNextImpl();
  }

  bool IsComplete() const { return is_complete_; }

  void SetComplete() { is_complete_ = true; }

 protected:
  virtual std::list<OwnedBuffer> PopNextImpl() = 0;

 protected:
  bool is_complete_ = false;
};

}  // namespace sics::graph::core::data_structures

#endif  // CORE_DATA_STRUCTURES_SERIALIZED_H_