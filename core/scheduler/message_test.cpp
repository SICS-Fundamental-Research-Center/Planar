#include "message.h"

#include <folly/Format.h>
#include <gmock/gmock.h>
#include <gtest/gtest.h>

namespace sics::graph::core::scheduler {

class MessageTest : public ::testing::Test {
 protected:
  MessageTest() {
    // Suppress death test warnings.
    ::testing::FLAGS_gtest_death_test_style = "threadsafe";
  }
};

TEST(MessageTest, MessageShouldBeOfTheSetType) {
  using ::testing::EndsWith;
  using ::testing::internal::CaptureStderr;
  using ::testing::internal::GetCapturedStderr;
  CaptureStderr();

  ReadMessage read_msg{};
  Message msg(read_msg);
  msg.Get(&read_msg);
  EXPECT_EQ(msg.get_type(), Message::Type::kRead);

  ExecuteMessage execute_msg{};
  msg.Set(execute_msg);
  msg.Get(&execute_msg);
  EXPECT_EQ(msg.get_type(), Message::Type::kExecute);

  WriteMessage write_msg{};
  msg.Set(write_msg);
  msg.Get(&write_msg);
  EXPECT_EQ(msg.get_type(), Message::Type::kWrite);

  // Until now, there should be no warning message printed.
  EXPECT_EQ(folly::rtrimWhitespace(GetCapturedStderr()).toString().length(),
            0);
  CaptureStderr();
  msg.Get(&execute_msg);
  EXPECT_THAT(
      folly::rtrimWhitespace(GetCapturedStderr()).toString(),
      EndsWith(::fmt::format("Message type mismatch: expected {}, got {}",
                          Message::Type::kExecute,
                          Message::Type::kWrite)));
}

}  // namespace sics::graph::core::scheduler
