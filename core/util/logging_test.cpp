#include "logging.h"

#include <folly/FileUtil.h>
#include <folly/Format.h>
#include <folly/experimental/TestUtil.h>
#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <vector>

namespace xyz::graph::core::util {

// Define some strings for logging.
constexpr char fatal_message[] = "FATAL";
constexpr char error_message[] = "ERROR";
constexpr char warn_message[] = "WARN";
constexpr char info_message[] = "INFO";
constexpr char debug_message[] = "DEBUG";
constexpr char formatted_message[] = "message:{}";

// The fixture for testing class LogTest.
class LogTest : public ::testing::Test {
 protected:
  LogTest() {
    InitOrDie();
    // Suppress death test warnings.
    ::testing::FLAGS_gtest_death_test_style = "threadsafe";
  }
};

TEST_F(LogTest, DefaultLoggerLevelIsInfo) {
  using ::testing::EndsWith;
  using ::testing::internal::CaptureStderr;
  using ::testing::internal::GetCapturedStderr;
  // Disable asynchronous logging such that output can be tested.
  UpdateConfig("; default:async=false");

  // Test macro: `LOG_ERROR`.
  EXPECT_DEATH(LOG_FATAL(fatal_message), fatal_message);
  EXPECT_DEATH(LOGF_FATAL(formatted_message, fatal_message),
               folly::sformat("message:", fatal_message));

  // Test macro: `LOG_ERROR`.
  CaptureStderr();
  LOG_ERROR(error_message);
  EXPECT_THAT(folly::rtrimWhitespace(GetCapturedStderr()).toString(),
              EndsWith(error_message));
  CaptureStderr();
  LOGF_ERROR(formatted_message, error_message);
  EXPECT_THAT(folly::rtrimWhitespace(GetCapturedStderr()).toString(),
              EndsWith(folly::sformat(formatted_message, error_message)));

  // Test macro: `LOG_WARN`.
  CaptureStderr();
  LOG_WARN(warn_message);
  EXPECT_THAT(folly::rtrimWhitespace(GetCapturedStderr()).toString(),
              EndsWith(warn_message));
  CaptureStderr();
  LOGF_WARN(formatted_message, warn_message);
  EXPECT_THAT(folly::rtrimWhitespace(GetCapturedStderr()).toString(),
              EndsWith(folly::sformat(formatted_message, warn_message)));

  // Test macro: `LOG_INFO`.
  CaptureStderr();
  LOG_INFO(info_message);
  EXPECT_THAT(folly::rtrimWhitespace(GetCapturedStderr()).toString(),
              EndsWith(info_message));
  CaptureStderr();
  LOGF_INFO(formatted_message, info_message);
  EXPECT_THAT(folly::rtrimWhitespace(GetCapturedStderr()).toString(),
              EndsWith(folly::sformat(formatted_message, info_message)));

  // Test macro: `LOG_DEBUG`, which will not be printed under default level.
  CaptureStderr();
  LOG_DEBUG(debug_message);
  EXPECT_EQ(folly::rtrimWhitespace(GetCapturedStderr()).toString(), "");
  CaptureStderr();
  LOGF_DEBUG(formatted_message, info_message);
  EXPECT_EQ(folly::rtrimWhitespace(GetCapturedStderr()).toString(), "");
}

TEST_F(LogTest, UpdateConfigWorksAsIntended) {
  using ::testing::EndsWith;
  using ::testing::internal::CaptureStderr;
  using ::testing::internal::GetCapturedStderr;
  // Disable asynchronous logging such that output can be tested.
  UpdateConfig("; default:async=false");

  // Test macro: `LOG_DEBUG`, which will not be printed under default level.
  CaptureStderr();
  LOG_DEBUG(debug_message);
  EXPECT_EQ(folly::rtrimWhitespace(GetCapturedStderr()).toString(), "");

  // Using
  UpdateConfig(kDebugConfig);
  CaptureStderr();
  LOG_DEBUG(debug_message);
  EXPECT_THAT(folly::rtrimWhitespace(GetCapturedStderr()).toString(),
              EndsWith(debug_message));
}

TEST_F(LogTest, DefaultConfigWithLogFile) {
  using ::testing::EndsWith;
  using ::testing::internal::CaptureStderr;
  using ::testing::internal::CaptureStdout;
  using ::testing::internal::GetCapturedStderr;
  using ::testing::internal::GetCapturedStdout;

  // Initialize a temporary file for logs.
  folly::test::TemporaryFile log_file;
  const std::string file_name = log_file.path().string();
  std::string config = DefaultConfigWithLogFile(file_name);
  OverrideConfig(config);
  UpdateConfig("; default:async=false; f:async=false");

  // DefaultConfigWithLogFile sets log level to INFO.
  CaptureStdout();
  LOG_WARN(warn_message);
  LOG_DEBUG(debug_message);
  EXPECT_THAT(folly::rtrimWhitespace(GetCapturedStdout()).toString(),
              EndsWith(warn_message));

  // Check log file content.
  char log_content[60];
  size_t bytes_read = folly::readNoInt(log_file.fd(), log_content, 60);
  std::string log_str(log_content, bytes_read);
  EXPECT_THAT(folly::rtrimWhitespace(log_str).toString(),
              EndsWith(warn_message));

  // `log_file` is closed on destruction.
}

}  // namespace xyz::graph::core::util
