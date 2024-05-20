// Copyright 2024 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "storage/mvlog/file.h"
#include "storage/mvlog/readable_segment.h"
#include "storage/mvlog/segment_reader.h"

#include <seastar/core/seastar.hh>
#include <seastar/core/temporary_buffer.hh>

#include <gtest/gtest.h>

using namespace storage::experimental::mvlog;
using namespace experimental;

class SegmentReaderTest : public ::testing::Test {
public:
    void SetUp() override {
        cleanup_files_.emplace_back(file_);
        paging_file_ = file_manager_.create_file(file_).get();
    }

    void TearDown() override {
        paging_file_->close().get();

        for (auto& file : cleanup_files_) {
            try {
                ss::remove_file(file.string()).get();
            } catch (...) {
            }
        }
    }

protected:
    const std::filesystem::path file_{"segment"};
    file_manager file_manager_;
    std::unique_ptr<file> paging_file_;
    std::vector<std::filesystem::path> cleanup_files_;
};

TEST_F(SegmentReaderTest, TestCountReaders) {
    readable_segment readable_seg(paging_file_.get());
    ASSERT_EQ(0, readable_seg.num_readers());
    {
        auto reader = readable_seg.make_reader();
        ASSERT_EQ(1, readable_seg.num_readers());
    }
    ASSERT_EQ(0, readable_seg.num_readers());
    std::vector<std::unique_ptr<segment_reader>> readers;
    readers.reserve(10);
    for (int i = 0; i < 10; i++) {
        auto reader = readable_seg.make_reader();
        ASSERT_EQ(i + 1, readable_seg.num_readers());
        readers.emplace_back(std::move(reader));
    }
    ASSERT_EQ(10, readable_seg.num_readers());
    readers.clear();
    ASSERT_EQ(0, readable_seg.num_readers());
}

TEST_F(SegmentReaderTest, TestEmptyRead) {
    readable_segment readable_seg(paging_file_.get());
    auto reader = readable_seg.make_reader();
    auto stream = reader->make_stream();
    auto buf = stream.read().get();
    ASSERT_TRUE(buf.empty());
}

TEST_F(SegmentReaderTest, TestBasicReads) {
    ss::sstring data = "0123456789";
    iobuf buf;
    buf.append(data.data(), data.size());
    paging_file_->append(std::move(buf)).get();
    readable_segment readable_seg(paging_file_.get());
    for (int i = 0; i < data.size(); i++) {
        auto reader = readable_seg.make_reader();
        auto stream = reader->make_stream(i);
        auto buf = stream.read_up_to(data.size()).get();

        auto expected_str = data.substr(i);
        ss::temporary_buffer<char> expected_buf{
          expected_str.begin(), expected_str.size()};
        ASSERT_EQ(buf, expected_buf);
    }
}