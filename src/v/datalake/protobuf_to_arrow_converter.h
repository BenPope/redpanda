/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */
#pragma once
#include "datalake/errors.h"
#include "datalake/logger.h"
#include "datalake/proto_to_arrow_interface.h"
#include "datalake/proto_to_arrow_scalar.h"
#include "datalake/proto_to_arrow_struct.h"

#include <arrow/api.h>
#include <arrow/array/builder_base.h>
#include <arrow/array/builder_nested.h>
#include <arrow/chunked_array.h>
#include <arrow/io/api.h>
#include <arrow/result.h>
#include <arrow/status.h>
#include <arrow/type.h>
#include <arrow/type_fwd.h>
#include <arrow/type_traits.h>
#include <google/protobuf/arena.h>
#include <google/protobuf/compiler/parser.h>
#include <google/protobuf/descriptor.h>
#include <google/protobuf/dynamic_message.h>
#include <google/protobuf/io/tokenizer.h>
#include <google/protobuf/io/zero_copy_stream_impl_lite.h>
#include <google/protobuf/message.h>
#include <google/protobuf/stubs/common.h>
#include <google/protobuf/unknown_field_set.h>

#include <memory>
#include <stdexcept>

namespace datalake {

struct error_collector : ::google::protobuf::io::ErrorCollector {
    void AddError(int line, int column, const std::string& message) override {
        // Warning level because this is an error in the input, not Redpanda
        // itself.
        datalake_log.warn("Protobuf Error {}:{} {}", line, column, message);
    }
    void AddWarning(int line, int column, const std::string& message) override {
        datalake_log.warn("Protobuf Warning {}:{} {}", line, column, message);
    }
};

/** Top-level interface for parsing Protobuf messages to an Arrow table

This class deserializes protobuf messages and passes the deserialized messages
to an instance of proto_to_arrow_struct to recursively parse the structured
message.
*/
class proto_to_arrow_converter {
public:
    explicit proto_to_arrow_converter(std::string schema);

    [[nodiscard]] arrow_converter_status
    add_message(const std::string& serialized_message);

    [[nodiscard]] arrow_converter_status finish_batch();

    std::shared_ptr<arrow::Table> build_table();

    std::vector<std::shared_ptr<arrow::Field>> build_field_vec();

    std::shared_ptr<arrow::Schema> build_schema();

private:
    FRIEND_TEST(ArrowWriter, EmptyMessageTest);
    FRIEND_TEST(ArrowWriter, SimpleMessageTest);
    FRIEND_TEST(ArrowWriter, NestedMessageTest);

    void initialize_protobuf_schema(const std::string& schema);

    bool initialize_struct_converter();

    /// Parse the message to a protobuf message.
    /// Return nullptr on error.
    std::unique_ptr<google::protobuf::Message>
    parse_message(const std::string& message);
    const google::protobuf::Descriptor* message_descriptor();

private:
    google::protobuf::DescriptorPool _protobuf_descriptor_pool;
    google::protobuf::FileDescriptorProto _file_descriptor_proto;
    google::protobuf::DynamicMessageFactory _factory;
    const google::protobuf::FileDescriptor* _file_desc;

    std::unique_ptr<detail::proto_to_arrow_struct> _struct_converter;
    error_collector error_collector;
};

} // namespace datalake
