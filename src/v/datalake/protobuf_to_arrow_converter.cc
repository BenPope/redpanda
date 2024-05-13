/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */
#include "datalake/protobuf_to_arrow_converter.h"

#include "datalake/errors.h"
#include "datalake/proto_to_arrow_struct.h"

#include <arrow/type.h>

#include <memory>
#include <stdexcept>

namespace datalake {

proto_to_arrow_converter::proto_to_arrow_converter(std::string schema) {
    initialize_protobuf_schema(schema);
    if (!initialize_struct_converter()) {
        throw initialization_error("Could not initialize arrow arrays");
    }
}
arrow_converter_status
proto_to_arrow_converter::add_message(const std::string& serialized_message) {
    std::unique_ptr<google::protobuf::Message> message = parse_message(
      serialized_message);
    if (message == nullptr) {
        return arrow_converter_status::parse_error;
    }

    if (!_struct_converter->add_top_level_message(message.get()).ok()) {
        return arrow_converter_status::internal_error;
    }
    return arrow_converter_status::ok;
}
arrow_converter_status proto_to_arrow_converter::finish_batch() {
    if (!_struct_converter->finish_batch().ok()) {
        return arrow_converter_status::internal_error;
    }
    return arrow_converter_status::ok;
}
std::shared_ptr<arrow::Table> proto_to_arrow_converter::build_table() {
    auto table_result = arrow::Table::FromChunkedStructArray(
      _struct_converter->finish());
    if (table_result.ok()) {
        return table_result.ValueUnsafe();
    } else {
        return nullptr;
    }
}
std::vector<std::shared_ptr<arrow::Field>>
proto_to_arrow_converter::build_field_vec() {
    return _struct_converter->get_field_vector();
}
std::shared_ptr<arrow::Schema> proto_to_arrow_converter::build_schema() {
    return arrow::schema(_struct_converter->get_field_vector());
}

void proto_to_arrow_converter::initialize_protobuf_schema(
  const std::string& schema) {
    google::protobuf::io::ArrayInputStream proto_input_stream(
      schema.c_str(), schema.size());
    google::protobuf::io::Tokenizer tokenizer(&proto_input_stream, nullptr);

    google::protobuf::compiler::Parser parser;
    parser.RecordErrorsTo(&error_collector);
    if (!parser.Parse(&tokenizer, &_file_descriptor_proto)) {
        throw initialization_error("Could not parse protobuf schema");
    }

    if (!_file_descriptor_proto.has_name()) {
        _file_descriptor_proto.set_name("default_message_name");
    }

    _file_desc = _protobuf_descriptor_pool.BuildFile(_file_descriptor_proto);
    if (_file_desc == nullptr) {
        throw initialization_error("Could not build descriptor pool");
    }
}
bool proto_to_arrow_converter::initialize_struct_converter() {
    using namespace detail;
    namespace pb = google::protobuf;

    const pb::Descriptor* message_desc = message_descriptor();
    if (message_desc == nullptr) {
        return false;
    }

    _struct_converter = std::make_unique<detail::proto_to_arrow_struct>(
      message_desc);

    return true;
}
std::unique_ptr<google::protobuf::Message>
proto_to_arrow_converter::parse_message(const std::string& message) {
    // Get the message descriptor
    const google::protobuf::Descriptor* message_desc = message_descriptor();
    if (message_desc == nullptr) {
        return nullptr;
    }

    const google::protobuf::Message* prototype_msg = _factory.GetPrototype(
      message_desc);
    if (prototype_msg == nullptr) {
        return nullptr;
    }

    google::protobuf::Message* mutable_msg = prototype_msg->New();
    if (mutable_msg == nullptr) {
        return nullptr;
    }

    if (!mutable_msg->ParseFromString(message)) {
        return nullptr;
    }
    return std::unique_ptr<google::protobuf::Message>(mutable_msg);
}

const google::protobuf::Descriptor*
datalake::proto_to_arrow_converter::message_descriptor() {
    int message_type_count = _file_desc->message_type_count();
    if (message_type_count == 0) {
        return nullptr;
    }
    return _file_desc->message_type(message_type_count - 1);
}
} // namespace datalake
