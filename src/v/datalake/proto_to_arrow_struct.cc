/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */
#include "datalake/proto_to_arrow_struct.h"

#include <seastar/util/variant_utils.hh>

#include <fmt/format.h>

arrow::Status datalake::detail::proto_to_arrow_struct::finish_batch() {
    if (!_arrow_status.ok()) {
        return _arrow_status;
    }
    arrow::Result<std::shared_ptr<arrow::Array>> builder_result
      = _builder->Finish();

    _arrow_status = builder_result.status();
    if (!_arrow_status.ok()) {
        return _arrow_status;
    }

    // Safe because we validated the status after calling `Finish`
    _values.push_back(std::move(builder_result).ValueUnsafe());

    return _arrow_status;
}
arrow::Status datalake::detail::proto_to_arrow_struct::add_top_level_message(
  const google::protobuf::Message* msg) {
    if (!_arrow_status.ok()) {
        return _arrow_status;
    }
    for (size_t child_idx = 0; child_idx < _child_arrays.size(); child_idx++) {
        _arrow_status = seastar::visit(
          _child_arrays[child_idx], [msg, child_idx](auto& child) {
              return child->add_value(msg, int(child_idx));
          });
        if (!_arrow_status.ok()) {
            return _arrow_status;
        }
    }
    _arrow_status = _builder->Append();

    return _arrow_status;
}
arrow::Status datalake::detail::proto_to_arrow_struct::add_value(
  const google::protobuf::Message* msg, int field_idx) {
    if (!_arrow_status.ok()) {
        return _arrow_status;
    }
    auto desc = msg->GetDescriptor()->field(field_idx);
    auto child_message = &msg->GetReflection()->GetMessage(*msg, desc);
    return add_top_level_message(child_message);
}
datalake::detail::proto_to_arrow_struct::proto_to_arrow_struct(
  const google::protobuf::Descriptor* message_descriptor) {
    using namespace detail;
    namespace pb = google::protobuf;

    // Set up child arrays
    _child_arrays.reserve(message_descriptor->field_count());
    for (int field_idx = 0; field_idx < message_descriptor->field_count();
         field_idx++) {
        auto desc = message_descriptor->field(field_idx);
        switch (desc->cpp_type()) {
        case pb::FieldDescriptor::CPPTYPE_INT32:
            _child_arrays.emplace_back(
              proto_to_arrow_scalar<arrow::Int32Type>());
            break;
        case pb::FieldDescriptor::CPPTYPE_INT64:
            _child_arrays.emplace_back(
              proto_to_arrow_scalar<arrow::Int64Type>());
            break;
        case pb::FieldDescriptor::CPPTYPE_BOOL:
            _child_arrays.emplace_back(
              proto_to_arrow_scalar<arrow::BooleanType>());
            break;
        case pb::FieldDescriptor::CPPTYPE_FLOAT:
            _child_arrays.emplace_back(
              proto_to_arrow_scalar<arrow::FloatType>());
            break;
        case pb::FieldDescriptor::CPPTYPE_DOUBLE:
            _child_arrays.emplace_back(
              proto_to_arrow_scalar<arrow::DoubleType>());
            break;
        case pb::FieldDescriptor::CPPTYPE_STRING:
            _child_arrays.emplace_back(
              proto_to_arrow_scalar<arrow::StringType>());
            break;
        case pb::FieldDescriptor::CPPTYPE_MESSAGE: {
            auto field_message_descriptor = desc->message_type();
            if (field_message_descriptor == nullptr) {
                throw datalake::initialization_error(
                  std::string("Can't find schema for nested type : ")
                  + desc->cpp_type_name());
            }
            _child_arrays.emplace_back(std::make_unique<proto_to_arrow_struct>(
              field_message_descriptor));
        } break;
        default:
            throw datalake::initialization_error(
              fmt::format("Unknown type: {}", desc->cpp_type_name()));
        }
    }
    // Make Arrow data types

    // This could be combined into a single loop with the one above, but
    // this seems more readable to me
    _fields.reserve(message_descriptor->field_count());
    for (int field_idx = 0; field_idx < message_descriptor->field_count();
         field_idx++) {
        auto field_desc = message_descriptor->field(field_idx);
        _fields.push_back(
          seastar::visit(_child_arrays[field_idx], [field_desc](auto& child) {
              return child->field(field_desc->name());
          }));
    }
    _arrow_data_type = arrow::struct_(_fields);

    // Make builder
    std::vector<std::shared_ptr<arrow::ArrayBuilder>> child_builders;
    child_builders.reserve(_child_arrays.size());
    // This could also be collapsed into the above loop
    for (auto& child : _child_arrays) {
        child_builders.push_back(
          seastar::visit(child, [](auto& child) { return child->builder(); }));
    }
    _builder = std::make_shared<arrow::StructBuilder>(
      _arrow_data_type, arrow::default_memory_pool(), child_builders);
}
std::shared_ptr<arrow::ArrayBuilder>
datalake::detail::proto_to_arrow_struct::builder() {
    return _builder;
}
std::shared_ptr<arrow::Field>
datalake::detail::proto_to_arrow_struct::field(const std::string& name) {
    return arrow::field(name, _arrow_data_type);
}
arrow::FieldVector datalake::detail::proto_to_arrow_struct::get_field_vector() {
    return _fields;
}
