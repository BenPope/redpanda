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

#include <arrow/api.h>
#include <arrow/type_fwd.h>
#include <google/protobuf/message.h>

#include <memory>
#include <stdexcept>

namespace datalake::detail {
// class proto_to_arrow_interface {
// public:
//     virtual ~proto_to_arrow_interface() {}

//     // Pure virtual methods
//     virtual arrow::Status
//     add_value(const google::protobuf::Message*, int field_idx)
//       = 0;

//     /// Return an Arrow field descriptor for this Array. Used for building
//     /// A schema.
//     // The Arrow API is built around shared_ptr: the creation functions return
//     // shared pointers, and other expect them as arguments.
//     // There is a C API for Arrow that would avoid shared pointers leaking into
//     // our code. It's not clear if that would offer any benefit, since it's
//     // actually a GLib wrapper around the C++ API.
//     virtual std::shared_ptr<arrow::Field> field(const std::string& name) = 0;

//     /// Return the underlying ArrayBuilder. Used when this is a child of another
//     /// Builder
//     virtual std::shared_ptr<arrow::ArrayBuilder> builder() = 0;

//     // Methods with defaults
//     virtual arrow::Status finish_batch() { return arrow::Status::OK(); }
//     std::shared_ptr<arrow::ChunkedArray> finish() {
//         return std::make_shared<arrow::ChunkedArray>(_values);
//     }

// protected:
//     arrow::Status _arrow_status;
//     arrow::ArrayVector _values;
// };

} // namespace datalake::detail
