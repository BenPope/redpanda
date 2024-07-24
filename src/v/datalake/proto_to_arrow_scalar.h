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

#include "datalake/proto_to_arrow_interface.h"

#include <arrow/api.h>
#include <arrow/array/array_primitive.h>
#include <arrow/type.h>
#include <google/protobuf/message.h>

#include <memory>
#include <stdexcept>

namespace datalake::detail {

template<typename ArrowType>
class proto_to_arrow_scalar {
    using BuilderType = arrow::TypeTraits<ArrowType>::BuilderType;

public:
    proto_to_arrow_scalar()
      : _builder(std::make_shared<BuilderType>()) {}

    arrow::Status
    add_value(const google::protobuf::Message* msg, int field_idx) {
        if (!_arrow_status.ok()) {
            return _arrow_status;
        }
        do_add(msg, field_idx);
        return _arrow_status;
    }

    arrow::Status finish_batch() {
        if (!_arrow_status.ok()) {
            return _arrow_status;
        }
        auto builder_result = _builder->Finish();
        _arrow_status = builder_result.status();
        if (!_arrow_status.ok()) {
            return _arrow_status;
        }

        // Safe because we validated the status after calling `Finish`
        _values.push_back(std::move(builder_result).ValueUnsafe());
        return _arrow_status;
    }

    std::shared_ptr<arrow::Field> field(const std::string& name) {
        return arrow::field(
          name, arrow::TypeTraits<ArrowType>::type_singleton());
    }

    std::shared_ptr<arrow::ArrayBuilder> builder() { return _builder; }

    auto operator->() { return this; }

private:
    void do_add(const google::protobuf::Message* msg, int field_idx);

    arrow::Status _arrow_status;
    arrow::ArrayVector _values;
    std::shared_ptr<BuilderType> _builder;
};

} // namespace datalake::detail
