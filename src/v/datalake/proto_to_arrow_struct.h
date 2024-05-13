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
#include "datalake/proto_to_arrow_interface.h"
#include "datalake/proto_to_arrow_scalar.h"

#include <arrow/api.h>
#include <arrow/type.h>
#include <arrow/type_fwd.h>
#include <google/protobuf/descriptor.h>
#include <google/protobuf/message.h>

#include <memory>
#include <stdexcept>

namespace datalake::detail {

class proto_to_arrow_struct : public proto_to_arrow_interface {
public:
    explicit proto_to_arrow_struct(
      const google::protobuf::Descriptor* message_descriptor);

    arrow::Status
    add_value(const google::protobuf::Message* msg, int field_idx) override;

    // Like add_value, but adds a top-level message instead of a child field.
    arrow::Status add_top_level_message(const google::protobuf::Message* msg);

    arrow::Status finish_batch() override;

    std::shared_ptr<arrow::Field> field(const std::string& name) override;
    std::shared_ptr<arrow::ArrayBuilder> builder() override;

    arrow::FieldVector get_field_vector();

private:
    std::vector<std::unique_ptr<proto_to_arrow_interface>> _child_arrays;
    std::shared_ptr<arrow::DataType> _arrow_data_type;
    std::shared_ptr<arrow::StructBuilder> _builder;
    arrow::FieldVector _fields;
};

} // namespace datalake::detail
