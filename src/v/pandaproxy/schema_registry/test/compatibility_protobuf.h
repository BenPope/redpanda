// Copyright 2021 Vectorized, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#pragma once

#include "pandaproxy/schema_registry/protobuf.h"

namespace pp = pandaproxy;
namespace pps = pp::schema_registry;

const auto enum2 = pps::raw_schema_definition{
  R"(
syntax = "proto3";

message Test1 {
  enum Symbols {
    ZERO = 0;
    ONE = 1;
    TWO = 2;
  }
  Symbols symbols = 1;
})",
  pps::schema_type::protobuf};

const auto enum3 = pps::raw_schema_definition{
  R"(
syntax = "proto3";

message Test2 {
  enum Symbols {
    ZERO = 0;
    ONE = 1;
    TWO = 2;
    THREE = 3;
  }
  Symbols symbols = 1;
})",
  pps::schema_type::protobuf};

const auto simple = pps::raw_schema_definition{
  R"(
syntax = "proto3";

message Simple {
  string id = 1;
})",
  pps::schema_type::protobuf};

const auto imported = pps::raw_schema_definition{
  R"(
syntax = "proto3";

import "simple";

message Test2 {
  Simple id =  1;
})",
  pps::schema_type::protobuf};
