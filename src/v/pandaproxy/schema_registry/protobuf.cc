/*
 * Copyright 2021 Vectorized, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#include "pandaproxy/schema_registry/protobuf.h"

#include "pandaproxy/schema_registry/avro.h"
#include "pandaproxy/schema_registry/error.h"
#include "pandaproxy/schema_registry/types.h"
#include "random/generators.h"

#include <fmt/core.h>
#include <fmt/ostream.h>
#include <fmt/ranges.h>
#include <google/protobuf/compiler/parser.h>
#include <google/protobuf/io/tokenizer.h>
#include <google/protobuf/message.h>

namespace pandaproxy::schema_registry {

namespace {

namespace pb = google::protobuf;
bool check_compatible(
  const pb::Descriptor* reader, const pb::Descriptor* writer);

enum class encoding {
    struct_ = 0,
    varint,
    zigzag,
    bytes,
    int32,
    int64,
    float_,
    double_,
};

encoding get_encoding(pb::FieldDescriptor::Type type) {
    switch (type) {
    case pb::FieldDescriptor::Type::TYPE_MESSAGE:
    case pb::FieldDescriptor::Type::TYPE_GROUP:
        return encoding::struct_;
    case pb::FieldDescriptor::Type::TYPE_FLOAT:
        return encoding::float_;
    case pb::FieldDescriptor::Type::TYPE_DOUBLE:
        return encoding::double_;
    case pb::FieldDescriptor::Type::TYPE_INT64:
    case pb::FieldDescriptor::Type::TYPE_UINT64:
    case pb::FieldDescriptor::Type::TYPE_INT32:
    case pb::FieldDescriptor::Type::TYPE_UINT32:
    case pb::FieldDescriptor::Type::TYPE_BOOL:
    case pb::FieldDescriptor::Type::TYPE_ENUM:
        return encoding::varint;
    case pb::FieldDescriptor::Type::TYPE_SINT32:
    case pb::FieldDescriptor::Type::TYPE_SINT64:
        return encoding::zigzag;
    case pb::FieldDescriptor::Type::TYPE_STRING:
    case pb::FieldDescriptor::Type::TYPE_BYTES:
        return encoding::bytes;
    case pb::FieldDescriptor::Type::TYPE_FIXED32:
    case pb::FieldDescriptor::Type::TYPE_SFIXED32:
        return encoding::int32;
    case pb::FieldDescriptor::Type::TYPE_FIXED64:
    case pb::FieldDescriptor::Type::TYPE_SFIXED64:
        return encoding::int64;
    }
    __builtin_unreachable();
}

bool check_compatible(encoding reader, encoding writer) {
    return reader == writer && reader != encoding::struct_;
}

bool check_compatible(
  const pb::FieldDescriptor* reader, const pb::FieldDescriptor* writer) {
    switch (reader->type()) {
    case pb::FieldDescriptor::Type::TYPE_MESSAGE:
    case pb::FieldDescriptor::Type::TYPE_GROUP:
        return check_compatible(reader->message_type(), writer->message_type());
    case pb::FieldDescriptor::Type::TYPE_FLOAT:
    case pb::FieldDescriptor::Type::TYPE_DOUBLE:
    case pb::FieldDescriptor::Type::TYPE_INT64:
    case pb::FieldDescriptor::Type::TYPE_UINT64:
    case pb::FieldDescriptor::Type::TYPE_INT32:
    case pb::FieldDescriptor::Type::TYPE_UINT32:
    case pb::FieldDescriptor::Type::TYPE_BOOL:
    case pb::FieldDescriptor::Type::TYPE_ENUM:
    case pb::FieldDescriptor::Type::TYPE_SINT32:
    case pb::FieldDescriptor::Type::TYPE_SINT64:
    case pb::FieldDescriptor::Type::TYPE_STRING:
    case pb::FieldDescriptor::Type::TYPE_BYTES:
    case pb::FieldDescriptor::Type::TYPE_FIXED32:
    case pb::FieldDescriptor::Type::TYPE_SFIXED32:
    case pb::FieldDescriptor::Type::TYPE_FIXED64:
    case pb::FieldDescriptor::Type::TYPE_SFIXED64:
        return check_compatible(
          get_encoding(reader->type()), get_encoding(writer->type()));
    }
    __builtin_unreachable();
}

bool check_compatible(
  const pb::Descriptor* reader, const pb::Descriptor* writer) {
    if (reader->well_known_type() != writer->well_known_type()) {
        return false;
    }
    if (reader->field_count() != writer->field_count()) {
        return false;
    }
    for (int i = 0; i < reader->field_count(); ++i) {
        if (!check_compatible(reader->field(i), writer->field(i))) {
            return false;
        }
    }
    return true;
}

} // namespace

class io_error_collector final : public pb::io::ErrorCollector {
    enum class level {
        error,
        warn,
    };
    struct err {
        level lvl;
        int line;
        int column;
        ss::sstring message;
        friend std::ostream& operator<<(std::ostream& os, const err& e) {
            fmt::print(
              os,
              "{}: line: '{}', col: '{}', msg: '{}'",
              e.lvl == level::error ? "error" : "warn",
              e.line,
              e.column,
              e.message);
            return os;
        }
    };

public:
    void AddError(int line, int column, const std::string& message) final {
        _errors.emplace_back(err{level::error, line, column, message});
    }
    void AddWarning(int line, int column, const std::string& message) final {
        _errors.emplace_back(err{level::warn, line, column, message});
    }

    ss::sstring error() const {
        // return ssx::sformat("{}", fmt::join(_errors, "; "));
        return ssx::sformat("{}", _errors[0]);
    }

private:
    std::vector<err> _errors;
};

class dp_error_collector final : public pb::DescriptorPool::ErrorCollector {
public:
    void AddError(
      const std::string& filename,
      const std::string& element_name,
      const pb::Message* descriptor,
      ErrorLocation location,
      const std::string& message) final {
        _errors.emplace_back(err{
          level::error, filename, element_name, descriptor, location, message});
    }
    void AddWarning(
      const std::string& filename,
      const std::string& element_name,
      const pb::Message* descriptor,
      ErrorLocation location,
      const std::string& message) final {
        _errors.emplace_back(err{
          level::warn, filename, element_name, descriptor, location, message});
    }

    ss::sstring error() const {
        // return ssx::sformat("{}", fmt::join(_errors, "; "));
        return ssx::sformat("{}", _errors[0]);
    }

private:
    enum class level {
        error,
        warn,
    };
    struct err {
        level lvl;
        std::string filename;
        std::string element_name;
        const pb::Message* descriptor;
        ErrorLocation location;
        std::string message;
        friend std::ostream& operator<<(std::ostream& os, const err& e) {
            fmt::print(
              os,
              "{}: subject: '{}', element_name: '{}', descriptor: '{}', "
              "location: '{}', msg: '{}'",
              e.lvl == level::error ? "error" : "warn",
              e.filename,
              e.element_name,
              e.descriptor->DebugString(),
              e.location,
              e.message);
            return os;
        }
    };
    std::vector<err> _errors;
};

result<protobuf_schema_definition>
make_protobuf_schema_definition(std::string_view sv) {
    // TODO(Ben): Lifetime
    static pb::DescriptorPool pool;
    io_error_collector io_ec;
    dp_error_collector dp_ec;
    pb::io::ArrayInputStream str{sv.data(), static_cast<int>(sv.size())};

    pb::io::Tokenizer tok{&str, &io_ec};
    pb::FileDescriptorProto fdp;
    pb::compiler::Parser p;
    p.RecordErrorsTo(&io_ec);
    if (!p.Parse(&tok, &fdp)) {
        return error_info{error_code::schema_invalid, io_ec.error()};
    }
    fdp.set_name(random_generators::gen_alphanum_string(16));
    auto fd = pool.BuildFileCollectingErrors(fdp, &dp_ec);
    if (!fd) {
        return error_info{error_code::schema_invalid, dp_ec.error()};
    }
    if (fd->message_type_count() != 1) {
        return error_info{error_code::schema_invalid, "too many messages"};
    }
    return protobuf_schema_definition{fd};
}

bool check_compatible(
  const protobuf_schema_definition&, const protobuf_schema_definition&) {
    return true; // check_compatible(reader(), writer());
}

} // namespace pandaproxy::schema_registry
