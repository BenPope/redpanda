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

#include "pandaproxy/logger.h"
#include "pandaproxy/schema_registry/avro.h"
#include "pandaproxy/schema_registry/error.h"
#include "pandaproxy/schema_registry/errors.h"
#include "pandaproxy/schema_registry/fwd.h"
#include "pandaproxy/schema_registry/sharded_store.h"
#include "pandaproxy/schema_registry/types.h"
#include "random/generators.h"
#include "vlog.h"

#include <seastar/core/coroutine.hh>
#include <seastar/core/sleep.hh>
#include <seastar/core/thread.hh>

#include <absl/container/node_hash_map.h>
#include <boost/outcome/success_failure.hpp>
#include <fmt/core.h>
#include <fmt/ostream.h>
#include <fmt/ranges.h>
#include <google/protobuf/compiler/importer.h>
#include <google/protobuf/compiler/parser.h>
#include <google/protobuf/descriptor.h>
#include <google/protobuf/descriptor.pb.h>
#include <google/protobuf/descriptor_database.h>
#include <google/protobuf/io/tokenizer.h>
#include <google/protobuf/io/zero_copy_stream.h>
#include <google/protobuf/io/zero_copy_stream_impl_lite.h>
#include <google/protobuf/message.h>

#include <chrono>
#include <cstddef>
#include <utility>

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

bool check_compatible(
  const pb::FileDescriptor* reader, const pb::FileDescriptor* writer) {
    return check_compatible(reader->message_type(0), writer->message_type(0));
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
        if (_errors.empty()) {
            return "Huh, no error";
        }
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
              //   "{}: subject: '{}', element_name: '{}', descriptor: '{}', "
              //   "location: '{}', msg: '{}'",
              "{}: subject: '{}', element_name: '{}', msg: '{}'",
              e.lvl == level::error ? "error" : "warn",
              e.filename,
              e.element_name,
              //   e.descriptor->DebugString(),
              //   e.location,
              e.message);
            return os;
        }
    };
    std::vector<err> _errors;
};

///\brief Implements ZeroCopyInputStream with a copy of the definition
class schema_def_input_stream : public pb::io::ZeroCopyInputStream {
public:
    explicit schema_def_input_stream(const canonical_schema_definition& def)
      : _str(def.raw())
      , _impl{_str().data(), static_cast<int>(_str().size())} {}

    bool Next(const void** data, int* size) override {
        return _impl.Next(data, size);
    }
    void BackUp(int count) override { return _impl.BackUp(count); }
    bool Skip(int count) override { return _impl.Skip(count); }
    int64_t ByteCount() const override { return _impl.ByteCount(); }

private:
    canonical_schema_definition::raw_string _str;
    pb::io::ArrayInputStream _impl;
};

class parser {
public:
    parser()
      : _parser{}
      , _fdp{} {}

    const pb::FileDescriptorProto& parse(const canonical_schema& schema) {
        schema_def_input_stream is{schema.def()};
        io_error_collector error_collector;
        pb::io::Tokenizer t{&is, &error_collector};

        if (!_parser.Parse(&t, &_fdp)) {
            throw as_exception(
              error_info{error_code::schema_invalid, error_collector.error()});
        }

        _fdp.set_name(schema.sub()());
        return _fdp;
    }

private:
    pb::compiler::Parser _parser;
    pb::FileDescriptorProto _fdp;
};

///\brief Build a FileDescriptor: import references first.
ss::future<const pb::FileDescriptor*> build_file_with_refs(
  pb::DescriptorPool& dp, sharded_store& store, const canonical_schema& schema);

///\brief Build a FileDescriptor: import dependencies first.
ss::future<const pb::FileDescriptor*> build_file_with_deps(
  pb::DescriptorPool& dp, sharded_store& store, const canonical_schema& schema);

ss::future<const pb::FileDescriptor*> import_schema(
  pb::DescriptorPool& dp,
  sharded_store& store,
  const canonical_schema& schema) {
    vlog(plog.info, "\nImporting: {}", schema.sub());
    co_return co_await build_file_with_refs(dp, store, schema);
}

const pb::FileDescriptor*
build_file(pb::DescriptorPool& dp, const pb::FileDescriptorProto& fdp) {
    dp_error_collector dp_ec;
    vlog(plog.info, "BuildFile: {}", fdp.name());
    if (auto fd = dp.BuildFileCollectingErrors(fdp, &dp_ec); fd) {
        return fd;
    }
    throw as_exception(error_info{error_code::schema_invalid, dp_ec.error()});
}

///\brief Import schema
///
/// References are imported into the memory store with their `name` as the
/// `subject`.

ss::future<const pb::FileDescriptor*> build_file_with_refs(
  pb::DescriptorPool& dp,
  sharded_store& store,
  const canonical_schema& schema) {
    vlog(plog.info, "build_with_refs: {}", schema.sub());
    for (const auto& ref : schema.refs()) {
        auto dep = co_await store.get_subject_schema(
          ref.sub, ref.version, include_deleted::no);
        co_await build_file_with_refs(
          dp,
          store,
          canonical_schema{
            subject{ref.name},
            std::move(dep.schema).def(),
            std::move(dep.schema).refs()});
    }
    co_return co_await build_file_with_deps(dp, store, schema);
}

ss::future<const pb::FileDescriptor*> build_file_with_deps(
  pb::DescriptorPool& dp,
  sharded_store& store,
  const canonical_schema& schema) {
    vlog(plog.info, "build_with_deps: {}", schema.sub());
    parser p;
    auto fdp = p.parse(schema);

    const auto dependency_size = fdp.dependency_size();
    for (int i = 0; i < dependency_size; ++i) {
        auto sub = subject{fdp.dependency(i)};
        if (auto fd = dp.FindFileByName(sub()); !fd) {
            auto sub_schema = co_await store.get_subject_schema(
              sub, schema_version{1}, include_deleted::no);

            co_await build_file_with_refs(
              dp,
              store,
              canonical_schema{
                sub,
                std::move(sub_schema.schema).def(),
                std::move(sub_schema.schema).refs()});
        }
    }

    co_return build_file(dp, fdp);
}

struct protobuf_schema_definition::impl {
    pb::DescriptorPool _dp;
    const pb::FileDescriptor* fd{};
};

canonical_schema_definition::raw_string
protobuf_schema_definition::raw() const {
    return canonical_schema_definition::raw_string{_impl->fd->DebugString()};
}

bool operator==(
  const protobuf_schema_definition& lhs,
  const protobuf_schema_definition& rhs) {
    return lhs.raw() == rhs.raw();
}

std::ostream&
operator<<(std::ostream& os, const protobuf_schema_definition& def) {
    fmt::print(
      os, "type: {}, definition: {}", to_string_view(def.type()), def.raw()());
    return os;
}

ss::future<protobuf_schema_definition> make_protobuf_schema_definition(
  sharded_store& store, const canonical_schema& schema) {
    auto impl = ss::make_shared<protobuf_schema_definition::impl>();
    impl->fd = co_await import_schema(impl->_dp, store, schema);
    co_return protobuf_schema_definition{std::move(impl)};
}

ss::future<canonical_schema_definition>
validate_protobuf_schema(sharded_store& store, const canonical_schema& schema) {
    auto res = co_await make_protobuf_schema_definition(store, schema);
    co_return canonical_schema_definition{std::move(res)};
}

ss::future<canonical_schema>
make_canonical_protobuf_schema(sharded_store& store, unparsed_schema schema) {
    canonical_schema temp{
      std::move(schema).sub(),
      {canonical_schema_definition::raw_string{schema.def().raw()()},
       schema.def().type()},
      std::move(schema).refs()};

    auto validated = co_await validate_protobuf_schema(store, temp);
    co_return canonical_schema{
      std::move(temp).sub(), std::move(validated), std::move(temp).refs()};
}

bool check_compatible(
  const protobuf_schema_definition& reader,
  const protobuf_schema_definition& writer) {
    return check_compatible(reader().fd, writer().fd);
}

} // namespace pandaproxy::schema_registry
