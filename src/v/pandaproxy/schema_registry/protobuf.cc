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

#include "pandaproxy/schema_registry/errors.h"
#include "pandaproxy/schema_registry/sharded_store.h"

#include <seastar/core/coroutine.hh>

#include <fmt/ostream.h>
#include <google/protobuf/compiler/parser.h>
#include <google/protobuf/descriptor.h>
#include <google/protobuf/io/tokenizer.h>
#include <google/protobuf/io/zero_copy_stream.h>

namespace pandaproxy::schema_registry {

namespace {

namespace pb = google::protobuf;

}

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

///\brief Build a FileDescriptor using the DescriptorPool.
///
/// Dependencies are required to be in the DescriptorPool.
const pb::FileDescriptor*
build_file(pb::DescriptorPool& dp, const pb::FileDescriptorProto& fdp) {
    dp_error_collector dp_ec;
    if (auto fd = dp.BuildFileCollectingErrors(fdp, &dp_ec); fd) {
        return fd;
    }
    throw as_exception(error_info{error_code::schema_invalid, dp_ec.error()});
}

///\brief Import a schema in the DescriptorPool and return the FileDescriptor.
ss::future<const pb::FileDescriptor*> import_schema(
  pb::DescriptorPool& dp,
  sharded_store& store,
  const canonical_schema& schema) {
    try {
        parser p;
        auto fdp = p.parse(schema);
        co_return build_file(dp, fdp);
    } catch (const exception& e) {
        throw as_exception(invalid_schema(schema));
    }
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

} // namespace pandaproxy::schema_registry
