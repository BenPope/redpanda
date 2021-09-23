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
#include "pandaproxy/schema_registry/schema_util.h"
#include "pandaproxy/schema_registry/store.h"
#include "pandaproxy/schema_registry/types.h"
#include "random/generators.h"
#include "vlog.h"

#include <seastar/core/sleep.hh>
#include <seastar/core/thread.hh>

#include <boost/outcome/success_failure.hpp>
#include <fmt/core.h>
#include <fmt/ostream.h>
#include <fmt/ranges.h>
#include <google/protobuf/compiler/importer.h>
#include <google/protobuf/compiler/parser.h>
#include <google/protobuf/descriptor.h>
#include <google/protobuf/descriptor_database.h>
#include <google/protobuf/io/tokenizer.h>
#include <google/protobuf/io/zero_copy_stream.h>
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

class mf_error_collector final
  : public google::protobuf::compiler::MultiFileErrorCollector {
public:
    // Line and column numbers are zero-based.  A line number of -1 indicates
    // an error with the entire file (e.g. "not found").
    void AddError(
      const std::string& filename,
      int line,
      int column,
      const std::string& message) {
        _msg = fmt::format("{}:{}:{}: {}", filename, line, column, message);
    }

    virtual void AddWarning(
      const std::string& /* filename */,
      int /* line */,
      int /* column */,
      const std::string& /* message */) {}

    error_info error() {
        return error_info(error_code::schema_invalid, std::move(_msg));
    }

private:
    std::string _msg;
};

class schema_def_input_stream
  : public google::protobuf::io::ZeroCopyInputStream {
public:
    explicit schema_def_input_stream(const schema_definition& def)
      : _str(to_string(def))
      , _impl{_str.data(), static_cast<int>(_str.size())} {}

    bool Next(const void** data, int* size) override {
        return _impl.Next(data, size);
    }
    void BackUp(int count) override { return _impl.BackUp(count); }
    bool Skip(int count) override { return _impl.Skip(count); }
    int64_t ByteCount() const override { return _impl.ByteCount(); }

private:
    ss::sstring _str;
    google::protobuf::io::ArrayInputStream _impl;
};

class source_tree final : public google::protobuf::compiler::SourceTree {
public:
    explicit source_tree(store& s)
      : _store{s} {}

    google::protobuf::io::ZeroCopyInputStream*
    Open(const std::string& path) final {
        auto sub = subject{path};

        auto versions = _store.get_versions(sub, include_deleted::no);
        if (versions.has_error()) {
            _last_error = std::move(versions).assume_error();
            return nullptr;
        }

        auto ver = versions.assume_value().back();
        auto s = _store.get_subject_schema(sub, ver, include_deleted::no);
        if (s.has_error()) {
            _last_error = std::move(s).assume_error();
            return nullptr;
        }

        return new schema_def_input_stream{s.assume_value().schema.def};
    }

    std::string GetLastErrorMessage() final { return _last_error.message(); }

    store& get_store() { return _store; }

private:
    store& _store;
    error_info _last_error;
};

struct protobuf_store::impl {
    explicit impl(store& store)
      : _source_tree{store}
      , _error_handler{}
      , _importer{&_source_tree, &_error_handler} {}

    result<protobuf_schema_definition>
    make_schema_definition(const referenced_schema& ref) {
        return protobuf_schema_definition{_importer.Import(ref.sub())};
    }

    result<raw_schema_definition> validate(const referenced_schema& ref) {
        const auto& raw_def = std::get<raw_schema_definition>(ref.def);
        google::protobuf::compiler::Parser p;
        io_error_collector ec;
        schema_def_input_stream is{raw_def};
        google::protobuf::io::Tokenizer t{&is, &ec};
        google::protobuf::FileDescriptorProto fdp;
        auto p_res = p.Parse(&t, &fdp);
        if (!p_res) {
            return error_info{error_code::schema_invalid, ec.error()};
        }
        fdp.set_name(ref.sub());
        vlog(plog.error, "raw def: {}", raw_def);
        vlog(plog.error, "References");
        for (const auto& ref : ref.references) {
            vlog(plog.error, "   {} -> {}", ref.name, ref.sub);
        }
        // vlog(plog.error, "Dependencies");
        auto dependency_size = fdp.dependency_size();
        for (const auto& ref : ref.references) {
            bool found{false};
            for (int i = 0; i < dependency_size; ++i) {
                auto dep = fdp.mutable_dependency(i);
                if (*dep == ref.name) {
                    *dep = ref.sub();
                    found = true;
                    break;
                }
            }
            if (!found) {
                fdp.add_dependency(ref.sub());
            }
        }
        // vlog(plog.error, "Dependencies");
        // for (int i = 0; i < fdp.dependency_size(); ++i) {
        //     auto ifd = fdp.dependency(i);
        //     vlog(plog.error, "   {}", ifd);
        // }
        // vlog(plog.error, "Public dependencies");
        // for (int i = 0; i < fdp.public_dependency_size(); ++i) {
        //     auto ifd = fdp.public_dependency(i);
        //     vlog(plog.error, "   {}", ifd);
        // }
        // vlog(plog.error, "Weak Dependencies");
        // for (int i = 0; i < fdp.weak_dependency_size(); ++i) {
        //     auto ifd = fdp.weak_dependency(i);
        //     vlog(plog.error, "   {}", ifd);
        // }
        // vlog(plog.error, "fdp: \n{}", fdp.DebugString());
        // vlog(plog.error, "fdp: \n{}", fdp.ShortDebugString());

        // auto imp = google::protobuf::compiler::Importer(
        //   &_source_tree, &_error_handler);

        google::protobuf::SimpleDescriptorDatabase sdd;
        if (!sdd.Add(fdp)) {
            return error_info{error_code::schema_invalid, "couldn't add fdp"};
        }

        google::protobuf::compiler::SourceTreeDescriptorDatabase stdd(
          &_source_tree, &sdd);
        dp_error_collector dp_ec;
        google::protobuf::DescriptorPool dp(&stdd, &dp_ec);
        auto fd = dp.FindFileByName(ref.sub());
        if (!fd) {
            return error_info{error_code::schema_invalid, dp_ec.error()};
        }
        vlog(plog.error, "fd: \n{}", fd->DebugString());
        return raw_schema_definition{fd->DebugString(), schema_type::protobuf};
    }

    result<bool> insert(referenced_schema ref) {
        ref.def = BOOST_OUTCOME_TRYX(validate(ref));
        return _source_tree.get_store().insert(std::move(ref)).inserted;
    }

    result<protobuf_schema_definition> get(const subject& sub) {
        auto fd = _importer.Import(sub());
        if (!fd) {
            return _error_handler.error();
        }
        return protobuf_schema_definition{fd};
    }

    source_tree _source_tree;
    mf_error_collector _error_handler;
    google::protobuf::compiler::Importer _importer;
};

protobuf_store::protobuf_store(store& store)
  : _impl{std::make_unique<impl>(store)} {}

protobuf_store::~protobuf_store() = default;

result<bool> protobuf_store::insert(referenced_schema ref) {
    return _impl->insert(std::move(ref));
}

result<protobuf_schema_definition>
protobuf_store::make_schema_definition(const referenced_schema& ref) {
    return _impl->make_schema_definition(ref);
}

result<raw_schema_definition> protobuf_store::validate(referenced_schema ref) {
    return _impl->validate(std::move(ref));
}

result<protobuf_schema_definition> protobuf_store::get(const subject& sub) {
    return _impl->get(sub);
}

bool check_compatible(
  const protobuf_schema_definition& reader,
  const protobuf_schema_definition& writer) {
    return check_compatible(reader._fd, writer._fd);
}

} // namespace pandaproxy::schema_registry
