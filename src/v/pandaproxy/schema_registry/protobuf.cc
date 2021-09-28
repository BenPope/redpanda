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

///\brief Implements ZeroCopyInputStream with a copy of the definition
class schema_def_input_stream
  : public google::protobuf::io::ZeroCopyInputStream {
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
    google::protobuf::io::ArrayInputStream _impl;
};

///\brief Implements SourceTree backed by a store
class store_source_tree final : public google::protobuf::compiler::SourceTree {
public:
    explicit store_source_tree(store& s)
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

        return new schema_def_input_stream{s.assume_value().schema.def()};
    }

    std::string GetLastErrorMessage() final { return _last_error.message(); }

    store& get_store() { return _store; }

private:
    store& _store;
    error_info _last_error;
};

///\brief Implements SourceTree backed by a map
class memory_source_tree final : public google::protobuf::compiler::SourceTree {
public:
    google::protobuf::io::ZeroCopyInputStream*
    Open(const std::string& path) final {
        auto it = _schema.find(path);
        if (it == _schema.end()) {
            _last_error = fmt::format("Schema {} not found", path);
            return nullptr;
        }

        return new google::protobuf::io::ArrayInputStream{
          it->second().data(),
          static_cast<int>(it->second().length()),
        };
    }

    std::string GetLastErrorMessage() final { return _last_error; }

    bool insert(canonical_schema schema) {
        return _schema
          .emplace(std::move(schema).sub()(), std::move(schema).def().raw())
          .second;
    }

private:
    using raw_string = canonical_schema_definition::raw_string;
    absl::node_hash_map<std::string, raw_string> _schema;
    std::string _last_error;
};

///\brief Implements SourceTree backed by a map, with fallback to a store.
class fallback_source_tree final
  : public google::protobuf::compiler::SourceTree {
public:
    explicit fallback_source_tree(store& s)
      : _mem_st{}
      , _store_st{s} {}

    bool insert(canonical_schema schema) {
        return _mem_st.insert(std::move(schema));
    }

    store& get_store() { return _store_st.get_store(); }

    google::protobuf::io::ZeroCopyInputStream*
    Open(const std::string& path) final {
        if (auto os = _mem_st.Open(path); os != nullptr) {
            return os;
        }
        return _store_st.Open(path);
    }

    std::string GetLastErrorMessage() final {
        return _store_st.GetLastErrorMessage();
    }

    memory_source_tree _mem_st;
    store_source_tree _store_st;
};

///\brief Import schema
///
/// References are imported into the memory store with their `name` as the
/// `subject`.
class importer {
public:
    explicit importer(store& s)
      : _source_tree{s}
      , _mf_ec{}
      , _importer(&_source_tree, &_mf_ec) {}

    result<const google::protobuf::FileDescriptor*>
    import(canonical_schema schema) {
        auto deps = insert_with_refs(schema);
        if (deps.has_error()) {
            return deps.assume_error();
        }

        auto fd = _importer.Import(schema.sub()());
        if (!fd) {
            return _mf_ec.error();
        }
        return fd;
    }

private:
    result<void> insert_with_refs(canonical_schema schema) {
        for (const auto& ref : schema.refs()) {
            auto dep = _source_tree.get_store().get_subject_schema(
              ref.sub, ref.version, include_deleted::no);
            if (dep.has_error()) {
                return dep.assume_error();
            }
            auto& ref_schema = dep.assume_value().schema;
            auto fd = insert_with_refs(canonical_schema{
              subject{ref.name},
              std::move(ref_schema).def(),
              std::move(ref_schema).refs()});
        }

        if (!_source_tree.insert(std::move(schema))) {
            return error_info{
              error_code::schema_invalid, "Schema references aren't unique"};
        }
        return outcome::success();
    }

    fallback_source_tree _source_tree;
    mf_error_collector _mf_ec;
    google::protobuf::compiler::Importer _importer;
};

struct protobuf_schema_definition::impl {
    explicit impl(store& s)
      : ctx{s} {}

    importer ctx;
    const google::protobuf::FileDescriptor* fd{};
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

result<protobuf_schema_definition>
protobuf_store::make_schema_definition(const canonical_schema& schema) {
    auto impl = ss::make_shared<protobuf_schema_definition::impl>(_store);
    auto res = impl->ctx.import(schema);
    if (res.has_error()) {
        return res.assume_error();
    }
    impl->fd = res.assume_value();
    return protobuf_schema_definition{std::move(impl)};
}

result<canonical_schema_definition>
protobuf_store::validate_schema(const canonical_schema& schema) {
    auto res = make_schema_definition(schema);
    if (res.has_error()) {
        return res.assume_error();
    }
    return canonical_schema_definition{std::move(res).assume_value()};
}

result<canonical_schema>
protobuf_store::make_canonical_schema(unparsed_schema schema) {
    canonical_schema hack{
      std::move(schema).sub(),
      {canonical_schema_definition::raw_string{schema.def().raw()()},
       schema.def().type()},
      std::move(schema).refs()};

    auto canonical = BOOST_OUTCOME_TRYX(validate_schema(hack));

    return canonical_schema{
      std::move(hack).sub(), std::move(canonical), std::move(hack).refs()};
}

bool check_compatible(
  const protobuf_schema_definition& reader,
  const protobuf_schema_definition& writer) {
    return check_compatible(reader().fd, writer().fd);
}

} // namespace pandaproxy::schema_registry
