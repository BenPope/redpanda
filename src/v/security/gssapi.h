/*
 * Copyright 2022 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */
#pragma once

#include "bytes/bytes.h"

#include <gssapi/gssapi.h>

#include <string_view>

namespace security::gss {

struct buffer_view : ::gss_buffer_desc {
    explicit buffer_view(std::string_view sv)
      : ::gss_buffer_desc{sv.length(), const_cast<char*>(sv.data())} {}
    explicit buffer_view(bytes_view bv)
      : ::gss_buffer_desc{bv.length(), const_cast<unsigned char*>(bv.data())} {}

    explicit operator std::string_view() const {
        return {static_cast<char*>(value), length};
    };
    explicit operator bytes_view() const {
        return {static_cast<unsigned char*>(value), length};
    };
};

struct buffer : buffer_view {
    buffer()
      : buffer_view{std::string_view{}} {}
    using buffer_view::buffer_view;
    buffer(buffer&&) = delete;
    buffer(const buffer&) = delete;
    buffer operator=(buffer&&) = delete;
    buffer operator=(buffer const&) = delete;

    ~buffer() {
        if (value != nullptr) {
            OM_uint32 minor;
            gss_release_buffer(&minor, this);
        }
    }
};

struct name {
    gss_name_t _impl{nullptr};

    name() = default;
    name(name&&) = delete;
    name(const name&) = delete;
    name operator=(name&&) = delete;
    name operator=(name const&) = delete;

    ~name() {
        if (_impl != nullptr) {
            OM_uint32 minor;
            gss_release_name(&minor, &_impl);
        }
    }

    operator gss_name_t() { return _impl; };
    gss_name_t* operator&() { return &_impl; };
};

struct cred_id {
    gss_cred_id_t _impl{nullptr};

    cred_id() = default;
    cred_id(cred_id&&) = delete;
    cred_id(const cred_id&) = delete;
    cred_id operator=(cred_id&&) = delete;
    cred_id operator=(cred_id const&) = delete;

    ~cred_id() {
        if (_impl != nullptr) {
            OM_uint32 minor;
            gss_release_cred(&minor, &_impl);
        }
    }

    operator gss_cred_id_t() { return _impl; };
    gss_cred_id_t* operator&() { return &_impl; };
};

struct ctx_id {
    gss_ctx_id_t _impl{nullptr};

    ctx_id() = default;
    ctx_id(ctx_id&&) = delete;
    ctx_id(const ctx_id&) = delete;
    ctx_id operator=(ctx_id&&) = delete;
    ctx_id operator=(ctx_id const&) = delete;

    ~ctx_id() {
        if (_impl != nullptr) {
            OM_uint32 minor;
            gss_delete_sec_context(&minor, &_impl, GSS_C_NO_BUFFER);
        }
    }

    operator gss_ctx_id_t() { return _impl; };
    gss_ctx_id_t* operator&() { return &_impl; };
};

} // namespace security::gss
