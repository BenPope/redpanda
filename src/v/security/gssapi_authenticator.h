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
#include "security/acl.h"
#include "security/credential_store.h"
#include "security/gssapi.h"
#include "security/sasl_authentication.h"
#include "security/scram_algorithm.h"
#include "security/types.h"

namespace security {
class gssapi_authenticator final : public sasl_mechanism {
    enum class state { init = 0, more, complete, failed };
    friend std::ostream&
    operator<<(std::ostream& os, gssapi_authenticator::state const s);

public:
    static constexpr const char* name = "GSSAPI";

    explicit gssapi_authenticator(credential_store& credentials)
      : _credentials{credentials} {}

    result<bytes> authenticate(bytes_view) override;

    bool complete() const override { return _state == state::complete; }
    bool failed() const override { return _state == state::failed; }

    const security::acl_principal& principal() const override {
        return _principal;
    }

private:
    result<void> init();
    result<bytes> more(bytes_view);
    result<void> finish();
    [[maybe_unused]] credential_store& _credentials;
    security::acl_principal _principal;
    state _state{state::init};
    gss::cred_id _server_creds;
    gss::ctx_id _context;
    gss::name _client_name;
    gss_OID _doid{};
};

} // namespace security
