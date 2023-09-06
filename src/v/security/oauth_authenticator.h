/*
 * Copyright 2023 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */
#pragma once
#include "security/acl.h"
#include "security/sasl_authentication.h"
#include "ssx/fwd.h"

#include <seastar/core/lowres_clock.hh>
#include <seastar/core/sstring.hh>

namespace security {

class oauth_authenticator final : public sasl_mechanism {
public:
    using clock_type = ss::lowres_clock;
    enum class state { init = 0, complete, failed };
    static constexpr const char* name = "OAUTHBEARER";

    oauth_authenticator(
      ss::sstring discovery_url,
      ss::sstring audience,
      clock_type::duration clock_skew_tolerance);
    ~oauth_authenticator() override;

    ss::future<result<bytes>> authenticate(bytes) override;

    bool complete() const override { return _state == state::complete; }
    bool failed() const override { return _state == state::failed; }

    const security::acl_principal& principal() const override {
        return _principal;
    }

private:
    friend std::ostream&
    operator<<(std::ostream& os, oauth_authenticator::state const s);

    security::acl_principal _principal;
    state _state{state::init};
    class impl;
    std::unique_ptr<impl> _impl;
};

} // namespace security
