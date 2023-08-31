/*
 * Copyright 2023 Redpanda Data, Inc.
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
#include "seastarx.h"
#include "ssx/metrics.h"

#include <seastar/core/lowres_clock.hh>
#include <seastar/net/tls.hh>

#include <chrono>
#include <exception>
#include <iosfwd>

namespace net {

class tls_certificate_probe {
public:
    using clock_type = std::chrono::system_clock;
    tls_certificate_probe() = default;
    tls_certificate_probe(const tls_certificate_probe&) = delete;
    tls_certificate_probe& operator=(const tls_certificate_probe&) = delete;
    tls_certificate_probe(tls_certificate_probe&&) = delete;
    tls_certificate_probe& operator=(tls_certificate_probe&&) = delete;
    ~tls_certificate_probe() = default;

    void loaded(
      const ss::tls::certificate_credentials& creds, std::exception_ptr ex);

    void
    setup_metrics(std::string_view service, std::string_view listener_name);

    void setup_public_metrics(
      std::string_view service, std::string_view listener_name);

private:
    ssx::metrics::metric_groups _metrics
      = ssx::metrics::metric_groups::make_internal();
    ssx::metrics::metric_groups _public_metrics
      = ssx::metrics::metric_groups::make_public();
    clock_type::time_point _load_time;
    clock_type::time_point _cert_expiry_time;
    clock_type::time_point _ca_expiry_time;
    bytes _cert_serial;
    bytes _ca_serial;
    bool _cert_valid{false};

    friend std::ostream&
    operator<<(std::ostream& o, const tls_certificate_probe& p);
};

}; // namespace net
