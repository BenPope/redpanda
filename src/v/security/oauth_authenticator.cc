/*
 * Copyright 2023 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */
#include "security/oauth_authenticator.h"

#include "bytes/bytes.h"
#include "bytes/iobuf_parser.h"
#include "cluster/fwd.h"
#include "config/configuration.h"
#include "http/client.h"
#include "json/document.h"
#include "kafka/protocol/wire.h"
#include "net/transport.h"
#include "security/acl.h"
#include "security/errc.h"
#include "security/logger.h"
#include "utils/base64.h"
#include "utils/string_switch.h"
#include "vlog.h"

#include <seastar/http/client.hh>

#include <absl/algorithm/container.h>
#include <boost/algorithm/string/split.hpp>
#include <boost/beast/http/verb.hpp>
#include <boost/outcome/success_failure.hpp>
#include <boost/url/parse.hpp>
#include <boost/url/url.hpp>
#include <cryptopp/base64.h>
#include <fmt/chrono.h>
#include <fmt/core.h>
#include <fmt/ranges.h>

#include <functional>
#include <iterator>
#include <string_view>

namespace security {

std::ostream& operator<<(std::ostream& os, oauth_authenticator::state const s) {
    using state = oauth_authenticator::state;
    switch (s) {
    case state::init:
        return os << "init";
    case state::complete:
        return os << "complete";
    case state::failed:
        return os << "failed";
    }
}

// impl runs on the thread_worker
class oauth_authenticator::impl {
public:
    using state = oauth_authenticator::state;

    template<typename R>
    struct state_result {
        oauth_authenticator::state state;
        result<R> result;
    };

    impl(
      ss::sstring discovery_url,
      ss::sstring audience,
      clock_type::duration clock_skew_tolerance)
      : _discovery_url{std::move(discovery_url)}
      , _audience{std::move(audience)}
      , _clock_skew_tolerance{std::move(clock_skew_tolerance)} {}

    state_result<bytes> authenticate(bytes auth_bytes);
    const security::acl_principal& principal() const {
        return _rp_user_principal;
    }

    void reset() {}

private:
    state_result<void> init();

    ss::sstring _discovery_url;
    ss::sstring _audience;
    clock_type::duration _clock_skew_tolerance;
    state _state{state::init};
    security::acl_principal _rp_user_principal;
};

oauth_authenticator::oauth_authenticator(
  ss::sstring discovery_url,
  ss::sstring audience,
  clock_type::duration clock_skew_tolerance)
  : _impl{std::make_unique<impl>(
    std::move(discovery_url), std::move(audience), clock_skew_tolerance)} {}

oauth_authenticator::~oauth_authenticator() = default;

ss::future<result<bytes>> oauth_authenticator::authenticate(bytes auth_bytes) {
    if (!_impl) {
        vlog(
          seclog.error,
          "authenticate received after handshake complete {} bytes {}",
          _state,
          auth_bytes.size());
        co_return errc::invalid_scram_state;
    }

    vlog(
      seclog.info,
      "oauth {} authenticate received {} bytes",
      _state,
      auth_bytes.size());

    auto auth_str = std::string_view(
      reinterpret_cast<char*>(auth_bytes.data()), auth_bytes.size());

    constexpr std::string_view sasl_header{"n,,\1auth=Bearer "};
    if (!auth_str.starts_with(sasl_header)) {
        vlog(seclog.error, "invalid sasl_header");
        _state = state::failed;
        co_return errc::invalid_credentials;
    }
    auth_str = auth_str.substr(sasl_header.length());
    if (!auth_str.ends_with("\1\1")) {
        vlog(seclog.error, "invalid sasl_header");
        _state = state::failed;
        co_return errc::invalid_credentials;
    }
    auth_str = auth_str.substr(0, auth_str.length() - 2);
    vlog(seclog.info, "auth_bytes: {}", auth_str);

    std::vector<std::string_view> jose_enc;
    jose_enc.reserve(3);
    boost::algorithm::split(
      jose_enc, auth_str, [](char c) { return c == '.'; });
    vlog(seclog.info, "jose_b64: {}", jose_enc);

    auto base64_url_decode = [](std::string_view sv) {
        CryptoPP::Base64URLDecoder decoder;

        decoder.Put((CryptoPP::byte*)sv.data(), sv.size());
        decoder.MessageEnd();
        std::string decoded;
        CryptoPP::word64 size = decoder.MaxRetrievable();
        if (size && size <= SIZE_MAX) {
            decoded.resize(size);
            decoder.Get((CryptoPP::byte*)decoded.data(), decoded.size());
        }
        return decoded;
    };

    auto jose_header_str = base64_url_decode(jose_enc[0]);
    auto jose_payload_str = base64_url_decode(jose_enc[1]);

    vlog(
      seclog.info,
      "jose_header: {}, jose_payload: {}",
      jose_header_str,
      jose_payload_str);

    json::Document jose_header;
    if (jose_header.Parse(jose_header_str).HasParseError()) {
        vlog(seclog.error, "invalid jose_header");
        _state = state::failed;
        co_return errc::invalid_credentials;
    }

    if (!jose_header.IsObject()) {
        vlog(seclog.error, "invalid jose_header");
        _state = state::failed;
        co_return errc::invalid_credentials;
    }

    const auto get_member = [](auto const& doc, std::string_view name) {
        std::string_view val;
        if (auto it = doc.FindMember(name.data()); it == doc.MemberEnd()) {
            return val;
        } else if (!it->value.IsString()) {
            return val;
        } else {
            val = {it->value.GetString(), it->value.GetStringLength()};
        }
        return val;
    };

    auto alg = get_member(jose_header, "alg");
    if (alg.empty()) {
        vlog(seclog.error, "empty alg");
        _state = state::failed;
        co_return errc::invalid_credentials;
    }

    if (alg == "RS256") {
    } else {
        vlog(seclog.error, "unsupported alg: {}", alg);
        _state = state::failed;
        co_return errc::invalid_credentials;
    }

    if (auto typ = get_member(jose_header, "typ"); typ != "JWT") {
        vlog(seclog.error, "invalid typ: {}", typ);
        _state = state::failed;
        co_return errc::invalid_credentials;
    }

    auto kid = get_member(jose_header, "kid");
    if (kid.empty()) {
        vlog(seclog.error, "empty kid");
        _state = state::failed;
        co_return errc::invalid_credentials;
    }

    json::Document jose_payload;
    if (jose_payload.Parse(jose_payload_str).HasParseError()) {
        vlog(seclog.error, "invalid jose_payload");
        _state = state::failed;
        co_return errc::invalid_credentials;
    }

    // {
    //   "iss":"https://dev-ltxchcls4igzho78.us.auth0.com/",
    //   "sub":"3JJeI4tmMC6v8mCVCSDnAGVf2vrnJ0BT@clients",
    //   "aud":"localhost",
    //   "iat":1694008485,
    //   "exp":1694094885,
    //   "azp":"3JJeI4tmMC6v8mCVCSDnAGVf2vrnJ0BT",
    //   "gty":"client-credentials"
    // }
    auto iss = get_member(jose_payload, "iss");
    if (iss.empty()) {
        vlog(seclog.error, "empty iss");
        _state = state::failed;
        co_return errc::invalid_credentials;
    }

    boost::urls::url_view discovery_url{
      std::string_view(config::shard_local_cfg().oidc_discovery_url.value())};

    uint16_t disco_port = discovery_url.port_number();
    if (discovery_url.port().empty()) {
        disco_port = discovery_url.scheme() == "https" ? 443 : 80;
    }

    vlog(
      seclog.info,
      "discovery_url: {}://{}:{}{}",
      discovery_url.scheme(),
      discovery_url.encoded_host(),
      disco_port,
      discovery_url.path());

    ss::tls::credentials_builder builder;
    builder.set_client_auth(ss::tls::client_auth::NONE);
    co_await builder.set_system_trust();

    constexpr auto sv_to_sv = [](auto sv) -> std::string_view {
        return {sv.data(), sv.size()};
    };

    auto creds = builder.build_certificate_credentials();
    creds->set_dn_verification_callback(
      [](ss::tls::session_type type, ss::sstring subject, ss::sstring issuer) {
          vlog(
            seclog.info,
            "type: ?, subject: {}, issuer: {}",
            (uint8_t)type,
            subject,
            issuer);
      });
    http::client client{net::base_transport::configuration{
      .server_addr
      = {ss::sstring(sv_to_sv(discovery_url.encoded_host())), disco_port},
      .credentials = creds}};

    http::client::request_header req_hdr;
    req_hdr.method(boost::beast::http::verb::get);
    req_hdr.target(sv_to_sv(discovery_url.encoded_path()));
    req_hdr.insert(boost::beast::http::field::accept, "*/*");
    req_hdr.insert(
      boost::beast::http::field::host, discovery_url.encoded_host());

    vlog(seclog.info, "make_request");
    auto res = co_await client.request(std::move(req_hdr));

    ss::sstring response_body;
    while (!res->is_done()) {
        vlog(seclog.info, "!res->is_done()");
        iobuf buf = co_await res->recv_some();
        vlog(seclog.info, "some recv");
        for (auto& fragm : buf) {
            response_body.append(fragm.get(), fragm.size());
        }
    }

    vlog(seclog.warn, "response_body: {}", response_body);
    json::Document well_known;
    if (well_known.Parse(response_body).HasParseError()) {
        vlog(
          seclog.error,
          "invalid response from discovery_url: {}",
          response_body);
        _state = state::failed;
        co_return errc::invalid_credentials;
    }

    if (auto issuer = get_member(well_known, "issuer"); issuer != iss) {
        vlog(
          seclog.error, "invalid issuer: iss: {}, expected: {}", iss, issuer);
        _state = state::failed;
        co_return errc::invalid_credentials;
    }

    std::string_view jwks_uri = get_member(well_known, "jwks_uri");
    if (jwks_uri.empty()) {
        vlog(seclog.error, "jwks_uri not found in well_known");
        _state = state::failed;
        co_return errc::invalid_credentials;
    }

    http::client::request_header jwks_req_hdr;
    jwks_req_hdr.method(boost::beast::http::verb::get);
    jwks_req_hdr.target(sv_to_sv(jwks_uri));
    jwks_req_hdr.insert(boost::beast::http::field::accept, "*/*");
    jwks_req_hdr.insert(
      boost::beast::http::field::host, discovery_url.encoded_host());

    vlog(seclog.info, "make_request");
    res = co_await client.request(std::move(jwks_req_hdr));

    response_body = {};
    while (!res->is_done()) {
        vlog(seclog.info, "!res->is_done()");
        iobuf buf = co_await res->recv_some();
        vlog(seclog.info, "some recv");
        for (auto& fragm : buf) {
            response_body.append(fragm.get(), fragm.size());
        }
    }

    vlog(seclog.warn, "jwks response_body: {}", response_body);

    json::Document jwks;
    if (jwks.Parse(response_body).HasParseError()) {
        vlog(seclog.error, "invalid response from jwks_uri: {}", response_body);
        _state = state::failed;
        co_return errc::invalid_credentials;
    }

    if (!jwks.IsObject()) {
        vlog(
          seclog.error,
          "invalid response from jwks_uri, not object {}",
          jwks_uri);
        _state = state::failed;
        co_return errc::invalid_credentials;
    }

    json::Document::MemberIterator jwks_keys;
    if (jwks_keys = jwks.FindMember("keys"); jwks_keys == jwks.MemberEnd()) {
        vlog(seclog.error, "invalid response from jwks_uri, no keys");
        _state = state::failed;
        co_return errc::invalid_credentials;
    }

    if (!jwks_keys->value.IsArray()) {
        vlog(seclog.error, "invalid response from jwks_uri, keys not array");
        _state = state::failed;
        co_return errc::invalid_credentials;
    }

    bool kid_found{false};
    for (auto k = jwks_keys->value.Begin();
         !kid_found && k != jwks_keys->value.End();
         ++k) {
        if (!k->IsObject()) {
            vlog(
              seclog.error, "invalid response from jwks_uri, key not object");
            _state = state::failed;
            co_return errc::invalid_credentials;
        }
        auto jwks_kid = get_member(*k, "kid");
        if (kid == jwks_kid) {
            kid_found = true;
        }
    }

    if (!kid_found) {
        vlog(seclog.error, "invalid kid in JWT");
        _state = state::failed;
        co_return errc::invalid_credentials;
    }

    auto sub = get_member(jose_payload, "sub");
    if (sub.empty()) {
        vlog(seclog.error, "empty sub");
        _state = state::failed;
        co_return errc::invalid_credentials;
    }

    _state = state::complete;
    _principal = acl_principal{principal_type::user, ss::sstring(sub)};

    vlog(
      seclog.info,
      "alg: {}, kid: {}, iss: {}, principal: {}",
      alg,
      kid,
      iss,
      _principal);

    co_return bytes{};
}

oauth_authenticator::impl::state_result<bytes>
oauth_authenticator::impl::authenticate(bytes) {
    switch (_state) {
    case state::init: {
        auto res = init();
        if (res.result.has_error()) {
            return {res.state, res.result.assume_error()};
        }
    }
    case state::complete:
    case state::failed:
        break;
    }

    vlog(seclog.info, "gss {} authenticate failed", _state);
    return {_state, errc::invalid_gssapi_state};
}

oauth_authenticator::impl::state_result<void>
oauth_authenticator::impl::init() {
    vlog(
      seclog.info,
      "oauth {} authenticate skew {}",
      _state,
      _clock_skew_tolerance);
    _state = state::failed;
    return {_state, outcome::success()};
}

} // namespace security
