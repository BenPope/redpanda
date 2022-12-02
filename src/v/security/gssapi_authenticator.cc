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
#include "security/gssapi_authenticator.h"

#include "bytes/bytes.h"
#include "kafka/protocol/request_reader.h"
#include "security/acl.h"
#include "security/errc.h"
#include "security/logger.h"
#include "vlog.h"

#include <boost/outcome/basic_outcome.hpp>
#include <boost/outcome/success_failure.hpp>
#include <gssapi/gssapi.h>

#include <string_view>

namespace security {

std::ostream&
operator<<(std::ostream& os, gssapi_authenticator::state const s) {
    using state = gssapi_authenticator::state;
    switch (s) {
    case state::init:
        return os << "init";
    case state::more:
        return os << "more";
    case state::complete:
        return os << "complete";
    case state::failed:
        return os << "failed";
    }
}

static void display_status_1(std::string_view m, OM_uint32 code, int type) {
    while (true) {
        OM_uint32 msg_ctx{0};
        OM_uint32 min_stat{};
        gss::buffer msg;
        auto maj_stat = gss_display_status(
          &min_stat, code, type, GSS_C_NO_OID, &msg_ctx, &msg);
        if (maj_stat != GSS_S_COMPLETE) {
            vlog(seclog.info, "gss status from {}", m);
            break;
        } else {
            vlog(seclog.info, "GSS_API error {}: {}", m, msg);
        }

        if (!msg_ctx) {
            break;
        }
    }
}

/*
 * Function: display_status
 *
 * Purpose: displays GSS-API messages
 *
 * Arguments:
 *
 *      msg             a string to be displayed with the message
 *      maj_stat        the GSS-API major status code
 *      min_stat        the GSS-API minor status code
 *
 * Effects:
 *
 * The GSS-API messages associated with maj_stat and min_stat are
 * displayed on stderr, each preceeded by "GSS-API error <msg>:
" and
 * followed by a newline.
 */
void display_status(const char* msg, OM_uint32 maj_stat, OM_uint32 min_stat) {
    display_status_1(msg, maj_stat, GSS_C_GSS_CODE);
    display_status_1(msg, min_stat, GSS_C_MECH_CODE);
}

result<bytes> gssapi_authenticator::authenticate(bytes_view auth_bytes) {
    vlog(seclog.info, "gss {} authenticate", _state);
    switch (_state) {
    case state::init:
        if (auto res = init(); res.has_error()) {
            vlog(seclog.info, "gss {} authenticate failed", _state);
            _state = state::failed;
            return res.assume_error();
        }
        [[fallthrough]];
    case state::more: {
        auto res = more(auth_bytes);
        if (res.has_error()) {
            vlog(seclog.info, "gss {} authenticate failed", _state);
            _state = state::failed;
            return res.assume_error();
        }
        return res;
    }
    default:
        break;
    }
    vlog(seclog.info, "gss {} authenticate failed", _state);
    _state = state::failed;
    return errc::invalid_scram_state;
}

result<void> gssapi_authenticator::init() {
    OM_uint32 minor_status{};
    OM_uint32 major_status{};

    gss::buffer_view service_name{"redpanda"};
    gss::name server_name{};

    major_status = ::gss_import_name(
      &minor_status, &service_name, GSS_C_NT_HOSTBASED_SERVICE, &server_name);

    vlog(
      seclog.info,
      "gss {} import name {}:{}",
      _state,
      major_status,
      minor_status);
    if (major_status != GSS_S_COMPLETE) {
        display_status("import name", major_status, minor_status);
        _state = state::failed;
        return errc::invalid_scram_state;
    }

    // seems to have read from file
    major_status = ::gss_acquire_cred(
      &minor_status,
      server_name,
      0,
      GSS_C_NO_OID_SET,
      GSS_C_ACCEPT,
      &_server_creds,
      NULL,
      NULL);

    vlog(
      seclog.info,
      "gss {} acquire cred {}:{}",
      _state,
      major_status,
      minor_status);
    if (major_status != GSS_S_COMPLETE) {
        display_status("gss acquire cred", major_status, minor_status);
        _state = state::failed;
        return errc::invalid_scram_state;
    }

    gss::buffer display_name;
    major_status = ::gss_display_name(
      &minor_status, server_name, &display_name, &_doid);
    if (major_status != GSS_S_COMPLETE) {
        display_status("gss server name", major_status, minor_status);
    } else {
        vlog(
          seclog.info,
          "gss {} server name {}:{} = {}",
          _state,
          major_status,
          minor_status,
          std::string_view{display_name});
    }
    return outcome::success();
}

result<bytes> gssapi_authenticator::more(bytes_view auth_bytes) {
    gss::buffer_view recv_tok{auth_bytes};
    gss::buffer send_tok;

    OM_uint32 major_status{}, minor_status{}, ret_flags{};

    major_status = ::gss_accept_sec_context(
      &minor_status,
      &_context,
      _server_creds,
      &recv_tok,
      GSS_C_NO_CHANNEL_BINDINGS,
      &_client_name,
      &_doid,
      &send_tok,
      &ret_flags,
      NULL,  /* ignore time_rec */
      NULL); /* ignore del_cred_handle */

    vlog(
      seclog.info,
      "gss {} acccept sec context {}:{}",
      _state,
      major_status,
      minor_status);
    if (
      major_status != GSS_S_COMPLETE && major_status != GSS_S_CONTINUE_NEEDED) {
        _state = state::failed;
        return errc::invalid_scram_state;
    }

    if (send_tok.length != 0) {
        bytes ret{bytes_view{send_tok}};
        if (major_status == GSS_S_COMPLETE) {
            auto res = finish();
            if (res.has_error()) {
                return res.assume_error();
            }
        } else if (major_status == GSS_S_CONTINUE_NEEDED) {
            _state = state::more;
        }
        vlog(seclog.info, "gss {} sending {} bytes", _state, ret.length());

        return ret;
    }

    auto res = finish();
    if (res.has_error()) {
        return res.assume_error();
    }
    return outcome::success();
}

result<void> gssapi_authenticator::finish() {
    OM_uint32 major_status{}, minor_status{};
    _state = state::complete;
    gss::buffer oid_name;
    if (auto status = ::gss_oid_to_str(&minor_status, _doid, &oid_name);
        status != GSS_S_COMPLETE) {
        display_status("gss doid", status, minor_status);
        _state = state::failed;
        return errc::invalid_scram_state;
    } else {
        vlog(
          seclog.info,
          "gss {} doid {}:{} = {}",
          _state,
          status,
          minor_status,
          std::string_view{oid_name});
    }

    gss::buffer display_name;
    major_status = ::gss_display_name(
      &minor_status, _client_name, &display_name, &_doid);
    if (major_status != GSS_S_COMPLETE) {
        display_status("gss client name", major_status, minor_status);
        _state = state::failed;
        return errc::invalid_scram_state;
    } else {
        std::string_view name{display_name};
        vlog(
          seclog.info,
          "gss {} client name {}:{} = {}",
          _state,
          major_status,
          minor_status,
          name);
        _principal = acl_principal{principal_type::user, ss::sstring{name}};
    }
    return outcome::success();
}

} // namespace security
