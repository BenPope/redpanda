/*
 * Copyright 2021 Vectorized, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/vectorizedio/redpanda/blob/master/licenses/rcl.md
 */

#include "archival/types.h"

#include <fmt/format.h>

namespace archival {

static uint64_t to_milliseconds(ss::lowres_clock::duration interval) {
    auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(interval);
    return ms.count();
}

std::ostream&
operator<<(std::ostream& o, const std::optional<segment_time_limit>& tl) {
    if (tl) {
        auto ms = to_milliseconds(tl.value()());
        fmt::print(o, "{}", ms);
    } else {
        fmt::print(o, "N/A");
    }
    return o;
}

std::ostream& operator<<(std::ostream& o, const configuration& cfg) {
    fmt::print(
      o,
      "{{bucket_name: {}, interval: {}, initial_backoff: {}, "
      "segment_upload_timeout: {}, "
      "manifest_upload_timeout: {}, time_limit: {}}}",
      cfg.bucket_name,
      to_milliseconds(cfg.interval),
      to_milliseconds(cfg.initial_backoff),
      to_milliseconds(cfg.segment_upload_timeout),
      to_milliseconds(cfg.manifest_upload_timeout),
      cfg.time_limit);
    return o;
}

} // namespace archival