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

#include "utils/log_hist.h"

#include "vassert.h"

log_hist_base::log_hist_base(
  int number_of_buckets, uint64_t first_bucket_upper_bound)
  : _number_of_buckets(number_of_buckets)
  , _first_bucket_upper_bound(first_bucket_upper_bound)
  , _counts(number_of_buckets) {}

seastar::metrics::histogram
log_hist_base::seastar_histogram_logform(logform_config cfg) const {
    const auto bound_is_pow_2 = cfg.first_bucket_bound >= 1
                                && (cfg.first_bucket_bound
                                    & (cfg.first_bucket_bound - 1))
                                     == 0;

    vassert(bound_is_pow_2, "cfg.first_bucket_bound must be a power of 2");

    seastar::metrics::histogram hist;
    hist.buckets.resize(cfg.number_of_buckets);
    hist.sample_sum = static_cast<double>(_sample_sum)
                      / static_cast<double>(cfg.scale);

    const unsigned first_bucket_exp
      = 64 - std::countl_zero(_first_bucket_upper_bound - 1);
    const unsigned cfg_first_bucket_exp
      = 64 - std::countl_zero(cfg.first_bucket_bound - 1);

    // Write bounds to seastar histogram
    for (int i = 0; i < cfg.number_of_buckets; i++) {
        auto& bucket = hist.buckets[i];
        bucket.count = 0;

        uint64_t unscaled_upper_bound = ((uint64_t)1
                                         << (cfg_first_bucket_exp + i))
                                        - 1;
        bucket.upper_bound = static_cast<double>(unscaled_upper_bound)
                             / static_cast<double>(cfg.scale);
    }

    uint64_t cumulative_count = 0;
    size_t current_hist_idx = 0;
    double current_hist_upper_bound = hist.buckets[0].upper_bound;

    // Write _counts to seastar histogram
    for (size_t i = 0; i < _counts.size(); i++) {
        uint64_t unscaled_upper_bound = ((uint64_t)1 << (first_bucket_exp + i))
                                        - 1;
        double scaled_upper_bound = static_cast<double>(unscaled_upper_bound)
                                    / static_cast<double>(cfg.scale);

        cumulative_count += _counts[i];

        while (scaled_upper_bound > current_hist_upper_bound
               && current_hist_idx != (hist.buckets.size() - 1)) {
            current_hist_idx++;
            current_hist_upper_bound
              = hist.buckets[current_hist_idx].upper_bound;
        }

        hist.buckets[current_hist_idx].count = cumulative_count;
    }

    hist.sample_count = cumulative_count;
    return hist;
}

seastar::metrics::histogram log_hist_base::public_histogram_logform() const {
    constexpr logform_config public_hist_config = {
      .scale = 1'000'000, .first_bucket_bound = 256, .number_of_buckets = 18};

    return seastar_histogram_logform(public_hist_config);
}

seastar::metrics::histogram log_hist_base::internal_histogram_logform() const {
    constexpr logform_config internal_hist_config = {
      .scale = 1, .first_bucket_bound = 8, .number_of_buckets = 26};

    return seastar_histogram_logform(internal_hist_config);
}
