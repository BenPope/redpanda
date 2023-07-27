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

template<int number_of_buckets, uint64_t first_bucket_upper_bound>
log_hist_base<number_of_buckets, first_bucket_upper_bound>::log_hist_base()
  : _counts() {}

template<int number_of_buckets, uint64_t first_bucket_upper_bound>
template<typename cfg>
seastar::metrics::histogram
log_hist_base<number_of_buckets, first_bucket_upper_bound>::
  seastar_histogram_logform() const {
    const auto bound_is_pow_2 = cfg::first_bucket_bound >= 1
                                && (cfg::first_bucket_bound
                                    & (cfg::first_bucket_bound - 1))
                                     == 0;

    vassert(bound_is_pow_2, "cfg.first_bucket_bound must be a power of 2");

    seastar::metrics::histogram hist;
    hist.buckets.resize(cfg::bucket_count);
    hist.sample_sum = static_cast<double>(_sample_sum)
                      / static_cast<double>(cfg::scale);

    const unsigned first_bucket_exp
      = 64 - std::countl_zero(first_bucket_upper_bound - 1);
    const unsigned cfg_first_bucket_exp
      = 64 - std::countl_zero(cfg::first_bucket_bound - 1);

    // Write bounds to seastar histogram
    for (int i = 0; i < cfg::bucket_count; i++) {
        auto& bucket = hist.buckets[i];
        bucket.count = 0;

        uint64_t unscaled_upper_bound = ((uint64_t)1
                                         << (cfg_first_bucket_exp + i))
                                        - 1;
        bucket.upper_bound = static_cast<double>(unscaled_upper_bound)
                             / static_cast<double>(cfg::scale);
    }

    uint64_t cumulative_count = 0;
    size_t current_hist_idx = 0;
    double current_hist_upper_bound = hist.buckets[0].upper_bound;

    // Write _counts to seastar histogram
    for (size_t i = 0; i < _counts.size(); i++) {
        uint64_t unscaled_upper_bound = ((uint64_t)1 << (first_bucket_exp + i))
                                        - 1;
        double scaled_upper_bound = static_cast<double>(unscaled_upper_bound)
                                    / static_cast<double>(cfg::scale);

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

template<int number_of_buckets, uint64_t first_bucket_upper_bound>
seastar::metrics::histogram
log_hist_base<number_of_buckets, first_bucket_upper_bound>::
  public_histogram_logform() const {
    using public_hist_config = logform_config<1'000'000, 256, 18>;

    return seastar_histogram_logform<public_hist_config>();
}

template<int number_of_buckets, uint64_t first_bucket_upper_bound>
seastar::metrics::histogram
log_hist_base<number_of_buckets, first_bucket_upper_bound>::
  internal_histogram_logform() const {
    using internal_hist_config = logform_config<1, 8, 26>;

    return seastar_histogram_logform<internal_hist_config>();
}

template class log_hist_base<18, 256ul>;
template class log_hist_base<26, 8ul>;

template seastar::metrics::histogram
log_hist_base<18, 256ul>::seastar_histogram_logform<
  logform_config<1000000l, 256ul, 18>>() const;

template seastar::metrics::histogram
log_hist_base<18, 256ul>::seastar_histogram_logform<
  logform_config<1, 256ul, 18>>() const;

template seastar::metrics::histogram
log_hist_base<26, 8ul>::seastar_histogram_logform<
  logform_config<1000000l, 256ul, 18>>() const;

template seastar::metrics::histogram
log_hist_base<26, 8ul>::seastar_histogram_logform<
  logform_config<1l, 256ul, 18>>() const;
