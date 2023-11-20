// Copyright 2023 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#pragma once

#include "meta/concepts.h"

#include <daw/json/daw_json_data_contract.h>

#include <concepts>
#include <tuple>

namespace meta::json {

template<typename T>
concept has_meta = requires(T t) {
    typename T::meta_json_type;
    tuple_like<decltype(t.meta_json_data())>;
};

} // namespace meta::json

namespace daw::json {

// Generic Specialisation
template<::meta::json::has_meta T>
struct json_data_contract<T> {
    using type = T::meta_json_type;
    static constexpr auto to_json_data(T const& b) {
        return b.meta_json_data();
    }
};

} // namespace daw::json
