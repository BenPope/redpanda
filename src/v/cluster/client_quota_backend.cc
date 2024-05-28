// Copyright 2024 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "cluster/client_quota_backend.h"

#include "cluster/client_quota_serde.h"
#include "cluster/client_quota_store.h"
#include "cluster/commands.h"
#include "cluster/controller_snapshot.h"

#include <seastar/core/coroutine.hh>
#include <seastar/util/variant_utils.hh>

#include <optional>

namespace cluster::client_quota {

ss::future<std::error_code> backend::apply_update(model::record_batch batch) {
    auto cmd = co_await cluster::deserialize(std::move(batch), commands);
    co_await _quotas.invoke_on_all([cmd{std::move(cmd)}](store& quota_store) {
        return ss::visit(
          std::move(cmd), [&quota_store](alter_quotas_delta_cmd cmd) {
              for (auto& [key, value] : cmd.value.upsert) {
                  quota_store.set_quota(key, value);
              }

              for (auto& [key] : cmd.value.remove) {
                  quota_store.remove_quota(key);
              }
          });
    });
    co_return errc::success;
}

ss::future<> backend::fill_snapshot(controller_snapshot& snap) const {
    snap.client_quotas.quotas = _quotas.local().all_quotas();
    return ss::now();
}

ss::future<>
backend::apply_snapshot(model::offset, const controller_snapshot& snap) {
    return _quotas.invoke_on_all([&snap](store& quota_store) {
        quota_store.clear();
        for (const auto& [key, val] : snap.client_quotas.quotas) {
            quota_store.set_quota(key, val);
        }
    });
}

} // namespace cluster::client_quota
