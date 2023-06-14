/*
 * Copyright 2023 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */
#include "wasm/tests/wasm_fixture.h"

#include "model/record_batch_reader.h"
#include "model/tests/randoms.h"
#include "model/timeout_clock.h"

#include <seastar/core/circular_buffer.hh>
#include <seastar/core/reactor.hh>

wasm_test_fixture::wasm_test_fixture()
  : _service()
  , _meta(
      "test_wasm_transform",
      model::random_topic_namespace(),
      model::random_topic_namespace()) {
    _worker.start().get();
    _service.start(&_worker, nullptr).get();
}
wasm_test_fixture::~wasm_test_fixture() {
    _service.stop().get();
    _worker.stop().get();
}

void wasm_test_fixture::load_wasm(const std::string& path) {
    auto wasm_file = ss::util::read_entire_file_contiguous(path).get0();
    _service.local().deploy_transform(_meta, wasm_file).get();
}

ss::circular_buffer<model::record_batch>
wasm_test_fixture::transform(const model::record_batch& batch) {
    auto reader = _service.local().wrap_batch_reader(
      _meta.output, model::make_memory_record_batch_reader(batch.copy()));
    return model::consume_reader_to_memory(std::move(reader), model::no_timeout)
      .get();
}
model::record_batch wasm_test_fixture::make_tiny_batch() {
    return model::test::make_random_batch(model::test::record_batch_spec{
      .allow_compression = false,
      .count = 1,
    });
}
