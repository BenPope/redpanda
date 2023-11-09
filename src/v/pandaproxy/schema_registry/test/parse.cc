// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "http/client.h"
#include "json/document.h"
#include "pandaproxy/schema_registry/storage.h"
#include "pandaproxy/test/pandaproxy_fixture.h"
#include "pandaproxy/test/utils.h"

#include <boost/beast/http/field.hpp>
#include <boost/beast/http/status.hpp>
#include <boost/beast/http/verb.hpp>
#include <boost/test/tools/old/interface.hpp>
#include <rapidjson/error/en.h>
#include <rapidjson/error/error.h>
#include <rapidjson/istreamwrapper.h>

namespace pp = pandaproxy;
namespace ppj = pp::json;
namespace pps = pp::schema_registry;

SEASTAR_THREAD_TEST_CASE(schema_registry_parser) {
    using namespace std::chrono_literals;
    using namespace pps;

    // Open the file
    std::ifstream ifs("_schemas.json");
    rapidjson::IStreamWrapper isw(ifs);

    // Loop over each document
    while (true) {
        rapidjson::Document document;
        document.ParseStream<rapidjson::kParseStopWhenDoneFlag>(isw);
        if (document.HasParseError()) {
            std::cerr << "Error parsing JSON: "
                      << GetParseError_En(document.GetParseError()) << '\n';
        }

        // Process the current document (replace this with your logic)
        std::cout << "Parsed a document.\n";

        auto const& key = document["key"];
        if (!key.IsString()) {
            std::cout << "key is not string!\n";
            continue;
        }
        std::cout << "getting key: " << key.GetString() << "\n";
        auto key_type_str = ppj::rjson_parse(
          key.GetString(), pp::schema_registry::topic_key_type_handler<>());
        auto key_type = from_string_view<topic_key_type>(key_type_str);
        std::cout << "key_type: " << key_type << "\n";
        if (!key_type.has_value()) {
            std::cout << "Ignoring keytype: " << key_type_str;
            continue;
        }

        auto value = [&]() {
            auto const& v = document["value"];
            if (!v.IsString()) {
                std::cout << "val not a string!\n";
            } else {
                std::cout << "got value: " << v.GetString() << "\n";
            }
            return v.GetString();
        };

        switch (*key_type) {
        case topic_key_type::noop:
            break;
        case topic_key_type::schema: {
            ppj::rjson_parse(key.GetString(), schema_key_handler<>());
            if (document.HasMember("value")) {
                ppj::rjson_parse(value(), unparsed_schema_value_handler<>());
            } else {
                std::cout << "no value\n";
            }
            break;
        }
        case topic_key_type::config: {
            ppj::rjson_parse(key.GetString(), config_key_handler<>());
            if (document.HasMember("value")) {
                ppj::rjson_parse(value(), config_value_handler<>());
            } else {
                std::cout << "no value\n";
            }
            break;
        }
        case topic_key_type::delete_subject:
            ppj::rjson_parse(key.GetString(), delete_subject_key_handler<>());
            if (document.HasMember("value")) {
                ppj::rjson_parse(value(), delete_subject_value_handler<>());
            } else {
                std::cout << "no value\n";
            }
            break;
        }
    }

    return; // Return success
}
