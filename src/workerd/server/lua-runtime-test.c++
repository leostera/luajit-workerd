// Copyright (c) 2026 Cloudflare, Inc.
// Licensed under the Apache 2.0 license found in the LICENSE file or at:
//     https://opensource.org/licenses/Apache-2.0

#include "lua-runtime.h"
#include "lua-worker-interface.h"

#include <kj/test.h>

#include <cstring>

namespace workerd::server {
namespace {

kj::StringPtr asString(kj::ArrayPtr<const kj::byte> bytes) {
  return kj::StringPtr(reinterpret_cast<const char*>(bytes.begin()), bytes.size());
}

KJ_TEST("LuaJIT HTTP handler receives requests and returns status, headers, and body") {
  constexpr kj::StringPtr source = R"lua(
    return {
      fetch = function(request)
        local header = request.headers[1]
        return {
          status = 201,
          headers = {{ name = "x-lua", value = header.name .. ":" .. header.value }},
          body = request.method .. " " .. request.url .. " " .. request.body
        }
      end
    }
  )lua"_kj;

  LuaRuntime runtime(source);
  kj::HttpHeaderTable headerTable;
  kj::HttpHeaders headers(headerTable);
  headers.addPtrPtr("x-input"_kj, "received"_kj);
  auto requestBody = kj::heapArray<kj::byte>(4);
  std::memcpy(requestBody.begin(), "body", 4);

  auto response = runtime.fetch("POST"_kj, "https://example.com/"_kj, headers, requestBody);

  KJ_EXPECT(response.status == 201);
  KJ_EXPECT(asString(response.body) == "POST https://example.com/ body");
  KJ_REQUIRE(response.headers.size() == 1);
  KJ_EXPECT(response.headers[0].name == "x-lua");
  KJ_EXPECT(response.headers[0].value == "x-input:received");
}

KJ_TEST("LuaJIT state is independent between runtime instances") {
  constexpr kj::StringPtr source = R"lua(
    local calls = 0
    return {
      fetch = function(request)
        calls = calls + 1
        return { status = 200, body = tostring(calls) }
      end
    }
  )lua"_kj;

  LuaRuntime first(source);
  LuaRuntime second(source);
  kj::HttpHeaderTable headerTable;
  kj::HttpHeaders headers(headerTable);

  auto firstCall = first.fetch("GET"_kj, "/"_kj, headers, nullptr);
  auto secondCall = first.fetch("GET"_kj, "/"_kj, headers, nullptr);
  auto isolatedCall = second.fetch("GET"_kj, "/"_kj, headers, nullptr);

  KJ_EXPECT(asString(firstCall.body) == "1");
  KJ_EXPECT(asString(secondCall.body) == "2");
  KJ_EXPECT(asString(isolatedCall.body) == "1");
}

KJ_TEST("LuaJIT HTTP interface rejects scheduled and alarm events") {
  LuaRuntime runtime(
      R"lua(return { fetch = function() return { status = 200, body = "" } end })lua"_kj);
  LuaWorkerInterface worker(runtime);

  KJ_EXPECT(kj::runCatchingExceptions(
                [&]() { (void)worker.runScheduled(kj::UNIX_EPOCH, "* * * * *"_kj); }) != kj::none);
  KJ_EXPECT(
      kj::runCatchingExceptions([&]() { (void)worker.runAlarm(kj::UNIX_EPOCH, 0); }) != kj::none);
}

KJ_TEST("LuaJIT rejects invalid modules and handler results") {
  KJ_EXPECT(kj::runCatchingExceptions([]() { LuaRuntime runtime("return {}"_kj); }) != kj::none);
  KJ_EXPECT(kj::runCatchingExceptions([]() { LuaRuntime runtime("this is not valid Lua"_kj); }) !=
      kj::none);

  kj::HttpHeaderTable headerTable;
  kj::HttpHeaders headers(headerTable);
  LuaRuntime missingBody(R"lua(return { fetch = function() return { status = 200 } end })lua"_kj);
  KJ_EXPECT(kj::runCatchingExceptions(
                [&]() { missingBody.fetch("GET"_kj, "/"_kj, headers, nullptr); }) != kj::none);

  LuaRuntime badStatus(
      R"lua(return { fetch = function() return { status = 99, body = "" } end })lua"_kj);
  KJ_EXPECT(kj::runCatchingExceptions(
                [&]() { badStatus.fetch("GET"_kj, "/"_kj, headers, nullptr); }) != kj::none);

  LuaRuntime fractionalStatus(
      R"lua(return { fetch = function() return { status = 200.5, body = "" } end })lua"_kj);
  KJ_EXPECT(kj::runCatchingExceptions(
                [&]() { fractionalStatus.fetch("GET"_kj, "/"_kj, headers, nullptr); }) != kj::none);

  LuaRuntime nonStringBody(
      R"lua(return { fetch = function() return { status = 200, body = 7 } end })lua"_kj);
  KJ_EXPECT(kj::runCatchingExceptions(
                [&]() { nonStringBody.fetch("GET"_kj, "/"_kj, headers, nullptr); }) != kj::none);

  LuaRuntime thrownError(R"lua(return { fetch = function() error("handler failed") end })lua"_kj);
  KJ_EXPECT(kj::runCatchingExceptions(
                [&]() { thrownError.fetch("GET"_kj, "/"_kj, headers, nullptr); }) != kj::none);
}

}  // namespace
}  // namespace workerd::server
