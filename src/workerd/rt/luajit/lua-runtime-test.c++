// Copyright (c) 2026 Cloudflare, Inc.
// Licensed under the Apache 2.0 license found in the LICENSE file or at:
//     https://opensource.org/licenses/Apache-2.0

#include "lua-runtime.h"
#include "lua-worker-interface.h"

#include <kj/async-io.h>
#include <kj/test.h>

#include <cstring>

namespace workerd::rt::luajit {
namespace {

kj::StringPtr asString(kj::ArrayPtr<const kj::byte> bytes) {
  return kj::StringPtr(reinterpret_cast<const char*>(bytes.begin()), bytes.size());
}

KJ_TEST("LuaJIT HTTP handler receives requests and returns status, headers, and body") {
  auto io = kj::setupAsyncIo();
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

  LuaRuntime runtime(source, io.provider->getTimer());
  kj::HttpHeaderTable headerTable;
  kj::HttpHeaders headers(headerTable);
  headers.addPtrPtr("x-input"_kj, "received"_kj);
  auto requestBody = kj::heapArray<kj::byte>(4);
  std::memcpy(requestBody.begin(), "body", 4);

  auto response =
      runtime.fetch("POST"_kj, "https://example.com/"_kj, headers, requestBody).wait(io.waitScope);

  KJ_EXPECT(response.status == 201);
  KJ_EXPECT(asString(response.body) == "POST https://example.com/ body");
  KJ_REQUIRE(response.headers.size() == 1);
  KJ_EXPECT(response.headers[0].name == "x-lua");
  KJ_EXPECT(response.headers[0].value == "x-input:received");
}

KJ_TEST("LuaJIT state is independent between runtime instances") {
  auto io = kj::setupAsyncIo();
  constexpr kj::StringPtr source = R"lua(
    local calls = 0
    return {
      fetch = function(request)
        calls = calls + 1
        return { status = 200, body = tostring(calls) }
      end
    }
  )lua"_kj;

  LuaRuntime first(source, io.provider->getTimer());
  LuaRuntime second(source, io.provider->getTimer());
  kj::HttpHeaderTable headerTable;
  kj::HttpHeaders headers(headerTable);

  auto firstCall = first.fetch("GET"_kj, "/"_kj, headers, nullptr).wait(io.waitScope);
  auto secondCall = first.fetch("GET"_kj, "/"_kj, headers, nullptr).wait(io.waitScope);
  auto isolatedCall = second.fetch("GET"_kj, "/"_kj, headers, nullptr).wait(io.waitScope);

  KJ_EXPECT(asString(firstCall.body) == "1");
  KJ_EXPECT(asString(secondCall.body) == "2");
  KJ_EXPECT(asString(isolatedCall.body) == "1");
}

KJ_TEST("LuaJIT coroutine-backed sleep yields without blocking other requests") {
  auto io = kj::setupAsyncIo();
  constexpr kj::StringPtr source = R"lua(
    local calls = 0
    return {
      fetch = function(request)
        if request.url == "/invalid" then
          local succeeded = pcall(function() workerd.sleep(-1) end)
          return { status = 200, body = tostring(succeeded) }
        end
        calls = calls + 1
        local call = calls
        local result = "not-slept"
        if request.url == "/slow" then result = tostring(workerd.sleep(50)) end
        return { status = 200, body = tostring(call) .. ":" .. result }
      end
    }
  )lua"_kj;

  LuaRuntime runtime(source, io.provider->getTimer());
  kj::HttpHeaderTable headerTable;
  kj::HttpHeaders headers(headerTable);

  auto slow = runtime.fetch("GET"_kj, "/slow"_kj, headers, nullptr);
  KJ_EXPECT(!slow.poll(io.waitScope));
  auto fast = runtime.fetch("GET"_kj, "/fast"_kj, headers, nullptr).wait(io.waitScope);
  KJ_EXPECT(asString(fast.body) == "2:not-slept");

  auto invalidSleep = runtime.fetch("GET"_kj, "/invalid"_kj, headers, nullptr).wait(io.waitScope);
  KJ_EXPECT(asString(invalidSleep.body) == "false");

  auto delayed = slow.wait(io.waitScope);
  KJ_EXPECT(asString(delayed.body) == "1:true");
}

KJ_TEST("LuaJIT HTTP interface rejects scheduled and alarm events") {
  auto io = kj::setupAsyncIo();
  LuaRuntime runtime(
      R"lua(return { fetch = function() return { status = 200, body = "" } end })lua"_kj,
      io.provider->getTimer());
  LuaWorkerInterface worker(runtime);

  KJ_EXPECT(kj::runCatchingExceptions(
                [&]() { (void)worker.runScheduled(kj::UNIX_EPOCH, "* * * * *"_kj); }) != kj::none);
  KJ_EXPECT(
      kj::runCatchingExceptions([&]() { (void)worker.runAlarm(kj::UNIX_EPOCH, 0); }) != kj::none);
}

KJ_TEST("LuaJIT rejects invalid modules and handler results") {
  auto io = kj::setupAsyncIo();
  auto& timer = io.provider->getTimer();
  KJ_EXPECT(
      kj::runCatchingExceptions([&]() { LuaRuntime runtime("return {}"_kj, timer); }) != kj::none);
  KJ_EXPECT(kj::runCatchingExceptions(
                [&]() { LuaRuntime runtime("this is not valid Lua"_kj, timer); }) != kj::none);

  kj::HttpHeaderTable headerTable;
  kj::HttpHeaders headers(headerTable);
  LuaRuntime missingBody(
      R"lua(return { fetch = function() return { status = 200 } end })lua"_kj, timer);
  KJ_EXPECT(kj::runCatchingExceptions([&]() {
    (void)missingBody.fetch("GET"_kj, "/"_kj, headers, nullptr).wait(io.waitScope);
  }) != kj::none);

  LuaRuntime badStatus(
      R"lua(return { fetch = function() return { status = 99, body = "" } end })lua"_kj, timer);
  KJ_EXPECT(kj::runCatchingExceptions([&]() {
    (void)badStatus.fetch("GET"_kj, "/"_kj, headers, nullptr).wait(io.waitScope);
  }) != kj::none);

  LuaRuntime fractionalStatus(
      R"lua(return { fetch = function() return { status = 200.5, body = "" } end })lua"_kj, timer);
  KJ_EXPECT(kj::runCatchingExceptions([&]() {
    (void)fractionalStatus.fetch("GET"_kj, "/"_kj, headers, nullptr).wait(io.waitScope);
  }) != kj::none);

  LuaRuntime nonStringBody(
      R"lua(return { fetch = function() return { status = 200, body = 7 } end })lua"_kj, timer);
  KJ_EXPECT(kj::runCatchingExceptions([&]() {
    (void)nonStringBody.fetch("GET"_kj, "/"_kj, headers, nullptr).wait(io.waitScope);
  }) != kj::none);

  LuaRuntime thrownError(
      R"lua(return { fetch = function() error("handler failed") end })lua"_kj, timer);
  KJ_EXPECT(kj::runCatchingExceptions([&]() {
    (void)thrownError.fetch("GET"_kj, "/"_kj, headers, nullptr).wait(io.waitScope);
  }) != kj::none);
}

}  // namespace
}  // namespace workerd::rt::luajit
