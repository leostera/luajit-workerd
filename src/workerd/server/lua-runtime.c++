// Copyright (c) 2026 Cloudflare, Inc.
// Licensed under the Apache 2.0 license found in the LICENSE file or at:
//     https://opensource.org/licenses/Apache-2.0

#include "lua-runtime.h"

#include "lua-worker-interface.h"

#include <lua.hpp>

#include <kj/debug.h>

#include <cstring>

namespace workerd::server {
namespace {

class LuaStackGuard final {
 public:
  explicit LuaStackGuard(lua_State* state): state(state), top(lua_gettop(state)) {}
  ~LuaStackGuard() noexcept {
    lua_settop(state, top);
  }

 private:
  lua_State* state;
  int top;
};

int absoluteIndex(lua_State* state, int index) {
  return index > 0 || index <= LUA_REGISTRYINDEX ? index : lua_gettop(state) + index + 1;
}

void getRawField(lua_State* state, int tableIndex, const char* fieldName) {
  tableIndex = absoluteIndex(state, tableIndex);
  lua_pushstring(state, fieldName);
  lua_rawget(state, tableIndex);
}

kj::String readLuaErrorMessage(lua_State* state) {
  size_t length = 0;
  const char* message = lua_tolstring(state, -1, &length);
  if (message == nullptr) return kj::str("non-string Lua error");
  return kj::heapString(message, length);
}

kj::String readStringField(lua_State* state, int tableIndex, const char* fieldName) {
  getRawField(state, tableIndex, fieldName);
  if (lua_type(state, -1) != LUA_TSTRING) {
    KJ_FAIL_REQUIRE("Lua response field must be a string", fieldName);
  }
  size_t length = 0;
  const char* value = lua_tolstring(state, -1, &length);
  auto result = kj::heapString(value, length);
  lua_pop(state, 1);
  return result;
}

void setStringField(lua_State* state, const char* fieldName, kj::StringPtr value) {
  lua_pushlstring(state, value.begin(), value.size());
  lua_setfield(state, -2, fieldName);
}

void pushRequestHeaders(lua_State* state, const kj::HttpHeaders& headers) {
  lua_createtable(state, static_cast<int>(headers.size()), 0);
  int index = 1;
  headers.forEach([&](kj::StringPtr name, kj::StringPtr value) {
    lua_createtable(state, 0, 2);
    setStringField(state, "name", name);
    setStringField(state, "value", value);
    lua_rawseti(state, -2, index++);
  });
}

kj::Vector<LuaRuntime::Header> readResponseHeaders(lua_State* state, int tableIndex) {
  tableIndex = absoluteIndex(state, tableIndex);
  KJ_REQUIRE(lua_istable(state, tableIndex), "Lua response.headers must be an array");

  size_t count = lua_objlen(state, tableIndex);
  KJ_REQUIRE(count <= LuaRuntime::MAX_HTTP_HEADERS, "Lua response contains too many headers", count,
      "maximum", LuaRuntime::MAX_HTTP_HEADERS);

  kj::Vector<LuaRuntime::Header> headers(count);
  for (size_t i = 1; i <= count; ++i) {
    lua_rawgeti(state, tableIndex, static_cast<int>(i));
    KJ_REQUIRE(lua_istable(state, -1), "Lua response.headers entries must be tables", i);
    auto name = readStringField(state, -1, "name");
    auto value = readStringField(state, -1, "value");
    headers.add(LuaRuntime::Header{.name = kj::mv(name), .value = kj::mv(value)});
    lua_pop(state, 1);
  }
  return headers;
}

}  // namespace

LuaRuntime::LuaRuntime(kj::StringPtr moduleSource)
    : state(luaL_newstate()),
      fetchHandlerRef(LUA_NOREF) {
  KJ_REQUIRE(state != nullptr, "could not create LuaJIT state");
  bool keepState = false;
  KJ_DEFER(if (!keepState) lua_close(state));

  // This is an experimental trusted-code runtime, not a sandbox. The standard libraries include
  // operating-system and native-library access.
  luaL_openlibs(state);
  int loadStatus = luaL_loadbuffer(state, moduleSource.begin(), moduleSource.size(), "worker.lua");
  if (loadStatus != 0) {
    KJ_FAIL_REQUIRE("failed to compile LuaJIT module:", readLuaErrorMessage(state));
  }
  int evaluationStatus = lua_pcall(state, 0, 1, 0);
  if (evaluationStatus != 0) {
    KJ_FAIL_REQUIRE("failed to evaluate LuaJIT module:", readLuaErrorMessage(state));
  }
  KJ_REQUIRE(lua_istable(state, -1), "LuaJIT module must return a table");

  getRawField(state, -1, "fetch");
  KJ_REQUIRE(lua_isfunction(state, -1), "LuaJIT module must return a callable `fetch` field");
  fetchHandlerRef = luaL_ref(state, LUA_REGISTRYINDEX);
  lua_pop(state, 1);  // module table
  keepState = true;
}

LuaRuntime::~LuaRuntime() noexcept {
  if (state != nullptr) lua_close(state);
}

void LuaRuntime::requireCallSucceeded(kj::StringPtr operation) {
  if (lua_pcall(state, 1, 1, 0) == 0) return;
  KJ_FAIL_REQUIRE("LuaJIT", operation, "failed:", readLuaErrorMessage(state));
}

LuaRuntime::Response LuaRuntime::fetch(kj::StringPtr method,
    kj::StringPtr url,
    const kj::HttpHeaders& headers,
    kj::ArrayPtr<const kj::byte> body) {
  KJ_REQUIRE(
      body.size() <= MAX_HTTP_BODY_BYTES, "LuaJIT request body exceeds limit", MAX_HTTP_BODY_BYTES);
  KJ_REQUIRE(
      headers.size() <= MAX_HTTP_HEADERS, "LuaJIT request has too many headers", MAX_HTTP_HEADERS);

  LuaStackGuard stackGuard(state);
  lua_rawgeti(state, LUA_REGISTRYINDEX, fetchHandlerRef);
  lua_createtable(state, 0, 4);
  setStringField(state, "method", method);
  setStringField(state, "url", url);

  lua_pushlstring(state, reinterpret_cast<const char*>(body.begin()), body.size());
  lua_setfield(state, -2, "body");

  pushRequestHeaders(state, headers);
  lua_setfield(state, -2, "headers");

  requireCallSucceeded("fetch()");
  KJ_REQUIRE(lua_istable(state, -1), "LuaJIT fetch() must return a response table");
  int responseIndex = absoluteIndex(state, -1);

  getRawField(state, responseIndex, "status");
  int isNumber = 0;
  lua_Integer luaStatus = lua_tointegerx(state, -1, &isNumber);
  bool validStatus = lua_type(state, -1) == LUA_TNUMBER && isNumber && luaStatus >= 200 &&
      luaStatus <= 599 && static_cast<lua_Number>(luaStatus) == lua_tonumber(state, -1);
  if (!validStatus) {
    KJ_FAIL_REQUIRE("LuaJIT response.status must be an integer from 200 through 599");
  }
  lua_pop(state, 1);

  auto responseBody = readStringField(state, responseIndex, "body");
  KJ_REQUIRE(responseBody.size() <= MAX_HTTP_BODY_BYTES, "LuaJIT response body exceeds limit",
      MAX_HTTP_BODY_BYTES);
  auto bodyBytes = kj::heapArray<kj::byte>(responseBody.size());
  if (responseBody.size() > 0) {
    std::memcpy(bodyBytes.begin(), responseBody.begin(), responseBody.size());
  }

  kj::Vector<Header> responseHeaders;
  getRawField(state, responseIndex, "headers");
  if (!lua_isnil(state, -1)) {
    responseHeaders = readResponseHeaders(state, -1);
  }
  lua_pop(state, 1);

  return Response{
    .status = static_cast<uint16_t>(luaStatus),
    .headers = kj::mv(responseHeaders),
    .body = kj::mv(bodyBytes),
  };
}

namespace {

kj::Promise<void> respondNotImplemented(kj::HttpService::Response& response,
    const kj::HttpHeaders& requestHeaders,
    kj::StringPtr feature) {
  auto responseHeaders = requestHeaders.cloneShallow();
  responseHeaders.clear();
  auto body = kj::str("LuaJIT workers do not support ", feature, ".");
  auto output = response.send(501, "Not Implemented"_kj, responseHeaders, body.size());
  co_await output->write(body.asBytes());
}

}  // namespace

kj::Promise<void> LuaWorkerInterface::request(kj::HttpMethod method,
    kj::StringPtr url,
    const kj::HttpHeaders& headers,
    kj::AsyncInputStream& requestBody,
    Response& response) {
  if (headers.isWebSocket()) {
    co_return co_await respondNotImplemented(response, headers, "WebSockets");
  }

  auto methodName = kj::str(method);
  auto urlCopy = kj::str(url);
  auto requestHeaders = headers.clone();
  auto responseHeaders = requestHeaders.cloneShallow();
  responseHeaders.clear();
  auto body = co_await requestBody.readAllBytes(LuaRuntime::MAX_HTTP_BODY_BYTES);
  auto result = runtime.fetch(methodName, urlCopy, requestHeaders, body);

  for (auto& header: result.headers) {
    responseHeaders.add(kj::mv(header.name), kj::mv(header.value));
  }

  auto output = response.send(result.status, "LuaJIT"_kj, responseHeaders, result.body.size());
  co_await output->write(result.body);
}

kj::Promise<void> LuaWorkerInterface::connect(kj::StringPtr host,
    const kj::HttpHeaders& headers,
    kj::AsyncIoStream& connection,
    ConnectResponse& response,
    kj::HttpConnectSettings settings) {
  auto responseHeaders = headers.cloneShallow();
  responseHeaders.clear();
  auto output = response.reject(501, "Not Implemented"_kj, responseHeaders, uint64_t(0));
  kj::ArrayPtr<const kj::byte> noBody = nullptr;
  return output->write(noBody);
}

kj::Promise<void> LuaWorkerInterface::prewarm(kj::StringPtr url) {
  return kj::READY_NOW;
}

kj::Promise<WorkerInterface::ScheduledResult> LuaWorkerInterface::runScheduled(
    kj::Date scheduledTime, kj::StringPtr cron) {
  KJ_FAIL_REQUIRE("LuaJIT workers do not support scheduled events");
}

kj::Promise<WorkerInterface::AlarmResult> LuaWorkerInterface::runAlarm(
    kj::Date scheduledTime, uint32_t retryCount) {
  KJ_FAIL_REQUIRE("LuaJIT workers do not support alarms");
}

kj::Promise<WorkerInterface::CustomEvent::Result> LuaWorkerInterface::customEvent(
    kj::Own<WorkerInterface::CustomEvent> event) {
  return event->notSupported();
}

}  // namespace workerd::server
