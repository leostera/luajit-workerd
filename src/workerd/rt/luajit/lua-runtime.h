// Copyright (c) 2026 Cloudflare, Inc.
// Licensed under the Apache 2.0 license found in the LICENSE file or at:
//     https://opensource.org/licenses/Apache-2.0

#pragma once

#include <kj/array.h>
#include <kj/async-io.h>
#include <kj/async.h>
#include <kj/compat/http.h>
#include <kj/memory.h>
#include <kj/string.h>
#include <kj/timer.h>
#include <kj/vector.h>

#include <cstddef>
#include <cstdint>

struct lua_State;

namespace workerd::rt::luajit {

// A config binding copied into a Lua worker's service-global workerd.bindings table.
struct LuaBinding {
  enum class Type {
    TEXT,
    DATA,
    JSON,
  };

  kj::String name;
  Type type;
  kj::String text;
  kj::Array<kj::byte> data;
};

struct LuaHttpHeader {
  kj::String name;
  kj::String value;
};

struct LuaHttpRequest {
  kj::String method;
  kj::String url;
  kj::Vector<LuaHttpHeader> headers;
  kj::Array<kj::byte> body;
};

struct LuaHttpResponse {
  uint16_t status;
  kj::Vector<LuaHttpHeader> headers;
  kj::Own<kj::AsyncInputStream> body;
};

struct LuaResponse {
  uint16_t status;
  kj::Vector<LuaHttpHeader> headers;
  kj::Array<kj::byte> body;
};

class LuaHostOperations: public kj::PtrTarget {
 public:
  virtual ~LuaHostOperations() noexcept(false) = default;

  kj::Ptr<LuaHostOperations> getPtr() {
    return addPtrToThis();
  }
  virtual kj::Promise<LuaHttpResponse> fetch(LuaHttpRequest request) = 0;
};

class LuaRuntime final {
 public:
  using Header = LuaHttpHeader;
  using Response = LuaResponse;

  static constexpr size_t MAX_HTTP_BODY_BYTES = 1024 * 1024;
  static constexpr size_t MAX_HTTP_HEADERS = 128;
  static constexpr size_t MAX_HTTP_READ_CHUNK_BYTES = 64 * 1024;

  // The module must return a table with a callable `fetch` field. This runtime owns one LuaJIT
  // state for its lifetime; each fetch() runs on a coroutine and yields only through host async
  // operations. Coroutine access to the shared state is serialized on the event-loop thread.
  // Bindings are parsed and installed before the module source is evaluated.
  LuaRuntime(
      kj::StringPtr moduleSource, kj::Timer& timer, kj::Array<LuaBinding> bindings = nullptr);
  ~LuaRuntime() noexcept;
  KJ_DISALLOW_COPY_AND_MOVE(LuaRuntime);

  kj::Promise<Response> fetch(kj::StringPtr method,
      kj::StringPtr url,
      const kj::HttpHeaders& headers,
      kj::ArrayPtr<const kj::byte> body,
      kj::Maybe<kj::Ptr<LuaHostOperations>> hostOperations = kj::none);

 private:
  kj::Timer& timer;
  lua_State* state;
  int fetchHandlerRef;
  uint64_t nextRequestId = 1;
};

}  // namespace workerd::rt::luajit
