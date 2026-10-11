// Copyright (c) 2026 Cloudflare, Inc.
// Licensed under the Apache 2.0 license found in the LICENSE file or at:
//     https://opensource.org/licenses/Apache-2.0

#pragma once

#include <kj/array.h>
#include <kj/async.h>
#include <kj/compat/http.h>
#include <kj/string.h>
#include <kj/timer.h>
#include <kj/vector.h>

#include <cstddef>
#include <cstdint>

struct lua_State;

namespace workerd::rt::luajit {

class LuaRuntime final {
 public:
  struct Header {
    kj::String name;
    kj::String value;
  };

  struct Response {
    uint16_t status;
    kj::Vector<Header> headers;
    kj::Array<kj::byte> body;
  };

  static constexpr size_t MAX_HTTP_BODY_BYTES = 1024 * 1024;
  static constexpr size_t MAX_HTTP_HEADERS = 128;

  // The module must return a table with a callable `fetch` field. This runtime owns one LuaJIT
  // state for its lifetime; each fetch() runs on a coroutine and yields only through host async
  // operations. Coroutine access to the shared state is serialized on the event-loop thread.
  LuaRuntime(kj::StringPtr moduleSource, kj::Timer& timer);
  ~LuaRuntime() noexcept;
  KJ_DISALLOW_COPY_AND_MOVE(LuaRuntime);

  kj::Promise<Response> fetch(kj::StringPtr method,
      kj::StringPtr url,
      const kj::HttpHeaders& headers,
      kj::ArrayPtr<const kj::byte> body);

 private:
  kj::Timer& timer;
  lua_State* state;
  int fetchHandlerRef;
};

}  // namespace workerd::rt::luajit
