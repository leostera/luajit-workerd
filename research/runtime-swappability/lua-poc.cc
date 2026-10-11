// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 Cloudflare, Inc.
//
// Standalone, non-production experiment. This deliberately does not include workerd/JSG headers.

#include <lua.hpp>

#include <stdexcept>
#include <string>
#include <utility>

struct Request {
  std::string method;
  std::string path;
  std::string body;
};

struct Response {
  int status;
  std::string body;
};

class RuntimeBackend {
 public:
  virtual ~RuntimeBackend() = default;
  virtual Response handle(const Request& request) = 0;
};

class LuaBackend final: public RuntimeBackend {
 public:
  explicit LuaBackend(const char* moduleSource): state(luaL_newstate()) {
    if (state == nullptr) throw std::runtime_error("could not allocate Lua state");
    try {
      luaL_openlibs(state);
      requireLuaSuccess(luaL_loadstring(state, moduleSource));
      requireLuaSuccess(lua_pcall(state, 0, 1, 0));
      if (!lua_istable(state, -1)) throw std::runtime_error("module must return a table");

      lua_getfield(state, -1, "handle");
      if (!lua_isfunction(state, -1)) throw std::runtime_error("module.handle must be a function");
      handlerRef = luaL_ref(state, LUA_REGISTRYINDEX);
      lua_pop(state, 1);  // module table
    } catch (...) {
      lua_close(state);
      state = nullptr;
      throw;
    }
  }

  ~LuaBackend() override {
    if (state != nullptr) lua_close(state);
  }

  Response handle(const Request& request) override {
    lua_rawgeti(state, LUA_REGISTRYINDEX, handlerRef);
    lua_newtable(state);
    setStringField("method", request.method);
    setStringField("path", request.path);
    setStringField("body", request.body);
    requireLuaSuccess(lua_pcall(state, 1, 1, 0));

    if (!lua_istable(state, -1)) throw std::runtime_error("handler must return a response table");

    lua_getfield(state, -1, "status");
    if (!lua_isnumber(state, -1)) throw std::runtime_error("response.status must be a number");
    int status = static_cast<int>(lua_tonumber(state, -1));
    lua_pop(state, 1);

    lua_getfield(state, -1, "body");
    size_t bodySize = 0;
    const char* body = lua_tolstring(state, -1, &bodySize);
    if (body == nullptr) throw std::runtime_error("response.body must be a string");
    std::string copiedBody(body, bodySize);
    lua_pop(state, 2);  // body field and response table
    return Response{status, std::move(copiedBody)};
  }

 private:
  lua_State* state;
  int handlerRef = LUA_NOREF;

  void requireLuaSuccess(int status) {
    if (status == 0) return;
    size_t messageSize = 0;
    const char* message = lua_tolstring(state, -1, &messageSize);
    throw std::runtime_error(message == nullptr ? "Lua execution failed"
                                                 : std::string(message, messageSize));
  }

  void setStringField(const char* name, const std::string& value) {
    lua_pushlstring(state, value.data(), value.size());
    lua_setfield(state, -2, name);
  }
};

class HostService {
 public:
  explicit HostService(RuntimeBackend& backend): backend(backend) {}

  Response dispatch(const Request& request) {
    return backend.handle(request);
  }

 private:
  RuntimeBackend& backend;
};

int main() {
  constexpr auto moduleSource = R"lua(
    local calls = 0
    return {
      handle = function(request)
        calls = calls + 1
        return {
          status = 200,
          body = request.method .. " " .. request.path .. " " .. request.body .. " #" .. calls
        }
      end
    }
  )lua";

  LuaBackend firstRuntime(moduleSource);
  LuaBackend secondRuntime(moduleSource);
  HostService firstHost(firstRuntime);
  HostService secondHost(secondRuntime);

  const auto first = firstHost.dispatch(Request{"POST", "/from-lua", "payload"});
  const auto secondCall = firstHost.dispatch(Request{"GET", "/again", ""});
  const auto isolatedCall = secondHost.dispatch(Request{"GET", "/again", ""});

  if (first.status != 200 || first.body != "POST /from-lua payload #1") return 1;
  if (secondCall.body != "GET /again  #2") return 1;
  if (isolatedCall.body != "GET /again  #1") return 1;
  return 0;
}
