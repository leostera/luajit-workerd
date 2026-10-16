// Copyright (c) 2026 Cloudflare, Inc.
// Licensed under the Apache 2.0 license found in the LICENSE file or at:
//     https://opensource.org/licenses/Apache-2.0

#include "lua-runtime.h"

#include <lua.hpp>
#include <openssl/crypto.h>
#include <openssl/evp.h>
#include <openssl/hmac.h>
#include <simdutf.h>

#include <capnp/compat/json.h>
#include <capnp/message.h>
#include <kj/compat/url.h>
#include <kj/debug.h>

#include <climits>
#include <cmath>
#include <cstring>
#include <new>

namespace workerd::rt::luajit {
namespace {

constexpr char ASYNC_API_SOURCE[] = R"lua(
workerd = {}
workerd.null = setmetatable({}, { __tostring = function() return "workerd.null" end })

function workerd.await(operation)
  local succeeded, value = coroutine.yield(operation)
  if not succeeded then error(value, 2) end
  return value
end

function workerd.sleep(milliseconds)
  if type(milliseconds) ~= "number" or milliseconds ~= math.floor(milliseconds) or
      milliseconds < 0 or milliseconds > 4294967295 then
    error("workerd.sleep() requires an integer duration from 0 through 4294967295 milliseconds", 2)
  end
  return workerd.await({ operation = "sleep", milliseconds = milliseconds })
end

function workerd.randomBytes(size)
  if type(size) ~= "number" or size ~= math.floor(size) or size < 0 or size > 65536 then
    error("workerd.randomBytes() requires an integer size from 0 through 65536", 2)
  end
  return workerd.await({ operation = "randomBytes", size = size })
end

function workerd.randomUUID()
  return workerd.await({ operation = "randomUUID" })
end

function workerd.createServiceBinding(serviceBindingIndex)
  return {
    fetch = function(request)
      if type(request) ~= "table" then
        error("service binding fetch() requires a request table", 2)
      end
      return workerd.await({
        operation = "fetch",
        serviceBindingIndex = serviceBindingIndex,
        request = request,
      })
    end,
  }
end

function workerd.createKvNamespace(kvBindingIndex)
  return {
    get = function(key, options)
      if type(key) ~= "string" then error("KV get() requires a string key", 2) end
      if options ~= nil and type(options) ~= "table" then
        error("KV get() options must be a table", 2)
      end
      return workerd.await({
        operation = "kvGet",
        kvBindingIndex = kvBindingIndex,
        key = key,
        options = options,
      })
    end,
    getWithMetadata = function(key, options)
      if type(key) ~= "string" then error("KV getWithMetadata() requires a string key", 2) end
      if options ~= nil and type(options) ~= "table" then
        error("KV getWithMetadata() options must be a table", 2)
      end
      return workerd.await({
        operation = "kvGetWithMetadata",
        kvBindingIndex = kvBindingIndex,
        key = key,
        options = options,
      })
    end,
    put = function(key, value, options)
      if type(key) ~= "string" then error("KV put() requires a string key", 2) end
      if type(value) ~= "string" then error("KV put() requires a string value", 2) end
      if options ~= nil and type(options) ~= "table" then
        error("KV put() options must be a table", 2)
      end
      return workerd.await({
        operation = "kvPut",
        kvBindingIndex = kvBindingIndex,
        key = key,
        value = value,
        options = options,
      })
    end,
    delete = function(key)
      if type(key) ~= "string" then error("KV delete() requires a string key", 2) end
      return workerd.await({ operation = "kvDelete", kvBindingIndex = kvBindingIndex, key = key })
    end,
    list = function(options)
      if options ~= nil and type(options) ~= "table" then
        error("KV list() options must be a table", 2)
      end
      return workerd.await({
        operation = "kvList",
        kvBindingIndex = kvBindingIndex,
        options = options,
      })
    end,
  }
end

function workerd.createAiBinding(serviceBindingIndex)
  return {
    run = function(_, model, inputs, options)
      if type(model) ~= "string" or model == "" then
        error("Workers AI run() requires a model name", 2)
      end
      if type(inputs) ~= "table" then
        error("Workers AI run() requires an inputs table", 2)
      end
      if options ~= nil and type(options) ~= "table" then
        error("Workers AI run() options must be a table", 2)
      end
      local result = workerd.await({
        operation = "aiRun",
        serviceBindingIndex = serviceBindingIndex,
        model = model,
        inputs = inputs,
        options = options,
      })
      if result.status < 200 or result.status >= 300 then
        error("Workers AI request failed (" .. result.status .. "): " .. result.body, 2)
      end
      if result.isJson then return result.value end
      return result.body
    end,
  }
end

function workerd.createD1Database(serviceBindingIndex)
  local function makeStatement(sql, params)
    local function execute(mode)
      local result = workerd.await({
        operation = "d1Query",
        serviceBindingIndex = serviceBindingIndex,
        sql = sql,
        params = params,
        mode = mode,
      })
      if type(result) ~= "table" or result.success ~= true then
        local message = type(result) == "table" and result.error or nil
        error("D1_ERROR: " .. tostring(message or "D1 query failed"), 2)
      end
      return result
    end

    local function all()
      local result = execute("all")
      local rowsAndColumns = result.results
      if type(rowsAndColumns) ~= "table" or type(rowsAndColumns.columns) ~= "table" or
          type(rowsAndColumns.rows) ~= "table" then
        error("D1_ERROR: malformed rows-and-columns response", 2)
      end
      local rows = {}
      for rowIndex, values in ipairs(rowsAndColumns.rows) do
        if type(values) ~= "table" then
          error("D1_ERROR: malformed row in query response", 2)
        end
        local row = {}
        for columnIndex, name in ipairs(rowsAndColumns.columns) do
          if type(name) ~= "string" then
            error("D1_ERROR: malformed column name in query response", 2)
          end
          row[name] = values[columnIndex]
        end
        rows[rowIndex] = row
      end
      result.results = rows
      return result
    end

    return {
      bind = function(_, ...)
        local bound = { n = select("#", ...) }
        if bound.n > 100 then error("D1 bind() accepts at most 100 values", 2) end
        for i = 1, bound.n do bound[i] = select(i, ...) end
        return makeStatement(sql, bound)
      end,
      all = all,
      first = function(_, columnName)
        local rows = all().results
        local row = rows[1]
        if row == nil then return nil end
        if columnName ~= nil then
          if type(columnName) ~= "string" then
            error("D1 first() column name must be a string", 2)
          end
          if row[columnName] == nil then
            error("D1_COLUMN_NOTFOUND: Column not found (" .. columnName .. ")", 2)
          end
          return row[columnName]
        end
        return row
      end,
      run = function() return execute("run") end,
    }
  end

  return {
    prepare = function(_, sql)
      if type(sql) ~= "string" then error("D1 prepare() requires a SQL string", 2) end
      if #sql > 65536 then error("D1 prepare() SQL exceeds the 65536-byte limit", 2) end
      return makeStatement(sql, { n = 0 })
    end,
  }
end

function workerd._makeBodyProducer(producer)
  if type(producer) ~= "function" then
    error("request body producer must be a function", 2)
  end
  return function()
    while true do
      local chunk = producer()
      coroutine.yield({ operation = "bodyProducerChunk", chunk = chunk })
    end
  end
end

function workerd.fetch(request)
  if type(request) ~= "table" then
    error("workerd.fetch() requires a request table", 2)
  end
  return workerd.await({ operation = "fetch", request = request })
end

local function responseBodyToken(body)
  if type(body) ~= "table" or type(body.requestId) ~= "string" or
      type(body.streamId) ~= "number" or body.streamId ~= math.floor(body.streamId) or
      body.streamId < 1 then
    error("expected a response body stream", 3)
  end
  return body.requestId, body.streamId
end

function workerd.readBody(body, maxBytes)
  local requestId, streamId = responseBodyToken(body)
  if maxBytes == nil then maxBytes = 16384 end
  if type(maxBytes) ~= "number" or maxBytes ~= math.floor(maxBytes) or
      maxBytes < 1 or maxBytes > 65536 then
    error("workerd.readBody() chunk size must be an integer from 1 through 65536", 2)
  end
  return workerd.await({
    operation = "readBody", requestId = requestId, streamId = streamId, maxBytes = maxBytes,
  })
end

function workerd.cancelBody(body)
  local requestId, streamId = responseBodyToken(body)
  return workerd.await({ operation = "cancelBody", requestId = requestId, streamId = streamId })
end

function workerd.readAll(body)
  local chunks = {}
  while true do
    local chunk = workerd.readBody(body, 65536)
    if chunk == nil then break end
    chunks[#chunks + 1] = chunk
  end
  return table.concat(chunks)
end

local function requestBodyToken(body)
  if type(body) ~= "table" or type(body.requestId) ~= "string" or body.streamId ~= 0 then
    error("expected an inbound request body stream", 3)
  end
  return body.requestId
end

function workerd.readRequestBody(body, maxBytes)
  local requestId = requestBodyToken(body)
  if maxBytes == nil then maxBytes = 16384 end
  if type(maxBytes) ~= "number" or maxBytes ~= math.floor(maxBytes) or
      maxBytes < 1 or maxBytes > 65536 then
    error("workerd.readRequestBody() chunk size must be an integer from 1 through 65536", 2)
  end
  return workerd.await({
    operation = "readRequestBody", requestId = requestId, maxBytes = maxBytes,
  })
end
)lua";

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
    KJ_FAIL_REQUIRE("Lua table field must be a string", fieldName);
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

int pushUrlParseError(lua_State* state, kj::StringPtr message) {
  lua_pushnil(state);
  lua_pushlstring(state, message.begin(), message.size());
  return 2;
}

void pushParsedUrl(lua_State* state, kj::Url& url) {
  KJ_REQUIRE(url.path.size() <= INT_MAX && url.query.size() <= INT_MAX,
      "parsed URL has too many components");
  lua_createtable(state, 0, 9);
  setStringField(state, "href", url.toString());
  setStringField(state, "scheme", url.scheme);
  setStringField(state, "host", url.host);

  lua_createtable(state, static_cast<int>(url.path.size()), 0);
  int index = 1;
  for (auto& component: url.path) {
    lua_pushlstring(state, component.begin(), component.size());
    lua_rawseti(state, -2, index++);
  }
  lua_setfield(state, -2, "path");

  lua_createtable(state, static_cast<int>(url.query.size()), 0);
  index = 1;
  for (auto& parameter: url.query) {
    lua_createtable(state, 0, 2);
    setStringField(state, "name", parameter.name);
    if (parameter.value != nullptr) setStringField(state, "value", parameter.value);
    lua_rawseti(state, -2, index++);
  }
  lua_setfield(state, -2, "query");

  lua_pushboolean(state, url.hasTrailingSlash);
  lua_setfield(state, -2, "hasTrailingSlash");
  KJ_IF_SOME(fragment, url.fragment) {
    setStringField(state, "fragment", fragment);
  }
  KJ_IF_SOME(userInfo, url.userInfo) {
    setStringField(state, "username", userInfo.username);
    KJ_IF_SOME(password, userInfo.password) {
      setStringField(state, "password", password);
    }
  }
}

bool equalsAsciiCaseInsensitive(const char* value, size_t valueLength, const char* expected) {
  size_t expectedLength = std::strlen(expected);
  if (valueLength != expectedLength) return false;
  for (size_t i = 0; i < valueLength; ++i) {
    char character = value[i];
    if (character >= 'a' && character <= 'z') character -= 'a' - 'A';
    if (character != expected[i]) return false;
  }
  return true;
}

const EVP_MD* findLuaDigestAlgorithm(const char* algorithm, size_t algorithmLength) {
  if (equalsAsciiCaseInsensitive(algorithm, algorithmLength, "MD5")) return EVP_md5();
  if (equalsAsciiCaseInsensitive(algorithm, algorithmLength, "SHA-1")) return EVP_sha1();
  if (equalsAsciiCaseInsensitive(algorithm, algorithmLength, "SHA-256")) return EVP_sha256();
  if (equalsAsciiCaseInsensitive(algorithm, algorithmLength, "SHA-384")) return EVP_sha384();
  if (equalsAsciiCaseInsensitive(algorithm, algorithmLength, "SHA-512")) return EVP_sha512();
  return nullptr;
}

int digest(lua_State* state) {
  if (lua_gettop(state) != 2 || lua_type(state, 1) != LUA_TSTRING ||
      lua_type(state, 2) != LUA_TSTRING) {
    return luaL_error(state, "workerd.digest() expects an algorithm and a binary string");
  }

  size_t algorithmLength = 0;
  const char* algorithm = lua_tolstring(state, 1, &algorithmLength);
  const EVP_MD* digestAlgorithm = findLuaDigestAlgorithm(algorithm, algorithmLength);
  if (digestAlgorithm == nullptr) {
    return luaL_error(state, "workerd.digest() supports MD5, SHA-1, SHA-256, SHA-384, and SHA-512");
  }

  size_t inputLength = 0;
  const char* input = lua_tolstring(state, 2, &inputLength);
  if (inputLength > LuaRuntime::MAX_CRYPTO_INPUT_BYTES) {
    return luaL_error(state, "workerd.digest() input exceeds the 1048576-byte limit");
  }

  unsigned char output[64];
  int expectedOutputLength = EVP_MD_size(digestAlgorithm);
  if (expectedOutputLength <= 0 || static_cast<size_t>(expectedOutputLength) > sizeof(output)) {
    return luaL_error(state, "workerd.digest() failed");
  }
  unsigned int outputLength = 0;
  if (EVP_Digest(input, inputLength, output, &outputLength, digestAlgorithm, nullptr) != 1 ||
      outputLength != static_cast<unsigned int>(expectedOutputLength)) {
    return luaL_error(state, "workerd.digest() failed");
  }
  lua_pushlstring(state, reinterpret_cast<const char*>(output), outputLength);
  return 1;
}

bool computeHmac(const EVP_MD* algorithm,
    const char* key,
    size_t keyLength,
    const char* input,
    size_t inputLength,
    unsigned char output[64],
    unsigned int& outputLength) {
  int expectedOutputLength = EVP_MD_size(algorithm);
  if (expectedOutputLength <= 0 || static_cast<size_t>(expectedOutputLength) > 64) return false;
  auto result = HMAC(algorithm, key, static_cast<int>(keyLength),
      reinterpret_cast<const unsigned char*>(input), inputLength, output, &outputLength);
  return result != nullptr && outputLength == static_cast<unsigned int>(expectedOutputLength);
}

constexpr char LUA_HMAC_KEY_METATABLE[] = "workerd.HmacCryptoKey";

struct LuaHmacKeyUserdata {
  kj::Own<LuaHmacKey> key;
};

int destroyLuaHmacKey(lua_State* state) {
  auto* userdata = static_cast<LuaHmacKeyUserdata*>(lua_touserdata(state, 1));
  if (userdata != nullptr) userdata->~LuaHmacKeyUserdata();
  return 0;
}

int stringifyLuaHmacKey(lua_State* state) {
  lua_pushliteral(state, "[CryptoKey]");
  return 1;
}

void installLuaHmacKeyMetatable(lua_State* state) {
  if (luaL_newmetatable(state, LUA_HMAC_KEY_METATABLE) != 0) {
    lua_pushcfunction(state, destroyLuaHmacKey);
    lua_setfield(state, -2, "__gc");
    lua_pushcfunction(state, stringifyLuaHmacKey);
    lua_setfield(state, -2, "__tostring");
    lua_pushboolean(state, 0);
    lua_setfield(state, -2, "__metatable");
  }
  lua_pop(state, 1);
}

void pushLuaHmacKey(lua_State* state, kj::Own<LuaHmacKey> key) {
  KJ_REQUIRE(key.get() != nullptr, "LuaJIT HMAC key binding has no key data");
  new (lua_newuserdata(state, sizeof(LuaHmacKeyUserdata))) LuaHmacKeyUserdata{kj::mv(key)};
  luaL_getmetatable(state, LUA_HMAC_KEY_METATABLE);
  KJ_REQUIRE(lua_istable(state, -1), "LuaJIT HMAC key metatable is unavailable");
  lua_setmetatable(state, -2);
}

LuaHmacKeyUserdata* checkLuaHmacKey(lua_State* state, int index) {
  return static_cast<LuaHmacKeyUserdata*>(luaL_checkudata(state, index, LUA_HMAC_KEY_METATABLE));
}

bool hasLuaHmacKeyUsage(const LuaHmacKey& key, LuaHmacKeyUsage usage) {
  for (auto candidate: key.getUsages()) {
    if (candidate == usage) return true;
  }
  return false;
}

bool isSupportedBoundHmacAlgorithm(const EVP_MD* algorithm) {
  return algorithm == EVP_sha1() || algorithm == EVP_sha256() || algorithm == EVP_sha384() ||
      algorithm == EVP_sha512();
}

int hmac(lua_State* state) {
  const EVP_MD* digestAlgorithm = nullptr;
  const char* keyBytes = nullptr;
  size_t keyLength = 0;
  const char* input = nullptr;
  size_t inputLength = 0;

  if (lua_gettop(state) == 2 && lua_type(state, 1) == LUA_TUSERDATA) {
    auto* userdata = checkLuaHmacKey(state, 1);
    if (!hasLuaHmacKeyUsage(*userdata->key, LuaHmacKeyUsage::SIGN)) {
      return luaL_error(state, "workerd.hmac() CryptoKey does not allow sign usage");
    }
    auto algorithmName = userdata->key->getHashAlgorithm();
    digestAlgorithm = findLuaDigestAlgorithm(algorithmName.begin(), algorithmName.size());
    if (!isSupportedBoundHmacAlgorithm(digestAlgorithm)) {
      return luaL_error(state, "workerd.hmac() CryptoKey has an unsupported HMAC hash");
    }
    auto keyData = userdata->key->getKeyData();
    keyBytes = reinterpret_cast<const char*>(keyData.begin());
    keyLength = keyData.size();
    if (lua_type(state, 2) != LUA_TSTRING) {
      return luaL_error(state, "workerd.hmac() input must be a binary string");
    }
    input = lua_tolstring(state, 2, &inputLength);
  } else if (lua_gettop(state) == 3 && lua_type(state, 1) == LUA_TSTRING &&
      lua_type(state, 2) == LUA_TSTRING && lua_type(state, 3) == LUA_TSTRING) {
    size_t algorithmLength = 0;
    const char* algorithm = lua_tolstring(state, 1, &algorithmLength);
    digestAlgorithm = findLuaDigestAlgorithm(algorithm, algorithmLength);
    if (digestAlgorithm == nullptr) {
      return luaL_error(state, "workerd.hmac() supports MD5, SHA-1, SHA-256, SHA-384, and SHA-512");
    }
    keyBytes = lua_tolstring(state, 2, &keyLength);
    input = lua_tolstring(state, 3, &inputLength);
  } else {
    return luaL_error(
        state, "workerd.hmac() expects (algorithm, rawKey, input) or (cryptoKey, input)");
  }

  if (keyLength == 0 || keyLength > LuaRuntime::MAX_HMAC_KEY_BYTES) {
    return luaL_error(state, "workerd.hmac() key must be from 1 through 65536 bytes");
  }
  if (inputLength > LuaRuntime::MAX_CRYPTO_INPUT_BYTES) {
    return luaL_error(state, "workerd.hmac() input exceeds the 1048576-byte limit");
  }

  unsigned char output[64];
  unsigned int outputLength = 0;
  if (!computeHmac(
          digestAlgorithm, keyBytes, keyLength, input, inputLength, output, outputLength)) {
    return luaL_error(state, "workerd.hmac() failed");
  }
  lua_pushlstring(state, reinterpret_cast<const char*>(output), outputLength);
  return 1;
}

int verifyHmac(lua_State* state) {
  const EVP_MD* digestAlgorithm = nullptr;
  const char* keyBytes = nullptr;
  size_t keyLength = 0;
  const char* input = nullptr;
  size_t inputLength = 0;
  const char* mac = nullptr;
  size_t macLength = 0;

  if (lua_gettop(state) == 3 && lua_type(state, 1) == LUA_TUSERDATA) {
    auto* userdata = checkLuaHmacKey(state, 1);
    if (!hasLuaHmacKeyUsage(*userdata->key, LuaHmacKeyUsage::VERIFY)) {
      return luaL_error(state, "workerd.verifyHmac() CryptoKey does not allow verify usage");
    }
    auto algorithmName = userdata->key->getHashAlgorithm();
    digestAlgorithm = findLuaDigestAlgorithm(algorithmName.begin(), algorithmName.size());
    if (!isSupportedBoundHmacAlgorithm(digestAlgorithm)) {
      return luaL_error(state, "workerd.verifyHmac() CryptoKey has an unsupported HMAC hash");
    }
    auto keyData = userdata->key->getKeyData();
    keyBytes = reinterpret_cast<const char*>(keyData.begin());
    keyLength = keyData.size();
    if (lua_type(state, 2) != LUA_TSTRING || lua_type(state, 3) != LUA_TSTRING) {
      return luaL_error(state, "workerd.verifyHmac() input and MAC must be binary strings");
    }
    input = lua_tolstring(state, 2, &inputLength);
    mac = lua_tolstring(state, 3, &macLength);
  } else if (lua_gettop(state) == 4 && lua_type(state, 1) == LUA_TSTRING &&
      lua_type(state, 2) == LUA_TSTRING && lua_type(state, 3) == LUA_TSTRING &&
      lua_type(state, 4) == LUA_TSTRING) {
    size_t algorithmLength = 0;
    const char* algorithm = lua_tolstring(state, 1, &algorithmLength);
    digestAlgorithm = findLuaDigestAlgorithm(algorithm, algorithmLength);
    if (digestAlgorithm == nullptr) {
      return luaL_error(
          state, "workerd.verifyHmac() supports MD5, SHA-1, SHA-256, SHA-384, and SHA-512");
    }
    keyBytes = lua_tolstring(state, 2, &keyLength);
    input = lua_tolstring(state, 3, &inputLength);
    mac = lua_tolstring(state, 4, &macLength);
  } else {
    return luaL_error(state,
        "workerd.verifyHmac() expects (algorithm, rawKey, input, mac) or (cryptoKey, input, mac)");
  }

  if (keyLength == 0 || keyLength > LuaRuntime::MAX_HMAC_KEY_BYTES) {
    return luaL_error(state, "workerd.verifyHmac() key must be from 1 through 65536 bytes");
  }
  if (inputLength > LuaRuntime::MAX_CRYPTO_INPUT_BYTES) {
    return luaL_error(state, "workerd.verifyHmac() input exceeds the 1048576-byte limit");
  }

  unsigned char output[64];
  unsigned int outputLength = 0;
  if (!computeHmac(
          digestAlgorithm, keyBytes, keyLength, input, inputLength, output, outputLength)) {
    return luaL_error(state, "workerd.verifyHmac() failed");
  }
  bool valid = macLength == outputLength && CRYPTO_memcmp(output, mac, outputLength) == 0;
  lua_pushboolean(state, valid);
  return 1;
}

int parseUrl(lua_State* state) {
  int argumentCount = lua_gettop(state);
  if (argumentCount < 1 || argumentCount > 2 || lua_type(state, 1) != LUA_TSTRING ||
      (argumentCount == 2 && lua_type(state, 2) != LUA_TSTRING)) {
    return pushUrlParseError(
        state, "workerd.parseUrl() expects a URL string and optional base URL string");
  }

  size_t inputLength = 0;
  const char* inputBytes = lua_tolstring(state, 1, &inputLength);
  kj::StringPtr input(inputBytes, inputLength);

  try {
    kj::Maybe<kj::Url> parsed = kj::none;
    if (argumentCount == 1) {
      parsed = kj::Url::tryParse(input);
    } else {
      size_t baseLength = 0;
      const char* baseBytes = lua_tolstring(state, 2, &baseLength);
      auto base = kj::Url::tryParse(kj::StringPtr(baseBytes, baseLength));
      if (base == kj::none) {
        return pushUrlParseError(state, "workerd.parseUrl() base must be an absolute remote URL");
      }
      KJ_IF_SOME(baseUrl, base) {
        parsed = baseUrl.tryParseRelative(input);
      }
    }

    KJ_IF_SOME(url, parsed) {
      pushParsedUrl(state, url);
      return 1;
    }
    return pushUrlParseError(state, "workerd.parseUrl() could not parse URL");
  } catch (const kj::Exception& exception) {
    return pushUrlParseError(state, exception.getDescription());
  }
}

kj::Duration readSleepDelay(lua_State* state) {
  KJ_REQUIRE(lua_gettop(state) == 1, "LuaJIT host operation must yield one descriptor");
  KJ_REQUIRE(lua_istable(state, -1), "LuaJIT host operation descriptor must be a table");

  getRawField(state, -1, "operation");
  size_t operationLength = 0;
  const char* operation = lua_tolstring(state, -1, &operationLength);
  bool isSleep = operation != nullptr && kj::StringPtr(operation, operationLength) == "sleep";
  lua_pop(state, 1);
  KJ_REQUIRE(isSleep, "unsupported LuaJIT host operation");

  getRawField(state, -1, "milliseconds");
  int isNumber = 0;
  lua_Integer milliseconds = lua_tointegerx(state, -1, &isNumber);
  bool validDuration = lua_type(state, -1) == LUA_TNUMBER && isNumber && milliseconds >= 0 &&
      static_cast<uint64_t>(milliseconds) <= UINT32_MAX &&
      static_cast<lua_Number>(milliseconds) == lua_tonumber(state, -1);
  lua_pop(state, 1);
  KJ_REQUIRE(validDuration, "workerd.sleep() requires an integer duration from 0 through",
      UINT32_MAX, "milliseconds");
  return static_cast<uint32_t>(milliseconds) * kj::MILLISECONDS;
}

void pushWorkerdNull(lua_State* state) {
  lua_getglobal(state, "workerd");
  KJ_REQUIRE(lua_istable(state, -1), "LuaJIT workerd API table is unavailable");
  lua_getfield(state, -1, "null");
  lua_remove(state, -2);
}

void pushJsonValue(lua_State* state, capnp::JsonValue::Reader value, size_t depth = 0) {
  KJ_REQUIRE(depth <= 64, "JSON binding exceeds maximum nesting depth");
  switch (value.which()) {
    case capnp::JsonValue::NULL_:
      pushWorkerdNull(state);
      return;
    case capnp::JsonValue::BOOLEAN:
      lua_pushboolean(state, value.getBoolean());
      return;
    case capnp::JsonValue::NUMBER:
      lua_pushnumber(state, value.getNumber());
      return;
    case capnp::JsonValue::STRING: {
      auto string = value.getString();
      lua_pushlstring(state, string.begin(), string.size());
      return;
    }
    case capnp::JsonValue::ARRAY: {
      auto elements = value.getArray();
      KJ_REQUIRE(elements.size() <= INT_MAX, "JSON array is too large for LuaJIT");
      lua_createtable(state, static_cast<int>(elements.size()), 0);
      size_t index = 1;
      for (auto element: elements) {
        pushJsonValue(state, element, depth + 1);
        lua_rawseti(state, -2, static_cast<int>(index++));
      }
      return;
    }
    case capnp::JsonValue::OBJECT: {
      lua_createtable(state, 0, 0);
      for (auto field: value.getObject()) {
        auto name = field.getName();
        lua_pushlstring(state, name.begin(), name.size());
        pushJsonValue(state, field.getValue(), depth + 1);
        lua_rawset(state, -3);
      }
      return;
    }
    case capnp::JsonValue::CALL:
    case capnp::JsonValue::RAW:
      KJ_FAIL_REQUIRE(
          "Lua JSON bindings do not support Cap'n Proto JSON extensions", value.which());
  }
  KJ_UNREACHABLE;
}

void pushJsonText(lua_State* state, kj::StringPtr json) {
  capnp::JsonCodec codec;
  capnp::MallocMessageBuilder arena;
  auto root = arena.initRoot<capnp::JsonValue>();
  codec.decodeRaw(json, root);
  pushJsonValue(state, root.asReader());
}

void pushLuaKvListResult(lua_State* state, kj::StringPtr json, kj::Maybe<kj::String> cacheStatus) {
  pushJsonText(state, json);
  int resultIndex = absoluteIndex(state, -1);
  KJ_REQUIRE(lua_istable(state, resultIndex), "KV list response must be a JSON object");
  getRawField(state, resultIndex, "list_complete");
  KJ_REQUIRE(lua_type(state, -1) == LUA_TBOOLEAN,
      "KV list response must contain a boolean list_complete field");
  lua_pop(state, 1);
  getRawField(state, resultIndex, "cursor");
  KJ_REQUIRE(lua_type(state, -1) == LUA_TNIL || lua_type(state, -1) == LUA_TSTRING,
      "KV list response cursor must be a string when present");
  lua_pop(state, 1);

  getRawField(state, resultIndex, "keys");
  KJ_REQUIRE(lua_istable(state, -1), "KV list response must contain a keys array");
  int keysIndex = absoluteIndex(state, -1);
  size_t keyCount = lua_objlen(state, keysIndex);
  KJ_REQUIRE(keyCount <= INT_MAX, "KV list response contains too many keys");

  for (size_t i = 1; i <= keyCount; ++i) {
    lua_rawgeti(state, keysIndex, static_cast<int>(i));
    KJ_REQUIRE(lua_istable(state, -1), "KV list key must be a JSON object");
    int keyIndex = absoluteIndex(state, -1);
    getRawField(state, keyIndex, "name");
    KJ_REQUIRE(lua_type(state, -1) == LUA_TSTRING, "KV list key must have a string name");
    lua_pop(state, 1);
    getRawField(state, keyIndex, "metadata");
    if (lua_type(state, -1) == LUA_TSTRING) {
      size_t metadataSize = 0;
      const char* metadata = lua_tolstring(state, -1, &metadataSize);
      auto metadataJson = kj::heapString(metadata, metadataSize);
      lua_pop(state, 1);
      lua_pushstring(state, "metadata");
      pushJsonText(state, metadataJson);
      lua_rawset(state, keyIndex);
    } else {
      lua_pop(state, 1);
    }
    lua_pop(state, 1);
  }
  lua_pop(state, 1);

  lua_pushstring(state, "cacheStatus");
  KJ_IF_SOME(status, cacheStatus) {
    lua_pushlstring(state, status.begin(), status.size());
  } else {
    pushWorkerdNull(state);
  }
  lua_rawset(state, resultIndex);
}

int getRequestCf(lua_State* state) {
  if (lua_type(state, 2) != LUA_TSTRING) {
    lua_pushnil(state);
    return 1;
  }
  size_t nameSize = 0;
  const char* name = lua_tolstring(state, 2, &nameSize);
  if (kj::StringPtr(name, nameSize) != "cf") {
    lua_pushnil(state);
    return 1;
  }

  int originalTop = lua_gettop(state);
  {
    size_t jsonSize = 0;
    const char* json = lua_tolstring(state, lua_upvalueindex(1), &jsonSize);
    try {
      capnp::JsonCodec codec;
      capnp::MallocMessageBuilder arena;
      auto root = arena.initRoot<capnp::JsonValue>();
      codec.decodeRaw(kj::StringPtr(json, jsonSize), root);
      KJ_REQUIRE(root.asReader().which() == capnp::JsonValue::OBJECT,
          "LuaJIT request.cf metadata must be a JSON object");
      pushJsonValue(state, root.asReader());
      lua_pushvalue(state, -1);
      lua_setfield(state, 1, "cf");
      return 1;
    } catch (const kj::Exception& exception) {
      lua_settop(state, originalTop);
      auto description = exception.getDescription();
      lua_pushlstring(state, description.begin(), description.size());
    }
  }

  return lua_error(state);
}

void installRequestCfJson(lua_State* state, int tableIndex, kj::StringPtr json) {
  tableIndex = absoluteIndex(state, tableIndex);
  lua_createtable(state, 0, 1);
  lua_pushlstring(state, json.begin(), json.size());
  lua_pushcclosure(state, getRequestCf, 1);
  lua_setfield(state, -2, "__index");
  lua_setmetatable(state, tableIndex);
}

void pushLuaBindings(lua_State* state, kj::ArrayPtr<LuaBinding> bindings) {
  KJ_REQUIRE(bindings.size() <= INT_MAX, "too many Lua bindings");
  installLuaHmacKeyMetatable(state);
  lua_getglobal(state, "workerd");
  KJ_REQUIRE(lua_istable(state, -1), "LuaJIT workerd API table is unavailable");
  lua_createtable(state, 0, static_cast<int>(bindings.size()));
  for (auto& binding: bindings) {
    lua_pushlstring(state, binding.name.begin(), binding.name.size());
    KJ_SWITCH_ONEOF(binding.value) {
      KJ_CASE_ONEOF(text, LuaBinding::TextValue) {
        lua_pushlstring(state, text.value.begin(), text.value.size());
      }
      KJ_CASE_ONEOF(data, LuaBinding::DataValue) {
        const char* bytes =
            data.value.size() == 0 ? "" : reinterpret_cast<const char*>(data.value.begin());
        lua_pushlstring(state, bytes, data.value.size());
      }
      KJ_CASE_ONEOF(json, LuaBinding::JsonValue) {
        capnp::JsonCodec codec;
        capnp::MallocMessageBuilder arena;
        auto root = arena.initRoot<capnp::JsonValue>();
        KJ_IF_SOME(exception, kj::runCatchingExceptions([&]() {
          codec.decodeRaw(json.value, root);
          pushJsonValue(state, root.asReader());
        })) {
          KJ_FAIL_REQUIRE("LuaJIT JSON binding", binding.name, "is invalid:", exception);
        }
      }
      KJ_CASE_ONEOF(hmacKey, LuaBinding::HmacKeyValue) {
        KJ_REQUIRE(hmacKey.key.get() != nullptr, "LuaJIT HMAC key binding has no key data");
        auto algorithmName = hmacKey.key->getHashAlgorithm();
        auto* algorithm = findLuaDigestAlgorithm(algorithmName.begin(), algorithmName.size());
        KJ_REQUIRE(isSupportedBoundHmacAlgorithm(algorithm),
            "LuaJIT CryptoKey binding only supports HMAC with SHA-1/SHA-2");
        pushLuaHmacKey(state, kj::mv(hmacKey.key));
      }
      KJ_CASE_ONEOF(nullValue, LuaBinding::NullValue) {
        getRawField(state, -3, "null");
        KJ_REQUIRE(!lua_isnil(state, -1), "LuaJIT null sentinel is unavailable");
      }
      KJ_CASE_ONEOF(service, LuaBinding::ServiceBinding) {
        getRawField(state, -3, "createServiceBinding");
        KJ_REQUIRE(lua_isfunction(state, -1), "LuaJIT service binding factory is unavailable");
        KJ_REQUIRE(service.index < static_cast<size_t>(INT_MAX), "too many Lua service bindings");
        lua_pushinteger(state, static_cast<lua_Integer>(service.index + 1));
        int status = lua_pcall(state, 1, 1, 0);
        KJ_REQUIRE(
            status == 0, "failed to create Lua service binding:", readLuaErrorMessage(state));
      }
      KJ_CASE_ONEOF(kvNamespace, LuaBinding::KvNamespaceBinding) {
        getRawField(state, -3, "createKvNamespace");
        KJ_REQUIRE(lua_isfunction(state, -1), "LuaJIT KV binding factory is unavailable");
        KJ_REQUIRE(kvNamespace.index < static_cast<size_t>(INT_MAX), "too many Lua KV bindings");
        lua_pushinteger(state, static_cast<lua_Integer>(kvNamespace.index + 1));
        int status = lua_pcall(state, 1, 1, 0);
        KJ_REQUIRE(status == 0, "failed to create Lua KV binding:", readLuaErrorMessage(state));
      }
      KJ_CASE_ONEOF(ai, LuaBinding::AiBinding) {
        getRawField(state, -3, "createAiBinding");
        KJ_REQUIRE(lua_isfunction(state, -1), "LuaJIT Workers AI binding factory is unavailable");
        KJ_REQUIRE(ai.fetcherIndex < static_cast<size_t>(INT_MAX), "too many Lua service bindings");
        lua_pushinteger(state, static_cast<lua_Integer>(ai.fetcherIndex + 1));
        int status = lua_pcall(state, 1, 1, 0);
        KJ_REQUIRE(
            status == 0, "failed to create Lua Workers AI binding:", readLuaErrorMessage(state));
      }
      KJ_CASE_ONEOF(d1, LuaBinding::D1DatabaseBinding) {
        getRawField(state, -3, "createD1Database");
        KJ_REQUIRE(lua_isfunction(state, -1), "LuaJIT D1 binding factory is unavailable");
        KJ_REQUIRE(d1.fetcherIndex < static_cast<size_t>(INT_MAX), "too many Lua service bindings");
        lua_pushinteger(state, static_cast<lua_Integer>(d1.fetcherIndex + 1));
        int status = lua_pcall(state, 1, 1, 0);
        KJ_REQUIRE(status == 0, "failed to create Lua D1 binding:", readLuaErrorMessage(state));
      }
    }
    lua_rawset(state, -3);
  }
  lua_setfield(state, -2, "bindings");
  lua_pushnil(state);
  lua_setfield(state, -2, "createServiceBinding");
  lua_pushnil(state);
  lua_setfield(state, -2, "createKvNamespace");
  lua_pushnil(state);
  lua_setfield(state, -2, "createD1Database");
  lua_pushnil(state);
  lua_setfield(state, -2, "createAiBinding");
  lua_pop(state, 1);
}

kj::Vector<LuaRuntime::Header> readHttpHeaders(lua_State* state, int tableIndex) {
  tableIndex = absoluteIndex(state, tableIndex);
  KJ_REQUIRE(lua_istable(state, tableIndex), "HTTP headers must be an array");

  size_t count = lua_objlen(state, tableIndex);
  KJ_REQUIRE(count <= LuaRuntime::MAX_HTTP_HEADERS, "HTTP header list contains too many entries",
      count, "maximum", LuaRuntime::MAX_HTTP_HEADERS);

  kj::Vector<LuaRuntime::Header> headers(count);
  for (size_t i = 1; i <= count; ++i) {
    lua_rawgeti(state, tableIndex, static_cast<int>(i));
    KJ_REQUIRE(lua_istable(state, -1), "HTTP header entries must be tables", i);
    auto name = readStringField(state, -1, "name");
    auto value = readStringField(state, -1, "value");
    headers.add(LuaRuntime::Header{.name = kj::mv(name), .value = kj::mv(value)});
    lua_pop(state, 1);
  }
  return headers;
}

kj::Maybe<kj::String> readOptionalStringField(
    lua_State* state, int tableIndex, const char* fieldName) {
  getRawField(state, tableIndex, fieldName);
  if (lua_isnil(state, -1)) {
    lua_pop(state, 1);
    return kj::none;
  }
  if (lua_type(state, -1) != LUA_TSTRING) {
    KJ_FAIL_REQUIRE("Lua request field must be a string", fieldName);
  }
  size_t length = 0;
  const char* value = lua_tolstring(state, -1, &length);
  auto result = kj::heapString(value, length);
  lua_pop(state, 1);
  return result;
}

kj::String readBodyRequestId(lua_State* state, int tableIndex) {
  getRawField(state, tableIndex, "requestId");
  if (lua_type(state, -1) != LUA_TSTRING) {
    KJ_FAIL_REQUIRE("body stream requires a request identifier");
  }
  size_t length = 0;
  const char* value = lua_tolstring(state, -1, &length);
  auto result = kj::heapString(value, length);
  lua_pop(state, 1);
  return result;
}

kj::String readRequestBodyStreamId(lua_State* state, int tableIndex) {
  tableIndex = absoluteIndex(state, tableIndex);
  KJ_REQUIRE(lua_istable(state, tableIndex), "workerd.fetch() body stream must be a table");
  getRawField(state, tableIndex, "streamId");
  int isNumber = 0;
  lua_Integer streamId = lua_tointegerx(state, -1, &isNumber);
  bool validStreamId = lua_type(state, -1) == LUA_TNUMBER && isNumber && streamId == 0 &&
      static_cast<lua_Number>(streamId) == lua_tonumber(state, -1);
  lua_pop(state, 1);
  KJ_REQUIRE(validStreamId, "workerd.fetch() body table is not an inbound request stream");
  return readBodyRequestId(state, tableIndex);
}

class LuaRequestContext;

kj::Promise<int> resumeLuaCoroutine(lua_State* coroutine,
    int status,
    kj::Rc<LuaRequestContext> requestContext,
    bool allowProducerChunkYield = false);

class LuaFunctionRequestBodyProducer final: public LuaRequestBodyProducer {
 public:
  LuaFunctionRequestBodyProducer(
      lua_State* state, int functionRef, kj::Rc<LuaRequestContext> requestContext);
  ~LuaFunctionRequestBodyProducer() noexcept(false) override;

  kj::Promise<kj::Maybe<kj::Array<kj::byte>>> nextChunk() override;

 private:
  lua_State* state;
  lua_State* producerCoroutine;
  int functionRef;
  int coroutineRef;
  kj::Rc<LuaRequestContext> requestContext;
  size_t bytesGenerated = 0;
  bool started = false;
  bool reachedEof = false;
  bool pending = false;
  bool failed = false;
};

kj::Maybe<size_t> readOptionalServiceBindingIndex(lua_State* state, int descriptorIndex) {
  descriptorIndex = absoluteIndex(state, descriptorIndex);
  getRawField(state, descriptorIndex, "serviceBindingIndex");
  if (lua_isnil(state, -1)) {
    lua_pop(state, 1);
    return kj::none;
  }

  int isNumber = 0;
  lua_Integer index = lua_tointegerx(state, -1, &isNumber);
  bool validIndex = lua_type(state, -1) == LUA_TNUMBER && isNumber && index >= 1 &&
      index <= INT_MAX && static_cast<lua_Number>(index) == lua_tonumber(state, -1);
  lua_pop(state, 1);
  KJ_REQUIRE(validIndex, "service binding index must be a positive integer");
  return static_cast<size_t>(index - 1);
}

LuaHttpRequest readFetchRequest(lua_State* state,
    int tableIndex,
    kj::Maybe<size_t> serviceBindingIndex,
    kj::Rc<LuaRequestContext> requestContext,
    int bodyProducerWrapperRef) {
  tableIndex = absoluteIndex(state, tableIndex);
  KJ_REQUIRE(lua_istable(state, tableIndex), "workerd.fetch() request must be a table");

  kj::String method;
  KJ_IF_SOME(value, readOptionalStringField(state, tableIndex, "method")) {
    method = kj::mv(value);
  } else {
    method = kj::str("GET");
  }
  auto url = readStringField(state, tableIndex, "url");

  kj::Vector<LuaRuntime::Header> headers;
  getRawField(state, tableIndex, "headers");
  if (!lua_isnil(state, -1)) {
    headers = readHttpHeaders(state, -1);
  }
  lua_pop(state, 1);

  kj::Array<kj::byte> body = kj::heapArray<kj::byte>(0);
  kj::Maybe<kj::String> bodyStreamRequestId = kj::none;
  kj::Maybe<kj::Own<LuaRequestBodyProducer>> bodyProducer = kj::none;
  getRawField(state, tableIndex, "body");
  if (!lua_isnil(state, -1)) {
    if (lua_type(state, -1) == LUA_TSTRING) {
      size_t length = 0;
      const char* bytes = lua_tolstring(state, -1, &length);
      KJ_REQUIRE(length <= LuaRuntime::MAX_HTTP_BODY_BYTES,
          "workerd.fetch() request body exceeds limit", LuaRuntime::MAX_HTTP_BODY_BYTES);
      body = kj::heapArray<kj::byte>(length);
      if (length > 0) std::memcpy(body.begin(), bytes, length);
    } else if (lua_istable(state, -1)) {
      bodyStreamRequestId = readRequestBodyStreamId(state, absoluteIndex(state, -1));
    } else if (lua_isfunction(state, -1)) {
      int producerIndex = absoluteIndex(state, -1);
      lua_rawgeti(state, LUA_REGISTRYINDEX, bodyProducerWrapperRef);
      lua_pushvalue(state, producerIndex);
      int wrapperStatus = lua_pcall(state, 1, 1, 0);
      KJ_REQUIRE(wrapperStatus == 0,
          "could not create LuaJIT request body producer:", readLuaErrorMessage(state));
      int functionRef = luaL_ref(state, LUA_REGISTRYINDEX);
      bodyProducer =
          kj::heap<LuaFunctionRequestBodyProducer>(state, functionRef, kj::mv(requestContext));
    } else {
      KJ_FAIL_REQUIRE(
          "workerd.fetch() request body must be a string, request body stream, producer function, or nil");
    }
  }
  lua_pop(state, 1);

  KJ_REQUIRE(headers.size() <= LuaRuntime::MAX_HTTP_HEADERS,
      "workerd.fetch() request has too many headers", LuaRuntime::MAX_HTTP_HEADERS);
  return LuaHttpRequest{
    .method = kj::mv(method),
    .url = kj::mv(url),
    .headers = kj::mv(headers),
    .body = kj::mv(body),
    .bodyStreamRequestId = kj::mv(bodyStreamRequestId),
    .bodyProducer = kj::mv(bodyProducer),
    .serviceBindingIndex = kj::mv(serviceBindingIndex),
  };
}

struct LuaBodyReadRequest {
  kj::String requestId;
  size_t streamId;
  size_t maxBytes;
};

struct LuaBodyCancelRequest {
  kj::String requestId;
  size_t streamId;
};

struct LuaRequestBodyReadRequest {
  kj::String requestId;
  size_t maxBytes;
};

struct LuaRandomBytesRequest {
  size_t size;
};

struct LuaRandomUuidRequest {};

struct LuaProducerChunkYield {};

struct LuaKvGetRequest {
  size_t bindingIndex;
  kj::String key;
  LuaKvGetOptions options;
  LuaKvGetMode mode;
};

struct LuaKvPutRequest {
  size_t bindingIndex;
  kj::String key;
  kj::Array<kj::byte> value;
  LuaKvPutOptions options;
};

struct LuaKvDeleteRequest {
  size_t bindingIndex;
  kj::String key;
};

struct LuaKvListRequest {
  size_t bindingIndex;
  LuaKvListOptions options;
};

struct LuaD1QueryRequest {
  size_t bindingIndex;
  bool execute;
  kj::String body;
};

struct LuaAiRunRequest {
  size_t bindingIndex;
  kj::String model;
  kj::String body;
};

void requireValidUtf8(kj::StringPtr value, kj::StringPtr context) {
  KJ_REQUIRE(simdutf::validate_utf8(value.begin(), value.size()), context, "must be valid UTF-8");
}

void appendJsonString(kj::Vector<char>& output, kj::StringPtr value) {
  static constexpr char HEX[] = "0123456789abcdef";
  output.add('"');
  for (unsigned char byte: value) {
    if (byte == '"' || byte == '\\') {
      output.add('\\');
      output.add(static_cast<char>(byte));
    } else if (byte == '\b' || byte == '\f' || byte == '\n' || byte == '\r' || byte == '\t') {
      output.add('\\');
      switch (byte) {
        case '\b':
          output.add('b');
          break;
        case '\f':
          output.add('f');
          break;
        case '\n':
          output.add('n');
          break;
        case '\r':
          output.add('r');
          break;
        case '\t':
          output.add('t');
          break;
      }
    } else if (byte < 0x20) {
      output.add('\\');
      output.add('u');
      output.add('0');
      output.add('0');
      output.add(HEX[byte >> 4]);
      output.add(HEX[byte & 0x0f]);
    } else {
      output.add(static_cast<char>(byte));
    }
  }
  output.add('"');
}

bool isWorkerdNullValue(lua_State* state, int index) {
  index = absoluteIndex(state, index);
  lua_getglobal(state, "workerd");
  getRawField(state, -1, "null");
  bool result = lua_rawequal(state, index, -1);
  lua_pop(state, 2);
  return result;
}

struct LuaJsonTableShape {
  bool isArray;
  size_t count;
};

LuaJsonTableShape readLuaJsonTableShape(lua_State* state, int index) {
  index = absoluteIndex(state, index);
  bool sawArrayKey = false;
  bool sawObjectKey = false;
  size_t arrayCount = 0;
  size_t maxArrayIndex = 0;
  size_t objectCount = 0;
  lua_pushnil(state);
  while (lua_next(state, index) != 0) {
    if (lua_type(state, -2) == LUA_TNUMBER) {
      int isNumber = 0;
      lua_Integer key = lua_tointegerx(state, -2, &isNumber);
      KJ_REQUIRE(isNumber && key > 0 && static_cast<lua_Number>(key) == lua_tonumber(state, -2),
          "Workers AI JSON arrays require positive integer indices");
      KJ_REQUIRE(static_cast<uint64_t>(key) <= LuaRuntime::MAX_AI_JSON_BYTES,
          "Workers AI JSON array index exceeds the encoding limit");
      sawArrayKey = true;
      ++arrayCount;
      maxArrayIndex = kj::max(maxArrayIndex, static_cast<size_t>(key));
    } else if (lua_type(state, -2) == LUA_TSTRING) {
      sawObjectKey = true;
      ++objectCount;
    } else {
      lua_pop(state, 2);
      KJ_FAIL_REQUIRE("Workers AI JSON objects require string keys");
    }
    KJ_REQUIRE(!(sawArrayKey && sawObjectKey),
        "Workers AI JSON tables cannot mix array indices and object keys");
    lua_pop(state, 1);
  }
  if (sawArrayKey) {
    KJ_REQUIRE(arrayCount == maxArrayIndex,
        "Workers AI JSON arrays must use contiguous indices starting at 1");
    return {.isArray = true, .count = arrayCount};
  }
  return {.isArray = false, .count = objectCount};
}

void appendLuaAiJsonValue(lua_State* state,
    kj::Vector<char>& output,
    int index,
    size_t depth,
    kj::Vector<const void*>& parentTables);

void appendLuaAiJsonTable(lua_State* state,
    kj::Vector<char>& output,
    int index,
    size_t depth,
    kj::Vector<const void*>& parentTables,
    bool requireObject) {
  index = absoluteIndex(state, index);
  KJ_REQUIRE(depth < LuaRuntime::MAX_AI_JSON_DEPTH,
      "Workers AI JSON input exceeds maximum nesting depth", LuaRuntime::MAX_AI_JSON_DEPTH);
  const void* identity = lua_topointer(state, index);
  for (const void* parent: parentTables) {
    KJ_REQUIRE(parent != identity, "Workers AI JSON input cannot contain cycles");
  }
  parentTables.add(identity);
  KJ_DEFER(parentTables.removeLast());

  auto shape = readLuaJsonTableShape(state, index);
  KJ_REQUIRE(
      !requireObject || !shape.isArray, "Workers AI inputs and options must be JSON objects");
  if (shape.isArray) {
    output.add('[');
    for (size_t i = 1; i <= shape.count; ++i) {
      if (i > 1) output.add(',');
      lua_rawgeti(state, index, static_cast<int>(i));
      appendLuaAiJsonValue(state, output, -1, depth + 1, parentTables);
      lua_pop(state, 1);
    }
    output.add(']');
    KJ_REQUIRE(output.size() <= LuaRuntime::MAX_AI_JSON_BYTES,
        "Workers AI request JSON exceeds limit", LuaRuntime::MAX_AI_JSON_BYTES);
    return;
  }

  output.add('{');
  size_t emitted = 0;
  lua_pushnil(state);
  while (lua_next(state, index) != 0) {
    if (emitted++ > 0) output.add(',');
    size_t keyLength = 0;
    const char* key = lua_tolstring(state, -2, &keyLength);
    kj::StringPtr keyString(key, keyLength);
    requireValidUtf8(keyString, "JSON object key");
    appendJsonString(output, keyString);
    output.add(':');
    appendLuaAiJsonValue(state, output, -1, depth + 1, parentTables);
    lua_pop(state, 1);
    KJ_REQUIRE(output.size() <= LuaRuntime::MAX_AI_JSON_BYTES,
        "Workers AI request JSON exceeds limit", LuaRuntime::MAX_AI_JSON_BYTES);
  }
  output.add('}');
  KJ_REQUIRE(output.size() <= LuaRuntime::MAX_AI_JSON_BYTES,
      "Workers AI request JSON exceeds limit", LuaRuntime::MAX_AI_JSON_BYTES);
}

void appendLuaAiJsonValue(lua_State* state,
    kj::Vector<char>& output,
    int index,
    size_t depth,
    kj::Vector<const void*>& parentTables) {
  index = absoluteIndex(state, index);
  switch (lua_type(state, index)) {
    case LUA_TNIL:
      output.addAll("null"_kj.asArray());
      break;
    case LUA_TBOOLEAN:
      output.addAll(lua_toboolean(state, index) ? "true"_kj.asArray() : "false"_kj.asArray());
      break;
    case LUA_TNUMBER: {
      double value = lua_tonumber(state, index);
      KJ_REQUIRE(std::isfinite(value), "Workers AI JSON numbers must be finite");
      auto number = kj::str(value);
      output.addAll(number.asArray());
      break;
    }
    case LUA_TSTRING: {
      size_t length = 0;
      const char* value = lua_tolstring(state, index, &length);
      kj::StringPtr stringValue(value, length);
      requireValidUtf8(stringValue, "JSON string");
      KJ_REQUIRE(length <= LuaRuntime::MAX_AI_JSON_BYTES, "Workers AI JSON string exceeds limit",
          LuaRuntime::MAX_AI_JSON_BYTES);
      appendJsonString(output, stringValue);
      break;
    }
    case LUA_TTABLE:
      if (isWorkerdNullValue(state, index)) {
        output.addAll("null"_kj.asArray());
      } else {
        appendLuaAiJsonTable(state, output, index, depth, parentTables, false);
      }
      break;
    default:
      KJ_FAIL_REQUIRE(
          "Workers AI JSON values support only tables, strings, numbers, booleans, and null");
  }
  KJ_REQUIRE(output.size() <= LuaRuntime::MAX_AI_JSON_BYTES,
      "Workers AI request JSON exceeds limit", LuaRuntime::MAX_AI_JSON_BYTES);
}

kj::String serializeAiRunRequestBody(lua_State* state, int inputsIndex, int optionsIndex) {
  inputsIndex = absoluteIndex(state, inputsIndex);
  optionsIndex = absoluteIndex(state, optionsIndex);
  KJ_REQUIRE(lua_istable(state, inputsIndex) && !isWorkerdNullValue(state, inputsIndex),
      "Workers AI inputs must be an object table");
  KJ_REQUIRE(lua_isnil(state, optionsIndex) ||
          (lua_istable(state, optionsIndex) && !isWorkerdNullValue(state, optionsIndex)),
      "Workers AI options must be an object table");

  kj::Vector<char> output;
  kj::Vector<const void*> parentTables;
  output.addAll("{\"inputs\":"_kj.asArray());
  appendLuaAiJsonTable(state, output, inputsIndex, 0, parentTables, true);
  output.addAll(",\"options\":"_kj.asArray());
  if (lua_isnil(state, optionsIndex)) {
    output.addAll("{}"_kj.asArray());
  } else {
    appendLuaAiJsonTable(state, output, optionsIndex, 0, parentTables, true);
  }
  output.add('}');
  KJ_REQUIRE(output.size() <= LuaRuntime::MAX_AI_JSON_BYTES,
      "Workers AI request JSON exceeds limit", LuaRuntime::MAX_AI_JSON_BYTES);
  return kj::heapString(output.begin(), output.size());
}

LuaAiRunRequest readAiRunRequest(lua_State* state, int descriptorIndex) {
  descriptorIndex = absoluteIndex(state, descriptorIndex);
  size_t bindingIndex = 0;
  KJ_IF_SOME(index, readOptionalServiceBindingIndex(state, descriptorIndex)) {
    bindingIndex = index;
  } else {
    KJ_FAIL_REQUIRE("Workers AI request requires a configured service binding");
  }
  auto model = readStringField(state, descriptorIndex, "model");
  KJ_REQUIRE(model.size() > 0 && model.size() <= LuaRuntime::MAX_AI_MODEL_ID_BYTES,
      "Workers AI model name must contain 1 through", LuaRuntime::MAX_AI_MODEL_ID_BYTES, "bytes");
  requireValidUtf8(model, "model name");
  getRawField(state, descriptorIndex, "inputs");
  KJ_REQUIRE(lua_istable(state, -1), "Workers AI inputs must be an object table");
  int inputsIndex = absoluteIndex(state, -1);
  getRawField(state, descriptorIndex, "options");
  int optionsIndex = absoluteIndex(state, -1);
  auto body = serializeAiRunRequestBody(state, inputsIndex, optionsIndex);
  lua_pop(state, 2);
  return LuaAiRunRequest{
    .bindingIndex = bindingIndex,
    .model = kj::mv(model),
    .body = kj::mv(body),
  };
}

kj::String serializeD1RequestBody(lua_State* state, kj::StringPtr sql, int paramsIndex) {
  paramsIndex = absoluteIndex(state, paramsIndex);
  getRawField(state, paramsIndex, "n");
  int isNumber = 0;
  lua_Integer count = lua_tointegerx(state, -1, &isNumber);
  bool validCount = lua_type(state, -1) == LUA_TNUMBER && isNumber && count >= 0 &&
      static_cast<uint64_t>(count) <= LuaRuntime::MAX_D1_BINDING_VALUES &&
      static_cast<lua_Number>(count) == lua_tonumber(state, -1);
  lua_pop(state, 1);
  KJ_REQUIRE(validCount, "D1 binding accepts at most", LuaRuntime::MAX_D1_BINDING_VALUES, "values");
  KJ_REQUIRE(sql.size() <= LuaRuntime::MAX_D1_SQL_BYTES, "D1 SQL exceeds limit",
      LuaRuntime::MAX_D1_SQL_BYTES);
  requireValidUtf8(sql, "SQL text");

  kj::Vector<char> output;
  output.add('{');
  appendJsonString(output, "sql"_kj);
  output.add(':');
  appendJsonString(output, sql);
  output.add(',');
  appendJsonString(output, "params"_kj);
  output.add(':');
  output.add('[');
  for (lua_Integer i = 1; i <= count; ++i) {
    if (i > 1) output.add(',');
    lua_rawgeti(state, paramsIndex, static_cast<int>(i));
    switch (lua_type(state, -1)) {
      case LUA_TNIL:
        output.addAll("null"_kj.asArray());
        break;
      case LUA_TSTRING: {
        size_t length = 0;
        const char* value = lua_tolstring(state, -1, &length);
        auto stringValue = kj::StringPtr(value, length);
        requireValidUtf8(stringValue, "string binding value");
        appendJsonString(output, stringValue);
        break;
      }
      case LUA_TNUMBER: {
        double value = lua_tonumber(state, -1);
        KJ_REQUIRE(std::isfinite(value), "D1 binding values must be finite numbers");
        auto number = kj::str(value);
        output.addAll(number.asArray());
        break;
      }
      case LUA_TBOOLEAN:
        output.addAll(lua_toboolean(state, -1) ? "true"_kj.asArray() : "false"_kj.asArray());
        break;
      case LUA_TTABLE:
        if (isWorkerdNullValue(state, -1)) {
          output.addAll("null"_kj.asArray());
        } else {
          lua_pop(state, 1);
          KJ_FAIL_REQUIRE(
              "D1 binding values support only strings, finite numbers, booleans, and null");
        }
        break;
      default:
        lua_pop(state, 1);
        KJ_FAIL_REQUIRE(
            "D1 binding values support only strings, finite numbers, booleans, and null");
    }
    lua_pop(state, 1);
  }
  output.add(']');
  output.add('}');
  KJ_REQUIRE(output.size() <= LuaRuntime::MAX_HTTP_BODY_BYTES, "D1 query request exceeds limit",
      LuaRuntime::MAX_HTTP_BODY_BYTES);
  return kj::heapString(output.begin(), output.size());
}

LuaD1QueryRequest readD1QueryRequest(lua_State* state, int descriptorIndex) {
  descriptorIndex = absoluteIndex(state, descriptorIndex);
  size_t bindingIndex = 0;
  KJ_IF_SOME(index, readOptionalServiceBindingIndex(state, descriptorIndex)) {
    bindingIndex = index;
  } else {
    KJ_FAIL_REQUIRE("D1 query requires a configured D1 service binding");
  }
  auto sql = readStringField(state, descriptorIndex, "sql");
  getRawField(state, descriptorIndex, "mode");
  bool execute = false;
  if (lua_type(state, -1) == LUA_TSTRING) {
    size_t length = 0;
    const char* value = lua_tolstring(state, -1, &length);
    kj::StringPtr mode(value, length);
    KJ_REQUIRE(mode == "all" || mode == "run", "unsupported D1 query mode", mode);
    execute = mode == "run";
  } else {
    lua_pop(state, 1);
    KJ_FAIL_REQUIRE("D1 query mode must be 'all' or 'run'");
  }
  lua_pop(state, 1);
  getRawField(state, descriptorIndex, "params");
  KJ_REQUIRE(lua_istable(state, -1), "D1 query parameters must be a table");
  auto body = serializeD1RequestBody(state, sql, -1);
  lua_pop(state, 1);
  return LuaD1QueryRequest{
    .bindingIndex = bindingIndex,
    .execute = execute,
    .body = kj::mv(body),
  };
}

size_t readKvBindingIndex(lua_State* state, int descriptorIndex) {
  descriptorIndex = absoluteIndex(state, descriptorIndex);
  getRawField(state, descriptorIndex, "kvBindingIndex");
  int isNumber = 0;
  lua_Integer index = lua_tointegerx(state, -1, &isNumber);
  bool validIndex = lua_type(state, -1) == LUA_TNUMBER && isNumber && index >= 1 &&
      index <= INT_MAX && static_cast<lua_Number>(index) == lua_tonumber(state, -1);
  lua_pop(state, 1);
  KJ_REQUIRE(validIndex, "KV binding index must be a positive integer");
  return static_cast<size_t>(index - 1);
}

kj::String readKvKey(lua_State* state, int descriptorIndex, const char* operation) {
  descriptorIndex = absoluteIndex(state, descriptorIndex);
  getRawField(state, descriptorIndex, "key");
  if (lua_type(state, -1) != LUA_TSTRING) {
    lua_pop(state, 1);
    KJ_FAIL_REQUIRE("workerd.bindings KV", operation, "key must be a string");
  }
  size_t length = 0;
  const char* key = lua_tolstring(state, -1, &length);
  KJ_DEFER(lua_pop(state, 1));
  KJ_REQUIRE(length > 0, "KV key cannot be empty");
  KJ_REQUIRE(
      length <= LuaRuntime::MAX_KV_KEY_BYTES, "KV key exceeds limit", LuaRuntime::MAX_KV_KEY_BYTES);
  KJ_REQUIRE(length != 1 || key[0] != '.', "KV key cannot be .");
  KJ_REQUIRE(length != 2 || key[0] != '.' || key[1] != '.', "KV key cannot be ..");
  return kj::heapString(key, length);
}

void validateKvOptionFields(lua_State* state,
    int optionsIndex,
    const char* operation,
    kj::ArrayPtr<const char*> supportedFields) {
  optionsIndex = absoluteIndex(state, optionsIndex);
  lua_pushnil(state);
  while (lua_next(state, optionsIndex) != 0) {
    KJ_REQUIRE(lua_type(state, -2) == LUA_TSTRING, "workerd.bindings KV", operation,
        " options must use string field names");
    size_t keySize = 0;
    const char* key = lua_tolstring(state, -2, &keySize);
    kj::StringPtr keyName(key, keySize);
    bool supported = false;
    for (const char* field: supportedFields) {
      supported = supported || keyName == field;
    }
    KJ_REQUIRE(supported, "unsupported workerd.bindings KV", operation, " option", keyName);
    lua_pop(state, 1);
  }
}

LuaKvGetOptions readKvGetOptions(lua_State* state, int descriptorIndex, const char* operation) {
  descriptorIndex = absoluteIndex(state, descriptorIndex);
  LuaStackGuard stackGuard(state);
  getRawField(state, descriptorIndex, "options");
  if (lua_type(state, -1) == LUA_TNIL) return {};
  KJ_REQUIRE(lua_istable(state, -1), "KV", operation, " options must be a table");
  int optionsIndex = absoluteIndex(state, -1);
  const char* supportedFields[] = {"cacheTtl"};
  validateKvOptionFields(state, optionsIndex, operation, supportedFields);

  LuaKvGetOptions options;
  getRawField(state, optionsIndex, "cacheTtl");
  if (lua_type(state, -1) != LUA_TNIL) {
    int isNumber = 0;
    lua_Integer cacheTtl = lua_tointegerx(state, -1, &isNumber);
    bool validCacheTtl = lua_type(state, -1) == LUA_TNUMBER && isNumber && cacheTtl >= 60 &&
        cacheTtl <= INT_MAX && static_cast<lua_Number>(cacheTtl) == lua_tonumber(state, -1);
    KJ_REQUIRE(
        validCacheTtl, "KV", operation, " cacheTtl must be an integer of at least 60 seconds");
    options.cacheTtl = static_cast<uint32_t>(cacheTtl);
  }
  lua_pop(state, 1);
  return options;
}

LuaKvPutOptions readKvPutOptions(lua_State* state, int descriptorIndex) {
  descriptorIndex = absoluteIndex(state, descriptorIndex);
  LuaStackGuard stackGuard(state);
  getRawField(state, descriptorIndex, "options");
  if (lua_type(state, -1) == LUA_TNIL) return {};
  KJ_REQUIRE(lua_istable(state, -1), "KV put() options must be a table");
  int optionsIndex = absoluteIndex(state, -1);
  const char* supportedFields[] = {"expiration", "expirationTtl"};
  validateKvOptionFields(state, optionsIndex, "put", supportedFields);

  LuaKvPutOptions options;
  getRawField(state, optionsIndex, "expiration");
  if (lua_type(state, -1) != LUA_TNIL) {
    int isNumber = 0;
    lua_Integer expiration = lua_tointegerx(state, -1, &isNumber);
    bool validExpiration = lua_type(state, -1) == LUA_TNUMBER && isNumber && expiration >= 0 &&
        expiration <= INT_MAX && static_cast<lua_Number>(expiration) == lua_tonumber(state, -1);
    KJ_REQUIRE(validExpiration, "KV put() expiration must be an integer from 0 through", INT_MAX);
    options.expiration = static_cast<uint32_t>(expiration);
  }
  lua_pop(state, 1);
  getRawField(state, optionsIndex, "expirationTtl");
  if (lua_type(state, -1) != LUA_TNIL) {
    int isNumber = 0;
    lua_Integer expirationTtl = lua_tointegerx(state, -1, &isNumber);
    bool validExpirationTtl = lua_type(state, -1) == LUA_TNUMBER && isNumber &&
        expirationTtl >= 60 && expirationTtl <= INT_MAX &&
        static_cast<lua_Number>(expirationTtl) == lua_tonumber(state, -1);
    KJ_REQUIRE(
        validExpirationTtl, "KV put() expirationTtl must be an integer from 60 through", INT_MAX);
    options.expirationTtl = static_cast<uint32_t>(expirationTtl);
  }
  lua_pop(state, 1);
  return options;
}

kj::Maybe<kj::String> readOptionalKvListString(
    lua_State* state, int optionsIndex, const char* fieldName, size_t maxLength) {
  getRawField(state, optionsIndex, fieldName);
  if (lua_type(state, -1) == LUA_TNIL) {
    lua_pop(state, 1);
    return kj::none;
  }
  KJ_DEFER(lua_pop(state, 1));
  KJ_REQUIRE(lua_type(state, -1) == LUA_TSTRING, "KV list() option must be a string", fieldName);
  size_t length = 0;
  const char* value = lua_tolstring(state, -1, &length);
  KJ_REQUIRE(length <= maxLength, "KV list() option exceeds its byte limit", fieldName, maxLength);
  return kj::heapString(value, length);
}

LuaKvListOptions readKvListOptions(lua_State* state, int descriptorIndex) {
  descriptorIndex = absoluteIndex(state, descriptorIndex);
  LuaStackGuard stackGuard(state);
  getRawField(state, descriptorIndex, "options");
  if (lua_type(state, -1) == LUA_TNIL) return {};
  KJ_REQUIRE(lua_istable(state, -1), "KV list() options must be a table");
  int optionsIndex = absoluteIndex(state, -1);
  const char* supportedFields[] = {"limit", "prefix", "cursor"};
  validateKvOptionFields(state, optionsIndex, "list", supportedFields);

  LuaKvListOptions options;
  getRawField(state, optionsIndex, "limit");
  if (lua_type(state, -1) != LUA_TNIL) {
    int isNumber = 0;
    lua_Integer limit = lua_tointegerx(state, -1, &isNumber);
    bool validLimit = lua_type(state, -1) == LUA_TNUMBER && isNumber && limit >= 0 &&
        limit <= 1000 && static_cast<lua_Number>(limit) == lua_tonumber(state, -1);
    KJ_REQUIRE(validLimit, "KV list() limit must be an integer from 0 through 1000");
    options.limit = static_cast<uint32_t>(limit);
  }
  lua_pop(state, 1);
  options.prefix =
      readOptionalKvListString(state, optionsIndex, "prefix", LuaRuntime::MAX_KV_KEY_BYTES);
  options.cursor =
      readOptionalKvListString(state, optionsIndex, "cursor", LuaRuntime::MAX_KV_CURSOR_BYTES);
  return options;
}

kj::Array<kj::byte> readKvValue(lua_State* state, int descriptorIndex) {
  descriptorIndex = absoluteIndex(state, descriptorIndex);
  getRawField(state, descriptorIndex, "value");
  if (lua_type(state, -1) != LUA_TSTRING) {
    lua_pop(state, 1);
    KJ_FAIL_REQUIRE("workerd.bindings KV put() value must be a binary string");
  }
  size_t length = 0;
  const char* value = lua_tolstring(state, -1, &length);
  KJ_DEFER(lua_pop(state, 1));
  KJ_REQUIRE(length <= LuaRuntime::MAX_KV_VALUE_BYTES, "KV value exceeds limit",
      LuaRuntime::MAX_KV_VALUE_BYTES);
  auto bytes = kj::heapArray<kj::byte>(length);
  if (length > 0) std::memcpy(bytes.begin(), value, length);
  return bytes;
}

kj::Promise<kj::Array<kj::byte>> readLuaKvResponseBody(kj::Own<kj::AsyncInputStream> body) {
  KJ_IF_SOME(bodySize, body->tryGetLength()) {
    KJ_REQUIRE(bodySize <= LuaRuntime::MAX_KV_RESPONSE_BYTES, "Lua KV response exceeds limit",
        LuaRuntime::MAX_KV_RESPONSE_BYTES);
    auto value = kj::heapArray<kj::byte>(static_cast<size_t>(bodySize));
    size_t bytesRead = 0;
    while (bytesRead < value.size()) {
      size_t count = co_await body->tryRead(value.begin() + bytesRead, 1, value.size() - bytesRead);
      KJ_REQUIRE(count > 0, "Lua KV response body ended before its declared length");
      bytesRead += count;
    }
    co_return kj::mv(value);
  }

  kj::Vector<kj::byte> value;
  while (value.size() <= LuaRuntime::MAX_KV_RESPONSE_BYTES) {
    size_t maxBytes =
        kj::min<size_t>(16 * 1024, LuaRuntime::MAX_KV_RESPONSE_BYTES + 1 - value.size());
    auto chunk = kj::heapArray<kj::byte>(maxBytes);
    size_t bytesRead = co_await body->tryRead(chunk.begin(), 1, maxBytes);
    if (bytesRead == 0) break;
    KJ_REQUIRE(bytesRead <= LuaRuntime::MAX_KV_RESPONSE_BYTES - value.size(),
        "Lua KV response exceeds limit", LuaRuntime::MAX_KV_RESPONSE_BYTES);
    value.addAll(chunk.asPtr().first(bytesRead));
  }
  co_return value.releaseAsArray();
}

bool asciiCaseInsensitiveEquals(kj::StringPtr left, kj::StringPtr right) {
  if (left.size() != right.size()) return false;
  for (size_t i = 0; i < left.size(); ++i) {
    char a = left[i];
    char b = right[i];
    if (a >= 'A' && a <= 'Z') a += 'a' - 'A';
    if (b >= 'A' && b <= 'Z') b += 'a' - 'A';
    if (a != b) return false;
  }
  return true;
}

kj::Promise<kj::Array<kj::byte>> readLuaBoundedResponseBody(
    kj::Own<kj::AsyncInputStream> body, size_t limit, kj::StringPtr context) {
  kj::Vector<kj::byte> value;
  while (value.size() <= limit) {
    size_t maxBytes = kj::min<size_t>(16 * 1024, limit + 1 - value.size());
    auto chunk = kj::heapArray<kj::byte>(maxBytes);
    size_t bytesRead = co_await body->tryRead(chunk.begin(), 1, maxBytes);
    if (bytesRead == 0) break;
    KJ_REQUIRE(bytesRead <= limit - value.size(), context, " exceeds limit", limit);
    value.addAll(chunk.asPtr().first(bytesRead));
  }
  co_return value.releaseAsArray();
}

LuaBodyReadRequest readBodyReadRequest(lua_State* state) {
  int descriptorIndex = absoluteIndex(state, -1);
  auto requestId = readBodyRequestId(state, descriptorIndex);
  getRawField(state, descriptorIndex, "streamId");
  int isNumber = 0;
  lua_Integer streamId = lua_tointegerx(state, -1, &isNumber);
  bool validStreamId = lua_type(state, -1) == LUA_TNUMBER && isNumber && streamId >= 1 &&
      static_cast<lua_Number>(streamId) == lua_tonumber(state, -1);
  lua_pop(state, 1);
  KJ_REQUIRE(validStreamId, "workerd.readBody() requires a valid response body stream");

  getRawField(state, -1, "maxBytes");
  lua_Integer maxBytes = lua_tointegerx(state, -1, &isNumber);
  bool validMaxBytes = lua_type(state, -1) == LUA_TNUMBER && isNumber && maxBytes >= 1 &&
      maxBytes <= LuaRuntime::MAX_HTTP_READ_CHUNK_BYTES &&
      static_cast<lua_Number>(maxBytes) == lua_tonumber(state, -1);
  lua_pop(state, 1);
  KJ_REQUIRE(validMaxBytes, "workerd.readBody() chunk size must be an integer from 1 through",
      LuaRuntime::MAX_HTTP_READ_CHUNK_BYTES);
  return LuaBodyReadRequest{
    .requestId = kj::mv(requestId),
    .streamId = static_cast<size_t>(streamId),
    .maxBytes = static_cast<size_t>(maxBytes),
  };
}

using LuaHostOperation = kj::OneOf<kj::Duration,
    LuaHttpRequest,
    LuaBodyReadRequest,
    LuaBodyCancelRequest,
    LuaRequestBodyReadRequest,
    LuaRandomBytesRequest,
    LuaRandomUuidRequest,
    LuaProducerChunkYield,
    LuaKvGetRequest,
    LuaKvPutRequest,
    LuaKvDeleteRequest,
    LuaKvListRequest,
    LuaD1QueryRequest,
    LuaAiRunRequest>;

LuaHostOperation readHostOperation(
    lua_State* state, kj::Rc<LuaRequestContext> requestContext, int bodyProducerWrapperRef) {
  KJ_REQUIRE(lua_gettop(state) == 1, "LuaJIT host operation must yield one descriptor");
  KJ_REQUIRE(lua_istable(state, -1), "LuaJIT host operation descriptor must be a table");
  getRawField(state, -1, "operation");
  size_t operationLength = 0;
  const char* operation = lua_tolstring(state, -1, &operationLength);
  kj::StringPtr name =
      operation == nullptr ? kj::StringPtr() : kj::StringPtr(operation, operationLength);
  lua_pop(state, 1);

  if (name == "sleep") return readSleepDelay(state);
  if (name == "randomUUID") return LuaRandomUuidRequest{};
  if (name == "bodyProducerChunk") return LuaProducerChunkYield{};
  if (name == "randomBytes") {
    int descriptorIndex = absoluteIndex(state, -1);
    getRawField(state, descriptorIndex, "size");
    int isNumber = 0;
    lua_Integer size = lua_tointegerx(state, -1, &isNumber);
    bool validSize = lua_type(state, -1) == LUA_TNUMBER && isNumber && size >= 0 &&
        size <= LuaRuntime::MAX_RANDOM_BYTES &&
        static_cast<lua_Number>(size) == lua_tonumber(state, -1);
    lua_pop(state, 1);
    KJ_REQUIRE(validSize, "workerd.randomBytes() requires an integer size from 0 through",
        LuaRuntime::MAX_RANDOM_BYTES);
    return LuaRandomBytesRequest{.size = static_cast<size_t>(size)};
  }
  if (name == "fetch") {
    int descriptorIndex = absoluteIndex(state, -1);
    auto serviceBindingIndex = readOptionalServiceBindingIndex(state, descriptorIndex);
    getRawField(state, descriptorIndex, "request");
    auto request = readFetchRequest(
        state, -1, kj::mv(serviceBindingIndex), kj::mv(requestContext), bodyProducerWrapperRef);
    lua_pop(state, 1);
    return kj::mv(request);
  }
  if (name == "kvGet" || name == "kvGetWithMetadata" || name == "kvPut" || name == "kvDelete") {
    int descriptorIndex = absoluteIndex(state, -1);
    auto bindingIndex = readKvBindingIndex(state, descriptorIndex);
    auto key = readKvKey(state, descriptorIndex, name.begin());
    if (name == "kvGet" || name == "kvGetWithMetadata") {
      return LuaKvGetRequest{
        .bindingIndex = bindingIndex,
        .key = kj::mv(key),
        .options =
            readKvGetOptions(state, descriptorIndex, name == "kvGet" ? "get" : "getWithMetadata"),
        .mode = name == "kvGet" ? LuaKvGetMode::VALUE : LuaKvGetMode::WITH_METADATA,
      };
    }
    if (name == "kvPut") {
      auto options = readKvPutOptions(state, descriptorIndex);
      auto value = readKvValue(state, descriptorIndex);
      return LuaKvPutRequest{
        .bindingIndex = bindingIndex,
        .key = kj::mv(key),
        .value = kj::mv(value),
        .options = kj::mv(options),
      };
    }
    return LuaKvDeleteRequest{.bindingIndex = bindingIndex, .key = kj::mv(key)};
  }
  if (name == "kvList") {
    int descriptorIndex = absoluteIndex(state, -1);
    return LuaKvListRequest{
      .bindingIndex = readKvBindingIndex(state, descriptorIndex),
      .options = readKvListOptions(state, descriptorIndex),
    };
  }
  if (name == "d1Query") {
    return readD1QueryRequest(state, absoluteIndex(state, -1));
  }
  if (name == "aiRun") {
    return readAiRunRequest(state, absoluteIndex(state, -1));
  }
  if (name == "readBody") return readBodyReadRequest(state);
  if (name == "readRequestBody") {
    int descriptorIndex = absoluteIndex(state, -1);
    auto requestId = readBodyRequestId(state, descriptorIndex);
    getRawField(state, descriptorIndex, "maxBytes");
    int isNumber = 0;
    lua_Integer maxBytes = lua_tointegerx(state, -1, &isNumber);
    bool validMaxBytes = lua_type(state, -1) == LUA_TNUMBER && isNumber && maxBytes >= 1 &&
        maxBytes <= LuaRuntime::MAX_HTTP_READ_CHUNK_BYTES &&
        static_cast<lua_Number>(maxBytes) == lua_tonumber(state, -1);
    lua_pop(state, 1);
    KJ_REQUIRE(validMaxBytes,
        "workerd.readRequestBody() chunk size must be an integer from 1 through",
        LuaRuntime::MAX_HTTP_READ_CHUNK_BYTES);
    return LuaRequestBodyReadRequest{
      .requestId = kj::mv(requestId),
      .maxBytes = static_cast<size_t>(maxBytes),
    };
  }
  if (name == "cancelBody") {
    int descriptorIndex = absoluteIndex(state, -1);
    auto requestId = readBodyRequestId(state, descriptorIndex);
    getRawField(state, descriptorIndex, "streamId");
    int isNumber = 0;
    lua_Integer streamId = lua_tointegerx(state, -1, &isNumber);
    bool validStreamId = lua_type(state, -1) == LUA_TNUMBER && isNumber && streamId >= 1 &&
        static_cast<lua_Number>(streamId) == lua_tonumber(state, -1);
    lua_pop(state, 1);
    KJ_REQUIRE(validStreamId, "workerd.cancelBody() requires a valid response body stream");
    return LuaBodyCancelRequest{
      .requestId = kj::mv(requestId),
      .streamId = static_cast<size_t>(streamId),
    };
  }
  KJ_FAIL_REQUIRE("unsupported LuaJIT host operation", name);
}

void pushHttpResponse(lua_State* state,
    LuaHttpResponse&& response,
    kj::Vector<kj::Own<kj::AsyncInputStream>>& responseBodies,
    kj::StringPtr requestId) {
  lua_createtable(state, 0, 3);
  lua_pushinteger(state, response.status);
  lua_setfield(state, -2, "status");

  lua_createtable(state, static_cast<int>(response.headers.size()), 0);
  int index = 1;
  for (auto& header: response.headers) {
    lua_createtable(state, 0, 2);
    setStringField(state, "name", header.name);
    setStringField(state, "value", header.value);
    lua_rawseti(state, -2, index++);
  }
  lua_setfield(state, -2, "headers");
  responseBodies.add(kj::mv(response.body));
  lua_createtable(state, 0, 2);
  setStringField(state, "requestId", requestId);
  lua_pushinteger(state, static_cast<lua_Integer>(responseBodies.size()));
  lua_setfield(state, -2, "streamId");
  lua_setfield(state, -2, "body");
}

class LuaRequestContext final: public kj::Refcounted {
 public:
  LuaRequestContext(kj::Timer& timer,
      kj::Maybe<kj::Ptr<LuaHostOperations>> hostOperations,
      LuaRequestBodyMode requestBodyMode,
      kj::String requestId,
      int bodyProducerWrapperRef)
      : timer(timer),
        hostOperations(kj::mv(hostOperations)),
        requestBodyMode(requestBodyMode),
        requestId(kj::mv(requestId)),
        bodyProducerWrapperRef(bodyProducerWrapperRef) {}

  kj::Promise<void> execute(lua_State* coroutine, LuaHostOperation operation);

  kj::Timer& timer;
  kj::Maybe<kj::Ptr<LuaHostOperations>> hostOperations;
  LuaRequestBodyMode requestBodyMode;
  kj::String requestId;
  int bodyProducerWrapperRef;
  kj::Vector<kj::Own<kj::AsyncInputStream>> responseBodies;
};

kj::Promise<int> resumeLuaCoroutine(lua_State* coroutine,
    int status,
    kj::Rc<LuaRequestContext> requestContext,
    bool allowProducerChunkYield) {
  while (status == LUA_YIELD) {
    try {
      auto operation = readHostOperation(
          coroutine, requestContext.addRef(), requestContext->bodyProducerWrapperRef);
      if (operation.is<LuaProducerChunkYield>()) {
        KJ_REQUIRE(
            allowProducerChunkYield, "LuaJIT producer chunk yielded outside a body producer");
        co_return status;
      }
      lua_settop(coroutine, 0);
      lua_pushboolean(coroutine, 1);
      co_await requestContext->execute(coroutine, kj::mv(operation));
    } catch (const kj::Exception& exception) {
      lua_settop(coroutine, 0);
      auto description = exception.getDescription();
      lua_pushboolean(coroutine, 0);
      lua_pushlstring(coroutine, description.begin(), description.size());
    }
    status = lua_resume(coroutine, 2);
  }
  co_return status;
}

kj::Promise<void> LuaRequestContext::execute(lua_State* coroutine, LuaHostOperation operation) {
  KJ_SWITCH_ONEOF(operation) {
    KJ_CASE_ONEOF(delay, kj::Duration) {
      co_await timer.afterDelay(delay);
      lua_pushboolean(coroutine, 1);
    }
    KJ_CASE_ONEOF(producerChunk, LuaProducerChunkYield) {
      KJ_FAIL_ASSERT("LuaJIT body producer yield reached host-operation dispatcher");
    }
    KJ_CASE_ONEOF(randomUuid, LuaRandomUuidRequest) {
      KJ_IF_SOME(host, hostOperations) {
        auto uuid = host->generateRandomUuid();
        lua_pushlstring(coroutine, uuid.begin(), uuid.size());
      } else {
        KJ_FAIL_REQUIRE("workerd.randomUUID() requires host operations");
      }
    }
    KJ_CASE_ONEOF(randomBytes, LuaRandomBytesRequest) {
      auto bytes = kj::heapArray<kj::byte>(randomBytes.size);
      KJ_IF_SOME(host, hostOperations) {
        host->fillRandomBytes(bytes);
      } else {
        KJ_FAIL_REQUIRE("workerd.randomBytes() requires host operations");
      }
      if (bytes.size() == 0) {
        lua_pushlstring(coroutine, "", 0);
      } else {
        lua_pushlstring(coroutine, reinterpret_cast<const char*>(bytes.begin()), bytes.size());
      }
    }
    KJ_CASE_ONEOF(request, LuaHttpRequest) {
      KJ_IF_SOME(bodyStreamRequestId, request.bodyStreamRequestId) {
        KJ_REQUIRE(requestBodyMode == LuaRequestBodyMode::STREAMING,
            "workerd.fetch() request body streams require luaRequestBodyStreaming");
        KJ_REQUIRE(bodyStreamRequestId == requestId,
            "workerd.fetch() cannot use a body stream from another request");
      }
      KJ_IF_SOME(host, hostOperations) {
        auto response = co_await host->fetch(kj::mv(request));
        pushHttpResponse(coroutine, kj::mv(response), responseBodies, requestId);
      } else {
        KJ_FAIL_REQUIRE("workerd.fetch() is unavailable in this context");
      }
    }
    KJ_CASE_ONEOF(kvGet, LuaKvGetRequest) {
      KJ_IF_SOME(host, hostOperations) {
        auto response = co_await host->kvGet(
            kvGet.bindingIndex, kj::mv(kvGet.key), kj::mv(kvGet.options), kvGet.mode);
        kj::Maybe<kj::Array<kj::byte>> value;
        KJ_IF_SOME(body, response.body) {
          value = co_await readLuaKvResponseBody(kj::mv(body));
        }
        if (kvGet.mode == LuaKvGetMode::WITH_METADATA) {
          lua_createtable(coroutine, 0, 3);
          int resultIndex = absoluteIndex(coroutine, -1);
          lua_pushstring(coroutine, "value");
          KJ_IF_SOME(bytes, value) {
            const char* data =
                bytes.size() == 0 ? "" : reinterpret_cast<const char*>(bytes.begin());
            lua_pushlstring(coroutine, data, bytes.size());
          } else {
            pushWorkerdNull(coroutine);
          }
          lua_rawset(coroutine, resultIndex);
          lua_pushstring(coroutine, "metadata");
          KJ_IF_SOME(metadata, response.metadata) {
            pushJsonText(coroutine, metadata);
          } else {
            pushWorkerdNull(coroutine);
          }
          lua_rawset(coroutine, resultIndex);
          lua_pushstring(coroutine, "cacheStatus");
          KJ_IF_SOME(cacheStatus, response.cacheStatus) {
            lua_pushlstring(coroutine, cacheStatus.begin(), cacheStatus.size());
          } else {
            pushWorkerdNull(coroutine);
          }
          lua_rawset(coroutine, resultIndex);
        } else {
          KJ_IF_SOME(bytes, value) {
            const char* data =
                bytes.size() == 0 ? "" : reinterpret_cast<const char*>(bytes.begin());
            lua_pushlstring(coroutine, data, bytes.size());
          } else {
            lua_pushnil(coroutine);
          }
        }
      } else {
        KJ_FAIL_REQUIRE("workerd.bindings KV get() is unavailable in this context");
      }
    }
    KJ_CASE_ONEOF(kvPut, LuaKvPutRequest) {
      KJ_IF_SOME(host, hostOperations) {
        co_await host->kvPut(
            kvPut.bindingIndex, kj::mv(kvPut.key), kj::mv(kvPut.value), kj::mv(kvPut.options));
        lua_pushnil(coroutine);
      } else {
        KJ_FAIL_REQUIRE("workerd.bindings KV put() is unavailable in this context");
      }
    }
    KJ_CASE_ONEOF(kvDelete, LuaKvDeleteRequest) {
      KJ_IF_SOME(host, hostOperations) {
        co_await host->kvDelete(kvDelete.bindingIndex, kj::mv(kvDelete.key));
        lua_pushnil(coroutine);
      } else {
        KJ_FAIL_REQUIRE("workerd.bindings KV delete() is unavailable in this context");
      }
    }
    KJ_CASE_ONEOF(kvList, LuaKvListRequest) {
      KJ_IF_SOME(host, hostOperations) {
        auto response = co_await host->kvList(kvList.bindingIndex, kj::mv(kvList.options));
        auto cacheStatus = kj::mv(response.cacheStatus);
        auto value = co_await readLuaKvResponseBody(kj::mv(response.body));
        const char* bytes = value.size() == 0 ? "" : reinterpret_cast<const char*>(value.begin());
        pushLuaKvListResult(coroutine, kj::StringPtr(bytes, value.size()), kj::mv(cacheStatus));
      } else {
        KJ_FAIL_REQUIRE("workerd.bindings KV list() is unavailable in this context");
      }
    }
    KJ_CASE_ONEOF(d1Query, LuaD1QueryRequest) {
      KJ_IF_SOME(host, hostOperations) {
        auto requestBody = kj::heapArray<kj::byte>(d1Query.body.size());
        if (requestBody.size() > 0) {
          std::memcpy(requestBody.begin(), d1Query.body.begin(), requestBody.size());
        }
        kj::Vector<LuaHttpHeader> headers;
        headers.add(
            LuaHttpHeader{.name = kj::str("content-type"), .value = kj::str("application/json")});
        auto response = co_await host->fetch(LuaHttpRequest{
          .method = kj::str("POST"),
          .url = d1Query.execute ? kj::str("http://d1/execute?resultsFormat=NONE")
                                 : kj::str("http://d1/query?resultsFormat=ROWS_AND_COLUMNS"),
          .headers = kj::mv(headers),
          .body = kj::mv(requestBody),
          .serviceBindingIndex = kj::Maybe<size_t>(d1Query.bindingIndex),
        });
        auto responseBody = co_await readLuaBoundedResponseBody(
            kj::mv(response.body), LuaRuntime::MAX_D1_RESPONSE_BYTES, "D1 response");
        if (response.status < 200 || response.status >= 300) {
          KJ_FAIL_REQUIRE("D1 service returned HTTP status", response.status, ":",
              kj::StringPtr(
                  reinterpret_cast<const char*>(responseBody.begin()), responseBody.size()));
        }
        const char* json =
            responseBody.size() == 0 ? "" : reinterpret_cast<const char*>(responseBody.begin());
        pushJsonText(coroutine, kj::StringPtr(json, responseBody.size()));
      } else {
        KJ_FAIL_REQUIRE("D1 bindings are unavailable in this context");
      }
    }
    KJ_CASE_ONEOF(aiRun, LuaAiRunRequest) {
      KJ_IF_SOME(host, hostOperations) {
        auto requestBody = kj::heapArray<kj::byte>(aiRun.body.size());
        if (requestBody.size() > 0) {
          std::memcpy(requestBody.begin(), aiRun.body.begin(), requestBody.size());
        }
        kj::Vector<LuaHttpHeader> headers;
        headers.add(LuaHttpHeader{
          .name = kj::str("content-type"),
          .value = kj::str("application/json"),
        });
        headers.add(LuaHttpHeader{
          .name = kj::str("cf-consn-sdk-version"),
          .value = kj::str("2.0.0"),
        });
        headers.add(LuaHttpHeader{
          .name = kj::str("cf-consn-model-id"),
          .value = kj::mv(aiRun.model),
        });
        auto response = co_await host->fetch(LuaHttpRequest{
          .method = kj::str("POST"),
          .url = kj::str("https://workers-binding.ai/run?version=3"),
          .headers = kj::mv(headers),
          .body = kj::mv(requestBody),
          .serviceBindingIndex = kj::Maybe<size_t>(aiRun.bindingIndex),
        });
        bool isJson = false;
        if (response.status >= 200 && response.status < 300) {
          for (const auto& header: response.headers) {
            if (asciiCaseInsensitiveEquals(header.name, "content-type"_kj) &&
                header.value == "application/json") {
              isJson = true;
              break;
            }
          }
        }
        auto responseBody = co_await readLuaBoundedResponseBody(
            kj::mv(response.body), LuaRuntime::MAX_AI_RESPONSE_BYTES, "Workers AI response");
        const char* responseText =
            responseBody.size() == 0 ? "" : reinterpret_cast<const char*>(responseBody.begin());
        lua_createtable(coroutine, 0, 4);
        lua_pushinteger(coroutine, response.status);
        lua_setfield(coroutine, -2, "status");
        lua_pushboolean(coroutine, isJson);
        lua_setfield(coroutine, -2, "isJson");
        lua_pushlstring(coroutine, responseText, responseBody.size());
        lua_setfield(coroutine, -2, "body");
        if (isJson) {
          pushJsonText(coroutine, kj::StringPtr(responseText, responseBody.size()));
          lua_setfield(coroutine, -2, "value");
        }
      } else {
        KJ_FAIL_REQUIRE("Workers AI bindings are unavailable in this context");
      }
    }
    KJ_CASE_ONEOF(cancelRequest, LuaBodyCancelRequest) {
      KJ_REQUIRE(cancelRequest.requestId == requestId,
          "workerd.cancelBody() cannot use a stream from another request");
      KJ_REQUIRE(cancelRequest.streamId <= responseBodies.size(),
          "workerd.cancelBody() references an unknown response body stream",
          cancelRequest.streamId);
      responseBodies[cancelRequest.streamId - 1] = nullptr;
      lua_pushboolean(coroutine, 1);
    }
    KJ_CASE_ONEOF(readRequest, LuaBodyReadRequest) {
      KJ_REQUIRE(readRequest.requestId == requestId,
          "workerd.readBody() cannot use a stream from another request");
      KJ_REQUIRE(readRequest.streamId <= responseBodies.size(),
          "workerd.readBody() references an unknown response body stream", readRequest.streamId);
      auto& responseBody = responseBodies[readRequest.streamId - 1];
      if (responseBody.get() == nullptr) {
        lua_pushnil(coroutine);
      } else {
        auto chunk = kj::heapArray<kj::byte>(readRequest.maxBytes);
        auto bytesRead = co_await responseBody->tryRead(chunk.begin(), 1, chunk.size());
        if (bytesRead == 0) {
          responseBody = nullptr;
          lua_pushnil(coroutine);
        } else {
          lua_pushlstring(coroutine, reinterpret_cast<const char*>(chunk.begin()), bytesRead);
        }
      }
    }
    KJ_CASE_ONEOF(readRequestBody, LuaRequestBodyReadRequest) {
      KJ_REQUIRE(readRequestBody.requestId == requestId,
          "workerd.readRequestBody() cannot use a stream from another request");
      KJ_IF_SOME(host, hostOperations) {
        KJ_IF_SOME(chunk, co_await host->readRequestBody(readRequestBody.maxBytes)) {
          lua_pushlstring(coroutine, reinterpret_cast<const char*>(chunk.begin()), chunk.size());
        } else {
          lua_pushnil(coroutine);
        }
      } else {
        KJ_FAIL_REQUIRE("workerd.readRequestBody() is unavailable in this context");
      }
    }
  }
}

LuaFunctionRequestBodyProducer::LuaFunctionRequestBodyProducer(
    lua_State* state, int functionRef, kj::Rc<LuaRequestContext> requestContext)
    : state(state),
      producerCoroutine(lua_newthread(state)),
      functionRef(functionRef),
      coroutineRef(luaL_ref(state, LUA_REGISTRYINDEX)),
      requestContext(kj::mv(requestContext)) {}

LuaFunctionRequestBodyProducer::~LuaFunctionRequestBodyProducer() noexcept(false) {
  luaL_unref(state, LUA_REGISTRYINDEX, functionRef);
  luaL_unref(state, LUA_REGISTRYINDEX, coroutineRef);
}

kj::Promise<kj::Maybe<kj::Array<kj::byte>>> LuaFunctionRequestBodyProducer::nextChunk() {
  KJ_REQUIRE(!failed, "LuaJIT request body producer was cancelled or failed");
  KJ_REQUIRE(!pending, "LuaJIT request body producer cannot run concurrently");
  if (reachedEof) co_return kj::none;

  pending = true;
  bool completed = false;
  KJ_DEFER({
    pending = false;
    if (!completed) failed = true;
  });

  LuaStackGuard parentStackGuard(state);
  lua_settop(producerCoroutine, 0);
  LuaStackGuard coroutineStackGuard(producerCoroutine);
  if (!started) {
    lua_rawgeti(producerCoroutine, LUA_REGISTRYINDEX, functionRef);
    started = true;
  }

  auto status = lua_resume(producerCoroutine, 0);
  status = co_await resumeLuaCoroutine(producerCoroutine, status, requestContext.addRef(), true);
  KJ_REQUIRE(status == LUA_YIELD,
      "LuaJIT request body producer failed:", readLuaErrorMessage(producerCoroutine));
  KJ_REQUIRE(lua_gettop(producerCoroutine) == 1 && lua_istable(producerCoroutine, -1),
      "LuaJIT request body producer did not yield a chunk");

  getRawField(producerCoroutine, -1, "chunk");
  if (lua_isnil(producerCoroutine, -1)) {
    reachedEof = true;
    completed = true;
    co_return kj::none;
  }
  KJ_REQUIRE(lua_type(producerCoroutine, -1) == LUA_TSTRING,
      "LuaJIT request body producer must return a string or nil");

  size_t length = 0;
  const char* bytes = lua_tolstring(producerCoroutine, -1, &length);
  KJ_REQUIRE(length <= LuaRuntime::MAX_HTTP_READ_CHUNK_BYTES,
      "LuaJIT request body producer chunk exceeds limit", LuaRuntime::MAX_HTTP_READ_CHUNK_BYTES);
  KJ_REQUIRE(length <= LuaRuntime::MAX_HTTP_BODY_BYTES - bytesGenerated,
      "LuaJIT request body producer exceeds total upload limit", LuaRuntime::MAX_HTTP_BODY_BYTES);
  bytesGenerated += length;
  auto result = kj::heapArray<kj::byte>(length);
  if (length > 0) std::memcpy(result.begin(), bytes, length);
  completed = true;
  co_return kj::Maybe<kj::Array<kj::byte>>(kj::mv(result));
}

}  // namespace

LuaHmacKey::LuaHmacKey(
    kj::String hashAlgorithm, kj::Array<kj::byte> keyData, kj::Array<LuaHmacKeyUsage> usages)
    : hashAlgorithm(kj::mv(hashAlgorithm)),
      keyData(kj::mv(keyData)),
      usages(kj::mv(usages)) {
  KJ_REQUIRE(this->keyData.size() > 0 && this->keyData.size() <= LuaRuntime::MAX_HMAC_KEY_BYTES,
      "LuaJIT CryptoKey key data must be from 1 through 65536 bytes");
}

LuaHmacKey::~LuaHmacKey() noexcept {
  if (keyData.size() > 0) OPENSSL_cleanse(keyData.begin(), keyData.size());
}

kj::StringPtr LuaHmacKey::getHashAlgorithm() const {
  return hashAlgorithm;
}

kj::ArrayPtr<const kj::byte> LuaHmacKey::getKeyData() const {
  return keyData.asPtr();
}

kj::ArrayPtr<const LuaHmacKeyUsage> LuaHmacKey::getUsages() const {
  return usages.asPtr();
}

LuaRuntime::LuaRuntime(kj::StringPtr moduleSource, kj::Timer& timer, kj::Array<LuaBinding> bindings)
    : timer(timer),
      state(luaL_newstate()),
      fetchHandlerRef(LUA_NOREF),
      bindingsRef(LUA_NOREF),
      bodyProducerWrapperRef(LUA_NOREF) {
  KJ_REQUIRE(state != nullptr, "could not create LuaJIT state");
  bool keepState = false;
  KJ_DEFER(if (!keepState) lua_close(state));

  // This is an experimental trusted-code runtime, not a sandbox. The standard libraries include
  // operating-system and native-library access.
  luaL_openlibs(state);
  int asyncApiLoadStatus =
      luaL_loadbuffer(state, ASYNC_API_SOURCE, sizeof(ASYNC_API_SOURCE) - 1, "workerd-async.lua");
  KJ_REQUIRE(
      asyncApiLoadStatus == 0, "failed to compile LuaJIT async API:", readLuaErrorMessage(state));
  int asyncApiEvaluationStatus = lua_pcall(state, 0, 0, 0);
  KJ_REQUIRE(asyncApiEvaluationStatus == 0,
      "failed to evaluate LuaJIT async API:", readLuaErrorMessage(state));
  lua_getglobal(state, "workerd");
  getRawField(state, -1, "_makeBodyProducer");
  KJ_REQUIRE(lua_isfunction(state, -1), "LuaJIT async API is missing its body producer wrapper");
  bodyProducerWrapperRef = luaL_ref(state, LUA_REGISTRYINDEX);
  lua_pushnil(state);
  lua_setfield(state, -2, "_makeBodyProducer");
  lua_pop(state, 1);
  pushLuaBindings(state, bindings.asPtr());
  lua_getglobal(state, "workerd");
  getRawField(state, -1, "bindings");
  KJ_REQUIRE(lua_istable(state, -1), "LuaJIT configured bindings table is unavailable");
  bindingsRef = luaL_ref(state, LUA_REGISTRYINDEX);
  lua_pop(state, 1);
  lua_getglobal(state, "workerd");
  lua_pushcfunction(state, parseUrl);
  lua_setfield(state, -2, "parseUrl");
  lua_pushcfunction(state, digest);
  lua_setfield(state, -2, "digest");
  lua_pushcfunction(state, hmac);
  lua_setfield(state, -2, "hmac");
  lua_pushcfunction(state, verifyHmac);
  lua_setfield(state, -2, "verifyHmac");
  lua_pop(state, 1);

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

kj::Promise<LuaRuntime::Response> LuaRuntime::fetch(kj::StringPtr method,
    kj::StringPtr url,
    const kj::HttpHeaders& headers,
    kj::ArrayPtr<const kj::byte> body,
    kj::Maybe<kj::Ptr<LuaHostOperations>> hostOperations,
    LuaRequestBodyMode requestBodyMode) {
  KJ_REQUIRE(
      body.size() <= MAX_HTTP_BODY_BYTES, "LuaJIT request body exceeds limit", MAX_HTTP_BODY_BYTES);
  KJ_REQUIRE(
      headers.size() <= MAX_HTTP_HEADERS, "LuaJIT request has too many headers", MAX_HTTP_HEADERS);

  LuaStackGuard stackGuard(state);
  lua_State* coroutine = lua_newthread(state);
  int coroutineRef = luaL_ref(state, LUA_REGISTRYINDEX);
  KJ_DEFER(luaL_unref(state, LUA_REGISTRYINDEX, coroutineRef));

  KJ_REQUIRE(nextRequestId != 0, "LuaJIT request identifier space exhausted");
  auto requestId = kj::str(nextRequestId++);

  bool hasRequestBodyStream = false;
  if (requestBodyMode == LuaRequestBodyMode::STREAMING) {
    KJ_IF_SOME(host, hostOperations) {
      KJ_REQUIRE(host->hasRequestBodyStream(),
          "streaming request body mode requires an inbound body stream");
      hasRequestBodyStream = true;
    } else {
      KJ_FAIL_REQUIRE("streaming request body mode requires host operations");
    }
  }

  lua_rawgeti(coroutine, LUA_REGISTRYINDEX, fetchHandlerRef);
  lua_createtable(coroutine, 0, hasRequestBodyStream ? 6 : 5);
  setStringField(coroutine, "method", method);
  setStringField(coroutine, "url", url);

  if (hasRequestBodyStream) {
    lua_pushnil(coroutine);
    lua_setfield(coroutine, -2, "body");
    lua_createtable(coroutine, 0, 2);
    setStringField(coroutine, "requestId", requestId);
    lua_pushinteger(coroutine, 0);
    lua_setfield(coroutine, -2, "streamId");
    lua_setfield(coroutine, -2, "bodyStream");
  } else {
    lua_pushlstring(coroutine, reinterpret_cast<const char*>(body.begin()), body.size());
    lua_setfield(coroutine, -2, "body");
  }

  pushRequestHeaders(coroutine, headers);
  lua_setfield(coroutine, -2, "headers");

  KJ_IF_SOME(host, hostOperations) {
    KJ_IF_SOME(cfJson, host->getRequestCfJson()) {
      installRequestCfJson(coroutine, -1, cfJson);
    }
  }

  lua_rawgeti(coroutine, LUA_REGISTRYINDEX, bindingsRef);

  auto requestContext = kj::rc<LuaRequestContext>(
      timer, kj::mv(hostOperations), requestBodyMode, kj::str(requestId), bodyProducerWrapperRef);
  int status = lua_resume(coroutine, 2);
  status = co_await resumeLuaCoroutine(coroutine, status, requestContext.addRef());
  KJ_REQUIRE(status == 0, "LuaJIT fetch() failed:", readLuaErrorMessage(coroutine));
  KJ_REQUIRE(lua_istable(coroutine, -1), "LuaJIT fetch() must return a response table");
  int responseIndex = absoluteIndex(coroutine, -1);

  getRawField(coroutine, responseIndex, "status");
  int isNumber = 0;
  lua_Integer luaStatus = lua_tointegerx(coroutine, -1, &isNumber);
  bool validStatus = lua_type(coroutine, -1) == LUA_TNUMBER && isNumber && luaStatus >= 200 &&
      luaStatus <= 599 && static_cast<lua_Number>(luaStatus) == lua_tonumber(coroutine, -1);
  if (!validStatus) {
    KJ_FAIL_REQUIRE("LuaJIT response.status must be an integer from 200 through 599");
  }
  lua_pop(coroutine, 1);

  auto responseBody = readStringField(coroutine, responseIndex, "body");
  KJ_REQUIRE(responseBody.size() <= MAX_HTTP_BODY_BYTES, "LuaJIT response body exceeds limit",
      MAX_HTTP_BODY_BYTES);
  auto bodyBytes = kj::heapArray<kj::byte>(responseBody.size());
  if (responseBody.size() > 0) {
    std::memcpy(bodyBytes.begin(), responseBody.begin(), responseBody.size());
  }

  kj::Vector<Header> responseHeaders;
  getRawField(coroutine, responseIndex, "headers");
  if (!lua_isnil(coroutine, -1)) {
    responseHeaders = readHttpHeaders(coroutine, -1);
  }
  lua_pop(coroutine, 1);

  co_return Response{
    .status = static_cast<uint16_t>(luaStatus),
    .headers = kj::mv(responseHeaders),
    .body = kj::mv(bodyBytes),
  };
}

}  // namespace workerd::rt::luajit
