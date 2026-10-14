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

enum class LuaHmacKeyUsage {
  SIGN,
  VERIFY,
};

// Opaque, non-extractable HMAC key data parsed from a supported CryptoKey binding.
class LuaHmacKey final {
 public:
  LuaHmacKey(
      kj::String hashAlgorithm, kj::Array<kj::byte> keyData, kj::Array<LuaHmacKeyUsage> usages);
  ~LuaHmacKey() noexcept;
  KJ_DISALLOW_COPY_AND_MOVE(LuaHmacKey);

  kj::StringPtr getHashAlgorithm() const;
  kj::ArrayPtr<const kj::byte> getKeyData() const;
  kj::ArrayPtr<const LuaHmacKeyUsage> getUsages() const;

 private:
  kj::String hashAlgorithm;
  kj::Array<kj::byte> keyData;
  kj::Array<LuaHmacKeyUsage> usages;
};

// A configured binding in the service-global env table (`workerd.bindings` alias).
struct LuaBinding {
  enum class Type {
    TEXT,
    DATA,
    JSON,
    HMAC_KEY,
    NULL_VALUE,
    SERVICE,
    KV_NAMESPACE,
    D1_DATABASE,
  };

  kj::String name;
  Type type;
  size_t serviceBindingIndex = 0;
  size_t kvBindingIndex = 0;
  kj::Own<LuaHmacKey> hmacKey;
  kj::String text;
  kj::Array<kj::byte> data;
};

struct LuaHttpHeader {
  kj::String name;
  kj::String value;
};

class LuaRequestBodyProducer {
 public:
  virtual ~LuaRequestBodyProducer() noexcept(false) = default;
  // The producer is tied to its LuaRuntime; hosts must consume or destroy it before fetch completes.
  // Producers run in a child Lua coroutine and may yield through request host operations. Return one
  // chunk or none at EOF; hosts request the next chunk only after accepting the previous one.
  virtual kj::Promise<kj::Maybe<kj::Array<kj::byte>>> nextChunk() = 0;
};

struct LuaHttpRequest {
  kj::String method;
  kj::String url;
  kj::Vector<LuaHttpHeader> headers;
  kj::Array<kj::byte> body;
  // When present, body bytes come from this request's inbound body stream.
  kj::Maybe<kj::String> bodyStreamRequestId;
  // When present, the host pulls chunks from this Lua-produced source as backpressure allows.
  kj::Maybe<kj::Own<LuaRequestBodyProducer>> bodyProducer;
  kj::Maybe<size_t> serviceBindingIndex;
};

struct LuaHttpResponse {
  uint16_t status;
  kj::Vector<LuaHttpHeader> headers;
  kj::Own<kj::AsyncInputStream> body;
};

enum class LuaKvGetMode {
  VALUE,
  WITH_METADATA,
};

struct LuaKvGetOptions {
  kj::Maybe<uint32_t> cacheTtl;
};

struct LuaKvGetResponse {
  kj::Maybe<kj::Own<kj::AsyncInputStream>> body;
  kj::Maybe<kj::String> metadata;
  kj::Maybe<kj::String> cacheStatus;
};

struct LuaKvListResponse {
  kj::Own<kj::AsyncInputStream> body;
  kj::Maybe<kj::String> cacheStatus = kj::none;
};

struct LuaKvPutOptions {
  kj::Maybe<uint32_t> expiration;
  kj::Maybe<uint32_t> expirationTtl;
};

struct LuaKvListOptions {
  kj::Maybe<uint32_t> limit;
  kj::Maybe<kj::String> prefix;
  kj::Maybe<kj::String> cursor;
};

struct LuaResponse {
  uint16_t status;
  kj::Vector<LuaHttpHeader> headers;
  kj::Array<kj::byte> body;
};

enum class LuaRequestBodyMode {
  BUFFERED,
  // Expose request.bodyStream and host-mediated chunk reads while preserving the default mode.
  STREAMING,
};

class LuaHostOperations: public kj::PtrTarget {
 public:
  virtual ~LuaHostOperations() noexcept(false) = default;

  kj::Ptr<LuaHostOperations> getPtr() {
    return addPtrToThis();
  }
  virtual kj::Promise<LuaHttpResponse> fetch(LuaHttpRequest request) = 0;
  // Use the request's configured entropy source.
  virtual void fillRandomBytes(kj::ArrayPtr<kj::byte> output) = 0;
  virtual kj::String generateRandomUuid() = 0;
  // Keep serialized metadata host-neutral and defer decoding until Lua accesses request.cf.
  virtual kj::Maybe<kj::String> getRequestCfJson() const {
    return kj::none;
  }
  virtual bool hasRequestBodyStream() const = 0;
  // Read up to maxBytes from the current inbound request body; return none at EOF.
  virtual kj::Promise<kj::Maybe<kj::Array<kj::byte>>> readRequestBody(size_t maxBytes) = 0;
  virtual kj::Promise<LuaKvGetResponse> kvGet(
      size_t bindingIndex, kj::String key, LuaKvGetOptions options, LuaKvGetMode mode) = 0;
  virtual kj::Promise<void> kvPut(
      size_t bindingIndex, kj::String key, kj::Array<kj::byte> value, LuaKvPutOptions options) = 0;
  virtual kj::Promise<void> kvDelete(size_t bindingIndex, kj::String key) = 0;
  virtual kj::Promise<LuaKvListResponse> kvList(size_t bindingIndex, LuaKvListOptions options) = 0;
};

class LuaRuntime final {
 public:
  using Header = LuaHttpHeader;
  using Response = LuaResponse;

  static constexpr size_t MAX_HTTP_BODY_BYTES = 1024 * 1024;
  static constexpr size_t MAX_HTTP_HEADERS = 128;
  static constexpr size_t MAX_HTTP_READ_CHUNK_BYTES = 64 * 1024;
  static constexpr size_t MAX_RANDOM_BYTES = 64 * 1024;
  static constexpr size_t MAX_HMAC_KEY_BYTES = 64 * 1024;
  static constexpr size_t MAX_CRYPTO_INPUT_BYTES = 1024 * 1024;
  static constexpr size_t MAX_KV_KEY_BYTES = 512;
  static constexpr size_t MAX_KV_CURSOR_BYTES = 4 * 1024;
  static constexpr size_t MAX_KV_VALUE_BYTES = 1024 * 1024;
  static constexpr size_t MAX_KV_RESPONSE_BYTES = 1024 * 1024;
  static constexpr size_t MAX_D1_SQL_BYTES = 64 * 1024;
  static constexpr size_t MAX_D1_BINDING_VALUES = 100;
  static constexpr size_t MAX_D1_RESPONSE_BYTES = 1024 * 1024;

  // The module must return a table with a callable `fetch` field. The runtime calls it with
  // `(request, env)`, where `env` contains configured bindings and aliases `workerd.bindings`.
  // This runtime owns one LuaJIT state for its lifetime; each fetch() and request-body producer
  // runs on a coroutine and yields through host async operations. Coroutine access to the shared
  // state stays serialized on the event-loop thread. Bindings are parsed and installed before the
  // module source is evaluated.
  LuaRuntime(
      kj::StringPtr moduleSource, kj::Timer& timer, kj::Array<LuaBinding> bindings = nullptr);
  ~LuaRuntime() noexcept;
  KJ_DISALLOW_COPY_AND_MOVE(LuaRuntime);

  kj::Promise<Response> fetch(kj::StringPtr method,
      kj::StringPtr url,
      const kj::HttpHeaders& headers,
      kj::ArrayPtr<const kj::byte> body,
      kj::Maybe<kj::Ptr<LuaHostOperations>> hostOperations = kj::none,
      LuaRequestBodyMode requestBodyMode = LuaRequestBodyMode::BUFFERED);

 private:
  kj::Timer& timer;
  lua_State* state;
  int fetchHandlerRef;
  int bindingsRef;
  int bodyProducerWrapperRef;
  uint64_t nextRequestId = 1;
};

}  // namespace workerd::rt::luajit
