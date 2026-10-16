// Copyright (c) 2026 Cloudflare, Inc.
// Licensed under the Apache 2.0 license found in the LICENSE file or at:
//     https://opensource.org/licenses/Apache-2.0

#include "lua-worker-interface.h"

#include <workerd/io/trace.h>
#include <workerd/util/http-util.h>
#include <workerd/util/uuid.h>

#include <kj/compat/url.h>
#include <kj/debug.h>

#include <cstring>

namespace workerd::rt::luajit {
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

LuaIoContext::LuaIoContext(IoChannelFactory::SubrequestChannel& outbound,
    const kj::HttpHeaderTable& headerTable,
    IoChannelFactory::SubrequestMetadata metadata,
    kj::EntropySource& entropySource,
    kj::Own<SubrequestLimitPolicy> subrequestLimitPolicy,
    kj::Own<WorkerInvocationObserver> observer,
    kj::ArrayPtr<kj::Own<IoChannelFactory::SubrequestChannel>> serviceBindings,
    kj::ArrayPtr<kj::Own<IoChannelFactory::SubrequestChannel>> kvBindings,
    kj::Maybe<KvOperationLimitPolicy&> kvOperationLimitPolicy)
    : subrequestLimitPolicy(kj::mv(subrequestLimitPolicy)),
      observer(kj::mv(observer)),
      workerInvocation(*this->subrequestLimitPolicy,
          *this->observer,
          RequestScopedCancellationDomain{},
          kj::mv(kvOperationLimitPolicy)),
      outbound(outbound),
      serviceBindings(serviceBindings),
      kvBindings(kvBindings),
      headerTable(headerTable),
      entropySource(entropySource),
      cfBlobJson(kj::mv(metadata.cfBlobJson)),
      parentSpan(kj::mv(metadata.parentSpan)),
      userSpanParent(kj::mv(metadata.userSpanParent)) {}

LuaIoContext::~LuaIoContext() noexcept(false) = default;

bool LuaIoContext::hasRequestBodyStream() const {
  return requestBodyStream != kj::none;
}

void LuaIoContext::fillRandomBytes(kj::ArrayPtr<kj::byte> output) {
  entropySource.generate(output);
}

kj::String LuaIoContext::generateRandomUuid() {
  return workerd::randomUUID(entropySource);
}

kj::Maybe<kj::String> LuaIoContext::getRequestCfJson() const {
  return cfBlobJson.map([](const kj::String& value) { return kj::str(value); });
}

void LuaIoContext::setRequestBodyStream(kj::AsyncInputStream& stream) {
  KJ_REQUIRE(requestBodyStream == kj::none, "inbound request body stream is already set");
  requestBodyStream = stream;
  requestBodyBytesRead = 0;
}

void LuaIoContext::clearRequestBodyStream() {
  requestBodyStream = kj::none;
  requestBodyBytesRead = 0;
}

kj::Promise<kj::Maybe<kj::Array<kj::byte>>> LuaIoContext::readRequestBody(size_t maxBytes) {
  KJ_REQUIRE(maxBytes >= 1 && maxBytes <= LuaRuntime::MAX_HTTP_READ_CHUNK_BYTES,
      "workerd.readRequestBody() chunk size is outside the allowed range");
  KJ_IF_SOME(stream, requestBodyStream) {
    size_t bytesRemaining = requestBodyBytesRead < LuaRuntime::MAX_HTTP_BODY_BYTES
        ? LuaRuntime::MAX_HTTP_BODY_BYTES - requestBodyBytesRead
        : 0;
    size_t readLimit =
        bytesRemaining == 0 ? 1 : (maxBytes < bytesRemaining ? maxBytes : bytesRemaining);
    auto chunk = kj::heapArray<kj::byte>(readLimit);
    size_t bytesRead = co_await stream.tryRead(chunk.begin(), 1, readLimit);
    if (bytesRead == 0) co_return kj::none;
    KJ_REQUIRE(
        bytesRemaining != 0, "LuaJIT request body exceeds limit", LuaRuntime::MAX_HTTP_BODY_BYTES);
    requestBodyBytesRead += bytesRead;
    auto result = kj::heapArray<kj::byte>(bytesRead);
    std::memcpy(result.begin(), chunk.begin(), bytesRead);
    co_return kj::mv(result);
  }
  KJ_FAIL_REQUIRE("inbound request body streaming is unavailable in this context");
}

IoChannelFactory::SubrequestMetadata LuaIoContext::makeSubrequestMetadata(TraceContext& tracing) {
  return IoChannelFactory::SubrequestMetadata{
    .cfBlobJson = cfBlobJson.map([](const kj::String& value) { return kj::str(value); }),
    .parentSpan = tracing.getInternalSpanParent(),
    .userSpanParent = tracing.getUserSpanParent(),
  };
}

kj::Own<kj::HttpClient> LuaIoContext::getKvHttpClient(size_t bindingIndex,
    KvOperationType operation,
    kj::StringPtr url,
    kj::HttpHeaders& headers,
    TraceContext& tracing) {
  KJ_REQUIRE(bindingIndex < kvBindings.size(), "unknown Lua KV binding index", bindingIndex);
  workerInvocation.checkKvOperation(operation);
  workerInvocation.checkSubrequest(SubrequestType::IN_HOUSE);
  // KV operation counts use the KV policy; logical subrequest observation remains on fetch calls.
  auto subrequest = workerInvocation.wrapHostClientForCancellation(
      kvBindings[bindingIndex]->startRequest(IoChannelFactory::SubrequestMetadata{
        .cfBlobJson = kj::none,
        .parentSpan = tracing.getInternalSpanParent(),
        .userSpanParent = tracing.getUserSpanParent(),
      }));
  headers.addPtrPtr("CF-KV-FLPROD-405"_kj, url);
  return asHttpClient(kj::mv(subrequest));
}

kj::Promise<LuaKvGetResponse> LuaIoContext::kvGet(
    size_t bindingIndex, kj::String key, LuaKvGetOptions options, LuaKvGetMode mode) {
  auto tracing = TraceContextParent(parentSpan.addRef(), userSpanParent.addRef())
                     .newChild(kj::ConstString(kj::str(
                         mode == LuaKvGetMode::VALUE ? "lua.kv.get" : "lua.kv.get_with_metadata")));
  tracing.setTag("cloudflare.kv.query.keys"_kjc, key.asPtr());
  tracing.setTag("cloudflare.kv.query.keys.count"_kjc, static_cast<int64_t>(1));

  kj::Url url;
  url.scheme = kj::str("https");
  url.host = kj::str("fake-host");
  url.path.add(kj::mv(key));
  url.query.add(kj::Url::QueryParam{kj::str("urlencoded"), kj::str("true")});
  KJ_IF_SOME(cacheTtl, options.cacheTtl) {
    url.query.add(kj::Url::QueryParam{kj::str("cache_ttl"), kj::str(cacheTtl)});
    tracing.setTag("cloudflare.kv.query.cache_ttl"_kjc, static_cast<int64_t>(cacheTtl));
  }
  auto urlText = url.toString(kj::Url::Context::HTTP_PROXY_REQUEST);
  kj::HttpHeaders headers(headerTable);
  auto operation = mode == LuaKvGetMode::VALUE ? KvOperationType::GET : KvOperationType::GET_WITH;
  auto client = getKvHttpClient(bindingIndex, operation, urlText, headers, tracing);
  auto request = attachToRequest(client->request(kj::HttpMethod::GET, urlText, headers),
      kj::Rc<kj::HttpClient>(kj::mv(client)));
  auto response = co_await kj::mv(request.response);
  kj::Maybe<kj::String> cacheStatus;
  KJ_IF_SOME(headerId, headerTable.stringToId("cf-cache-status")) {
    cacheStatus =
        response.headers->get(headerId).map([](kj::StringPtr value) { return kj::str(value); });
  }
  if (response.statusCode == 404 || response.statusCode == 410) {
    co_return LuaKvGetResponse{.body = kj::none, .cacheStatus = kj::mv(cacheStatus)};
  }
  KJ_REQUIRE(response.statusCode >= 200 && response.statusCode < 300,
      "Lua KV GET failed:", response.statusCode, response.statusText);
  kj::Maybe<kj::String> metadata;
  KJ_IF_SOME(headerId, headerTable.stringToId("cf-kv-metadata")) {
    metadata =
        response.headers->get(headerId).map([](kj::StringPtr value) { return kj::str(value); });
  }
  co_return LuaKvGetResponse{
    .body = kj::mv(response.body),
    .metadata = kj::mv(metadata),
    .cacheStatus = kj::mv(cacheStatus),
  };
}

kj::Promise<void> LuaIoContext::kvPut(
    size_t bindingIndex, kj::String key, kj::Array<kj::byte> value, LuaKvPutOptions options) {
  auto tracing = TraceContextParent(parentSpan.addRef(), userSpanParent.addRef())
                     .newChild(kj::ConstString(kj::str("lua.kv.put")));
  tracing.setTag("cloudflare.kv.query.keys"_kjc, key.asPtr());
  tracing.setTag("cloudflare.kv.query.keys.count"_kjc, static_cast<int64_t>(1));

  kj::Url url;
  url.scheme = kj::str("https");
  url.host = kj::str("fake-host");
  url.path.add(kj::mv(key));
  url.query.add(kj::Url::QueryParam{kj::str("urlencoded"), kj::str("true")});
  KJ_IF_SOME(expiration, options.expiration) {
    url.query.add(kj::Url::QueryParam{kj::str("expiration"), kj::str(expiration)});
    tracing.setTag("cloudflare.kv.query.expiration"_kjc, static_cast<int64_t>(expiration));
  }
  KJ_IF_SOME(expirationTtl, options.expirationTtl) {
    url.query.add(kj::Url::QueryParam{kj::str("expiration_ttl"), kj::str(expirationTtl)});
    tracing.setTag("cloudflare.kv.query.expiration_ttl"_kjc, static_cast<int64_t>(expirationTtl));
  }
  auto urlText = url.toString(kj::Url::Context::HTTP_PROXY_REQUEST);
  kj::HttpHeaders headers(headerTable);
  headers.setPtr(kj::HttpHeaderId::CONTENT_TYPE, "application/octet-stream"_kj);
  auto client = getKvHttpClient(bindingIndex, KvOperationType::PUT, urlText, headers, tracing);
  auto request = attachToRequest(
      client->request(kj::HttpMethod::PUT, urlText, headers, static_cast<uint64_t>(value.size())),
      kj::Rc<kj::HttpClient>(kj::mv(client)));
  KJ_REQUIRE(request.body.get() != nullptr, "Lua KV PUT request has no body stream");
  auto writePromise = request.body->write(value).attach(kj::mv(request.body), kj::mv(value));
  co_await kj::mv(writePromise);
  auto response = co_await kj::mv(request.response);
  KJ_REQUIRE(response.statusCode >= 200 && response.statusCode < 300,
      "Lua KV PUT failed:", response.statusCode, response.statusText);
  co_await response.body->readAllBytes(64 * 1024).attach(kj::mv(response.body));
}

kj::Promise<void> LuaIoContext::kvDelete(size_t bindingIndex, kj::String key) {
  kj::Url url;
  url.scheme = kj::str("https");
  url.host = kj::str("fake-host");
  url.path.add(kj::mv(key));
  url.query.add(kj::Url::QueryParam{kj::str("urlencoded"), kj::str("true")});
  auto urlText = url.toString(kj::Url::Context::HTTP_PROXY_REQUEST);
  kj::HttpHeaders headers(headerTable);
  auto tracing = TraceContextParent(parentSpan.addRef(), userSpanParent.addRef())
                     .newChild(kj::ConstString(kj::str("lua.kv.delete")));
  auto client = getKvHttpClient(bindingIndex, KvOperationType::DELETE, urlText, headers, tracing);
  auto request = attachToRequest(
      client->request(kj::HttpMethod::DELETE, urlText, headers, static_cast<uint64_t>(0)),
      kj::Rc<kj::HttpClient>(kj::mv(client)));
  auto response = co_await kj::mv(request.response);
  KJ_REQUIRE(response.statusCode >= 200 && response.statusCode < 300,
      "Lua KV DELETE failed:", response.statusCode, response.statusText);
  co_await response.body->readAllBytes(64 * 1024).attach(kj::mv(response.body));
}

kj::Promise<LuaKvListResponse> LuaIoContext::kvList(size_t bindingIndex, LuaKvListOptions options) {
  kj::Url url;
  url.scheme = kj::str("https");
  url.host = kj::str("fake-host");
  KJ_IF_SOME(limit, options.limit) {
    if (limit > 0) {
      url.query.add(kj::Url::QueryParam{kj::str("key_count_limit"), kj::str(limit)});
    }
  }
  KJ_IF_SOME(prefix, options.prefix) {
    url.query.add(kj::Url::QueryParam{kj::str("prefix"), kj::mv(prefix)});
  }
  KJ_IF_SOME(cursor, options.cursor) {
    url.query.add(kj::Url::QueryParam{kj::str("cursor"), kj::mv(cursor)});
  }
  auto urlText = url.toString(kj::Url::Context::HTTP_PROXY_REQUEST);
  kj::HttpHeaders headers(headerTable);
  auto tracing = TraceContextParent(parentSpan.addRef(), userSpanParent.addRef())
                     .newChild(kj::ConstString(kj::str("lua.kv.list")));
  auto client = getKvHttpClient(bindingIndex, KvOperationType::LIST, urlText, headers, tracing);
  auto request = attachToRequest(client->request(kj::HttpMethod::GET, urlText, headers),
      kj::Rc<kj::HttpClient>(kj::mv(client)));
  auto response = co_await kj::mv(request.response);
  KJ_REQUIRE(response.statusCode >= 200 && response.statusCode < 300,
      "Lua KV LIST failed:", response.statusCode, response.statusText);
  kj::Maybe<kj::String> cacheStatus;
  KJ_IF_SOME(headerId, headerTable.stringToId("cf-cache-status")) {
    cacheStatus =
        response.headers->get(headerId).map([](kj::StringPtr value) { return kj::str(value); });
  }
  co_return LuaKvListResponse{.body = kj::mv(response.body), .cacheStatus = kj::mv(cacheStatus)};
}

kj::Promise<LuaHttpResponse> LuaIoContext::fetch(LuaHttpRequest request) {
  kj::HttpMethod method = kj::HttpMethod::GET;
  KJ_IF_SOME(parsedMethod, kj::tryParseHttpMethod(request.method)) {
    method = parsedMethod;
  } else {
    KJ_FAIL_REQUIRE("workerd.fetch() has an unsupported HTTP method", request.method);
  }
  bool hasInboundBodyStream = request.bodyStreamRequestId != kj::none;
  bool hasBodyProducer = request.bodyProducer != kj::none;
  bool hasStreamingBody = hasInboundBodyStream || hasBodyProducer;
  KJ_REQUIRE(!(hasInboundBodyStream && hasBodyProducer),
      "workerd.fetch() request cannot have multiple streaming body sources");
  KJ_REQUIRE(!hasStreamingBody || request.body.size() == 0,
      "workerd.fetch() request cannot combine a string body with a streaming body source");
  KJ_REQUIRE((method != kj::HttpMethod::GET && method != kj::HttpMethod::HEAD) ||
          (request.body.size() == 0 && !hasStreamingBody),
      "workerd.fetch() GET and HEAD requests cannot have a body");
  KJ_REQUIRE(!hasInboundBodyStream || requestBodyStream != kj::none,
      "workerd.fetch() request body stream is unavailable in this request");

  kj::HttpHeaders headers(headerTable);
  for (auto& header: request.headers) {
    headers.addPtrPtr(header.name, header.value);
  }
  if (hasStreamingBody) {
    KJ_IF_SOME(id, headerTable.stringToId("content-length")) headers.unset(id);
    KJ_IF_SOME(id, headerTable.stringToId("transfer-encoding")) headers.unset(id);
  }

  KJ_IF_SOME(index, request.serviceBindingIndex) {
    KJ_REQUIRE(index < serviceBindings.size(), "unknown Lua service binding index", index);
  }
  workerInvocation.checkSubrequest(SubrequestType::EXTERNAL);
  auto tracing = TraceContextParent(parentSpan.addRef(), userSpanParent.addRef())
                     .newChild(kj::ConstString(kj::str("lua.fetch")));
  auto startRequest = [&]() {
    KJ_IF_SOME(index, request.serviceBindingIndex) {
      return serviceBindings[index]->startRequest(makeSubrequestMetadata(tracing));
    }
    return outbound.startRequest(makeSubrequestMetadata(tracing));
  };
  auto subrequest = workerInvocation.wrapSubrequestClient(startRequest(), CountSubrequest::YES);
  auto client = asHttpClient(kj::mv(subrequest));
  kj::Maybe<uint64_t> expectedBodySize = kj::none;
  if (!hasStreamingBody) expectedBodySize = request.body.size();
  auto outgoing = client->request(method, request.url, headers, expectedBodySize);
  if (outgoing.body.get() != nullptr) {
    if (hasInboundBodyStream) {
      while (true) {
        KJ_IF_SOME(chunk, co_await readRequestBody(LuaRuntime::MAX_HTTP_READ_CHUNK_BYTES)) {
          co_await outgoing.body->write(chunk);
        } else {
          break;
        }
      }
    } else KJ_IF_SOME(producer, request.bodyProducer) {
      size_t bytesWritten = 0;
      while (true) {
        KJ_IF_SOME(chunk, co_await producer->nextChunk()) {
          KJ_REQUIRE(chunk.size() <= LuaRuntime::MAX_HTTP_BODY_BYTES - bytesWritten,
              "workerd.fetch() request body exceeds limit", LuaRuntime::MAX_HTTP_BODY_BYTES);
          bytesWritten += chunk.size();
          co_await outgoing.body->write(chunk);
        } else {
          break;
        }
      }
    } else if (request.body.size() > 0) {
      co_await outgoing.body->write(request.body);
    }
    outgoing.body = nullptr;
  }

  auto response = co_await kj::mv(outgoing.response);
  KJ_REQUIRE(response.headers->size() <= LuaRuntime::MAX_HTTP_HEADERS,
      "workerd.fetch() response has too many headers", LuaRuntime::MAX_HTTP_HEADERS);
  kj::Vector<LuaRuntime::Header> responseHeaders;
  response.headers->forEach([&](kj::StringPtr name, kj::StringPtr value) {
    responseHeaders.add(LuaRuntime::Header{.name = kj::str(name), .value = kj::str(value)});
  });
  co_return LuaHttpResponse{
    .status = static_cast<uint16_t>(response.statusCode),
    .headers = kj::mv(responseHeaders),
    .body = kj::mv(response.body),
  };
}

LuaWorkerInterface::LuaWorkerInterface(LuaRuntime& runtime,
    IoChannelFactory::SubrequestChannel& outbound,
    const kj::HttpHeaderTable& headerTable,
    IoChannelFactory::SubrequestMetadata metadata,
    kj::EntropySource& entropySource,
    kj::Own<SubrequestLimitPolicy> subrequestLimitPolicy,
    kj::Own<WorkerInvocationObserver> observer,
    LuaRequestBodyMode requestBodyMode,
    kj::ArrayPtr<kj::Own<IoChannelFactory::SubrequestChannel>> serviceBindings,
    kj::ArrayPtr<kj::Own<IoChannelFactory::SubrequestChannel>> kvBindings,
    kj::Maybe<KvOperationLimitPolicy&> kvOperationLimitPolicy)
    : runtime(runtime),
      hostOperations(outbound,
          headerTable,
          kj::mv(metadata),
          entropySource,
          kj::mv(subrequestLimitPolicy),
          kj::mv(observer),
          serviceBindings,
          kvBindings,
          kvOperationLimitPolicy),
      requestBodyMode(requestBodyMode) {}

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
  kj::Maybe<LuaRuntime::Response> maybeResult = kj::none;
  if (requestBodyMode == LuaRequestBodyMode::STREAMING) {
    hostOperations.setRequestBodyStream(requestBody);
    {
      KJ_DEFER(hostOperations.clearRequestBodyStream());
      maybeResult = co_await runtime.fetch(
          methodName, urlCopy, requestHeaders, nullptr, hostOperations.getPtr(), requestBodyMode);
    }
  } else {
    auto body = co_await requestBody.readAllBytes(LuaRuntime::MAX_HTTP_BODY_BYTES);
    maybeResult = co_await runtime.fetch(
        methodName, urlCopy, requestHeaders, body, hostOperations.getPtr(), requestBodyMode);
  }
  auto result = KJ_ASSERT_NONNULL(kj::mv(maybeResult));

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
  auto output =
      response.reject(501, "Not Implemented"_kj, responseHeaders, static_cast<uint64_t>(0));
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

}  // namespace workerd::rt::luajit
