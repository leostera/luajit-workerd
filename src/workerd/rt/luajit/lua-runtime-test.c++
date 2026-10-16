// Copyright (c) 2026 Cloudflare, Inc.
// Licensed under the Apache 2.0 license found in the LICENSE file or at:
//     https://opensource.org/licenses/Apache-2.0

#include "lua-runtime.h"

#include <workerd/io/observer.h>
#include <workerd/rt/luajit/lua-worker-interface.h>
#include <workerd/util/stream-utils.h>

#include <kj/async-io.h>
#include <kj/test.h>

#include <cstring>

namespace workerd::rt::luajit {
namespace {

kj::String asString(kj::ArrayPtr<const kj::byte> bytes) {
  return kj::heapString(reinterpret_cast<const char*>(bytes.begin()), bytes.size());
}

class TestEntropySource final: public kj::EntropySource {
 public:
  void generate(kj::ArrayPtr<kj::byte> buffer) override {
    for (size_t i = 0; i < buffer.size(); ++i) buffer[i] = static_cast<kj::byte>(i);
  }
};

TestEntropySource& testEntropySource() {
  static TestEntropySource source;
  return source;
}

class InertSubrequestChannel final: public IoChannelFactory::SubrequestChannel {
 public:
  void requireAllowsTransfer() override {}

  kj::OneOf<kj::Array<byte>, kj::Promise<kj::Array<byte>>> getTokenMaybeSync(
      IoChannelFactory::ChannelTokenUsage usage) override {
    KJ_UNREACHABLE;
  }

  kj::Own<WorkerInterface> startRequest(IoChannelFactory::SubrequestMetadata metadata) override {
    KJ_UNREACHABLE;
  }
};

class PendingHostWorker final: public WorkerInterface {
 public:
  explicit PendingHostWorker(bool& canceled): canceled(canceled) {}

  kj::Promise<void> request(kj::HttpMethod method,
      kj::StringPtr url,
      const kj::HttpHeaders& headers,
      kj::AsyncInputStream& requestBody,
      Response& response) override {
    auto* canceledFlag = &canceled;
    return kj::Promise<void>(kj::NEVER_DONE).attach(kj::defer([canceledFlag]() {
      *canceledFlag = true;
    }));
  }

  kj::Promise<void> connect(kj::StringPtr host,
      const kj::HttpHeaders& headers,
      kj::AsyncIoStream& connection,
      ConnectResponse& response,
      kj::HttpConnectSettings settings) override {
    KJ_FAIL_REQUIRE("unexpected CONNECT");
  }

  kj::Promise<void> prewarm(kj::StringPtr url) override {
    return kj::READY_NOW;
  }

  kj::Promise<ScheduledResult> runScheduled(kj::Date scheduledTime, kj::StringPtr cron) override {
    KJ_FAIL_REQUIRE("unexpected scheduled event");
  }

  kj::Promise<AlarmResult> runAlarm(kj::Date scheduledTime, uint32_t retryCount) override {
    KJ_FAIL_REQUIRE("unexpected alarm");
  }

  kj::Promise<CustomEvent::Result> customEvent(kj::Own<CustomEvent> event) override {
    KJ_FAIL_REQUIRE("unexpected custom event");
  }

 private:
  bool& canceled;
};

class PendingSubrequestChannel final: public IoChannelFactory::SubrequestChannel {
 public:
  explicit PendingSubrequestChannel(bool& canceled): canceled(canceled) {}

  void requireAllowsTransfer() override {}

  kj::OneOf<kj::Array<byte>, kj::Promise<kj::Array<byte>>> getTokenMaybeSync(
      IoChannelFactory::ChannelTokenUsage usage) override {
    KJ_UNREACHABLE;
  }

  kj::Own<WorkerInterface> startRequest(IoChannelFactory::SubrequestMetadata metadata) override {
    return kj::heap<PendingHostWorker>(canceled);
  }

 private:
  bool& canceled;
};

class TraceObservation final: public kj::Refcounted {
 public:
  kj::Vector<kj::String> operationNames;
};

class RecordingSpanObserver final: public SpanObserver {
 public:
  explicit RecordingSpanObserver(kj::Rc<TraceObservation> observation)
      : observation(kj::mv(observation)) {}

  kj::Rc<SpanObserver> newChild() override {
    return kj::rc<RecordingSpanObserver>(observation.addRef());
  }

  void onOpen(kj::ConstString operationName, kj::Date startTime) override {
    observation->operationNames.add(kj::str(operationName));
  }

  void onClose(kj::Date endTime, Span::TagMap&& tags, kj::Vector<Span::Log>&& logs) override {}

  kj::Maybe<tracing::SpanContext> toSpanContext() override {
    return tracing::SpanContext(tracing::TraceId(0x1234, 0x5678), tracing::SpanId(0x9));
  }

  tracing::SpanId getSpanId() override {
    return tracing::SpanId(0x9);
  }

 private:
  kj::Rc<TraceObservation> observation;
};

class SubrequestAccountingState final: public kj::Refcounted {
 public:
  size_t admissionCalls = 0;
  SubrequestType lastType = SubrequestType::EXTERNAL;
  bool rejectSubrequest = false;
  size_t kvAdmissionCalls = 0;
  kj::Vector<KvOperationType> kvOperations;
  KvOperationType lastKvOperation = KvOperationType::GET;
  bool rejectKvOperation = false;
  size_t observerCalls = 0;
  CountSubrequest lastCount = CountSubrequest::NO;
};

class RecordingSubrequestLimitPolicy final: public SubrequestLimitPolicy {
 public:
  explicit RecordingSubrequestLimitPolicy(kj::Rc<SubrequestAccountingState> state)
      : state(kj::mv(state)) {}

  void checkSubrequest(SubrequestType type) override {
    ++state->admissionCalls;
    state->lastType = type;
    KJ_REQUIRE(!state->rejectSubrequest, "subrequest rejected by request scope");
  }

 private:
  kj::Rc<SubrequestAccountingState> state;
};

class RecordingKvOperationLimitPolicy final: public KvOperationLimitPolicy {
 public:
  explicit RecordingKvOperationLimitPolicy(kj::Rc<SubrequestAccountingState> state)
      : state(kj::mv(state)) {}

  void checkKvOperation(KvOperationType operation) override {
    ++state->kvAdmissionCalls;
    state->kvOperations.add(operation);
    state->lastKvOperation = operation;
    KJ_REQUIRE(!state->rejectKvOperation, "KV operation rejected by request scope");
  }

 private:
  kj::Rc<SubrequestAccountingState> state;
};

class RecordingRequestObserver final: public RequestObserver {
 public:
  explicit RecordingRequestObserver(kj::Rc<SubrequestAccountingState> state)
      : state(kj::mv(state)) {}

  kj::Own<WorkerInterface> wrapSubrequestClient(
      kj::Own<WorkerInterface> client, CountSubrequest countSubrequest) override {
    ++state->observerCalls;
    state->lastCount = countSubrequest;
    return kj::mv(client);
  }

 private:
  kj::Rc<SubrequestAccountingState> state;
};

class RecordingSubrequestChannel final: public IoChannelFactory::SubrequestChannel {
 public:
  void requireAllowsTransfer() override {}

  kj::OneOf<kj::Array<byte>, kj::Promise<kj::Array<byte>>> getTokenMaybeSync(
      IoChannelFactory::ChannelTokenUsage usage) override {
    KJ_UNREACHABLE;
  }

  kj::Own<WorkerInterface> startRequest(IoChannelFactory::SubrequestMetadata metadata) override {
    ++requestsStarted;
    cfBlobJson = metadata.cfBlobJson.map([](const kj::String& value) { return kj::str(value); });
    parentContext = metadata.parentSpan.toSpanContext();
    userParentContext = metadata.userSpanParent.toSpanContext();
    return WorkerInterface::fromException(KJ_EXCEPTION(FAILED, "mock subrequest failure"));
  }

  size_t requestsStarted = 0;
  kj::Maybe<kj::String> cfBlobJson;
  kj::Maybe<tracing::SpanContext> parentContext;
  kj::Maybe<tracing::SpanContext> userParentContext;
};

class PartialThenFailInputStream final: public kj::AsyncInputStream {
 public:
  kj::Promise<size_t> tryRead(void* buffer, size_t minBytes, size_t maxBytes) override {
    KJ_REQUIRE(minBytes <= maxBytes);
    if (readAny) KJ_FAIL_REQUIRE("injected request-body read failure");

    static constexpr kj::byte bytes[] = {0, 255, 1};
    auto size = kj::min(maxBytes, sizeof(bytes));
    KJ_REQUIRE(size >= minBytes);
    std::memcpy(buffer, bytes, size);
    readAny = true;
    return size;
  }

 private:
  bool readAny = false;
};

class LuaHostOperationsWithoutRequestBody: public LuaHostOperations {
 public:
  bool hasRequestBodyStream() const override {
    return false;
  }

  void fillRandomBytes(kj::ArrayPtr<kj::byte> output) override {
    KJ_FAIL_REQUIRE("mock does not provide random bytes");
  }

  kj::String generateRandomUuid() override {
    KJ_FAIL_REQUIRE("mock does not provide random UUIDs");
  }

  kj::Promise<kj::Maybe<kj::Array<kj::byte>>> readRequestBody(size_t maxBytes) override {
    KJ_FAIL_REQUIRE("mock does not provide an inbound request body stream");
  }

  kj::Promise<LuaKvGetResponse> kvGet(
      size_t bindingIndex, kj::String key, LuaKvGetOptions options, LuaKvGetMode mode) override {
    KJ_FAIL_REQUIRE("mock does not provide KV get()", bindingIndex, key);
  }

  kj::Promise<void> kvPut(size_t bindingIndex,
      kj::String key,
      kj::Array<kj::byte> value,
      LuaKvPutOptions options) override {
    KJ_FAIL_REQUIRE("mock does not provide KV put()", bindingIndex, key);
  }

  kj::Promise<void> kvDelete(size_t bindingIndex, kj::String key) override {
    KJ_FAIL_REQUIRE("mock does not provide KV delete()", bindingIndex, key);
  }

  kj::Promise<LuaKvListResponse> kvList(size_t bindingIndex, LuaKvListOptions options) override {
    KJ_FAIL_REQUIRE("mock does not provide KV list()", bindingIndex);
  }
};

class TestLuaHostOperations final: public LuaHostOperationsWithoutRequestBody {
 public:
  bool hasRequestBodyStream() const override {
    return true;
  }

  void fillRandomBytes(kj::ArrayPtr<kj::byte> output) override {
    ++randomBytesCalls;
    for (size_t i = 0; i < output.size(); ++i) output[i] = static_cast<kj::byte>(i);
  }

  kj::String generateRandomUuid() override {
    ++randomUuidCalls;
    return kj::str("01234567-89ab-4cde-8fab-0123456789ab");
  }

  kj::Maybe<kj::String> getRequestCfJson() const override {
    return requestCfJson.map([](const kj::String& value) { return kj::str(value); });
  }

  kj::Promise<LuaKvGetResponse> kvGet(
      size_t bindingIndex, kj::String key, LuaKvGetOptions options, LuaKvGetMode mode) override {
    ++kvGetCalls;
    KJ_REQUIRE(bindingIndex == 0 && key == "lookup", "unexpected KV get request");
    KJ_REQUIRE(mode == kvGetExpectedMode, "unexpected KV get mode");
    if (kvGetCacheTtlExpected) {
      KJ_IF_SOME(cacheTtl, options.cacheTtl) {
        KJ_REQUIRE(cacheTtl == 120, "unexpected KV get cacheTtl");
      } else {
        KJ_FAIL_REQUIRE("KV get cacheTtl was not passed");
      }
    }
    if (kvGetMissing) co_return LuaKvGetResponse{.cacheStatus = kj::str("MISS")};
    if (kvGetOversizedResponse) {
      auto value = kj::heapArray<kj::byte>(LuaRuntime::MAX_KV_RESPONSE_BYTES + 1);
      co_return LuaKvGetResponse{.body = newMemoryInputStream(kj::mv(value))};
    }
    static constexpr kj::byte value[] = {0, 255, 65};
    co_return LuaKvGetResponse{
      .body = newMemoryInputStream(kj::arrayPtr(value)),
      .metadata = kj::str(R"({"source":"runtime"})"),
      .cacheStatus = kj::str("HIT"),
    };
  }

  kj::Promise<void> kvPut(size_t bindingIndex,
      kj::String key,
      kj::Array<kj::byte> value,
      LuaKvPutOptions options) override {
    ++kvPutCalls;
    KJ_REQUIRE(bindingIndex == 0 && key == "stored", "unexpected KV put request");
    KJ_REQUIRE(value.size() == 3 && value[0] == 0 && value[1] == 255 && value[2] == 65,
        "unexpected KV put value");
    if (kvPutOptionsExpected && kvPutCalls == 1) {
      KJ_IF_SOME(expirationTtl, options.expirationTtl) {
        KJ_REQUIRE(expirationTtl == 120, "unexpected KV put expirationTtl");
      } else {
        KJ_FAIL_REQUIRE("KV put expirationTtl was not passed");
      }
    } else if (kvPutOptionsExpected && kvPutCalls == 2) {
      KJ_IF_SOME(expiration, options.expiration) {
        KJ_REQUIRE(expiration == 2'000'000'000, "unexpected KV put expiration");
      } else {
        KJ_FAIL_REQUIRE("KV put expiration was not passed");
      }
    }
    co_return;
  }

  kj::Promise<void> kvDelete(size_t bindingIndex, kj::String key) override {
    ++kvDeleteCalls;
    KJ_REQUIRE(bindingIndex == 0 && key == "removed", "unexpected KV delete request");
    co_return;
  }

  kj::Promise<LuaKvListResponse> kvList(size_t bindingIndex, LuaKvListOptions options) override {
    ++kvListCalls;
    KJ_REQUIRE(bindingIndex == 0, "unexpected KV list binding");
    KJ_IF_SOME(limit, options.limit) {
      KJ_REQUIRE(limit == 2, "unexpected KV list limit");
    } else {
      KJ_FAIL_REQUIRE("KV list limit was not passed");
    }
    KJ_IF_SOME(prefix, options.prefix) {
      KJ_REQUIRE(prefix == "te", "unexpected KV list prefix");
    } else {
      KJ_FAIL_REQUIRE("KV list prefix was not passed");
    }
    KJ_IF_SOME(cursor, options.cursor) {
      KJ_REQUIRE(cursor == "token", "unexpected KV list cursor");
    } else {
      KJ_FAIL_REQUIRE("KV list cursor was not passed");
    }
    if (kvListInvalidResponse) {
      co_return LuaKvListResponse{
        .body = newMemoryInputStream(R"({"keys":[],"list_complete":"false"})"_kj),
      };
    }
    co_return LuaKvListResponse{
      .body = newMemoryInputStream(
          R"({"keys":[{"name":"test","expiration":123,"metadata":"{\"id\":1}"},{"name":"null-meta","metadata":"null"}],"list_complete":false,"cursor":"next"})"_kj),
      .cacheStatus = kj::str("HIT"),
    };
  }

  kj::Promise<LuaHttpResponse> fetch(LuaHttpRequest request) override {
    if (aiMockEnabled) {
      KJ_IF_SOME(bindingIndex, request.serviceBindingIndex) {
        KJ_REQUIRE(bindingIndex == 0, "unexpected Workers AI service binding index");
      } else {
        KJ_FAIL_REQUIRE("Workers AI request did not use its configured service binding");
      }
      KJ_REQUIRE(request.method == "POST", "Workers AI request must use POST");
      KJ_REQUIRE(request.url == "https://workers-binding.ai/run?version=3",
          "unexpected Workers AI request URL", request.url);
      aiRequestUrls.add(kj::str(request.url));
      aiRequestBodies.add(asString(request.body.asPtr()));
      for (const auto& header: request.headers) {
        aiRequestHeaders.add(LuaHttpHeader{
          .name = kj::str(header.name),
          .value = kj::str(header.value),
        });
      }
      lastRequest = kj::mv(request);
      kj::Vector<LuaHttpHeader> headers;
      headers.add(LuaHttpHeader{
        .name = kj::str("content-type"),
        .value = kj::str("application/json"),
      });
      co_return LuaHttpResponse{
        .status = aiResponseStatus,
        .headers = kj::mv(headers),
        .body = newMemoryInputStream(aiResponseJson),
      };
    }

    if (d1MockEnabled) {
      KJ_IF_SOME(bindingIndex, request.serviceBindingIndex) {
        KJ_REQUIRE(bindingIndex == 0, "unexpected D1 service binding index");
      } else {
        KJ_FAIL_REQUIRE("D1 query did not use its configured service binding");
      }
      KJ_REQUIRE(request.method == "POST", "D1 service request must use POST");
      d1RequestUrls.add(kj::str(request.url));
      d1RequestBodies.add(asString(request.body.asPtr()));
      bool execute = request.url == "http://d1/execute?resultsFormat=NONE";
      KJ_REQUIRE(execute || request.url == "http://d1/query?resultsFormat=ROWS_AND_COLUMNS",
          "unexpected D1 request URL", request.url);
      auto responseBody = newMemoryInputStream(
          execute ? kj::StringPtr(d1RunResponse) : kj::StringPtr(d1QueryResponse));
      co_return LuaHttpResponse{.status = 200, .headers = {}, .body = kj::mv(responseBody)};
    }

    if (request.bodyProducer != kj::none) {
      KJ_IF_SOME(producer, request.bodyProducer) {
        while (true) {
          KJ_IF_SOME(chunk, co_await producer->nextChunk()) {
            bodyProducerChunks.add(kj::mv(chunk));
          } else {
            bodyProducerReachedEof = true;
            break;
          }
        }
      }
      request.bodyProducer = kj::none;
    }

    lastRequest = kj::mv(request);
    auto body = newMemoryInputStream("upstream"_kj);
    kj::Vector<LuaHttpHeader> headers;
    headers.add(LuaHttpHeader{.name = kj::str("x-upstream"), .value = kj::str("yes")});
    co_return LuaHttpResponse{
      .status = 202,
      .headers = kj::mv(headers),
      .body = kj::mv(body),
    };
  }

  kj::Maybe<LuaHttpRequest> lastRequest;
  bool aiMockEnabled = false;
  uint16_t aiResponseStatus = 200;
  kj::String aiResponseJson = kj::str("");
  kj::Vector<kj::String> aiRequestUrls;
  kj::Vector<kj::String> aiRequestBodies;
  kj::Vector<LuaHttpHeader> aiRequestHeaders;
  bool d1MockEnabled = false;
  kj::String d1QueryResponse = kj::str("");
  kj::String d1RunResponse = kj::str("");
  kj::Vector<kj::String> d1RequestUrls;
  kj::Vector<kj::String> d1RequestBodies;
  kj::Vector<kj::Array<kj::byte>> bodyProducerChunks;
  bool bodyProducerReachedEof = false;
  kj::Maybe<kj::String> requestCfJson = kj::none;
  size_t randomBytesCalls = 0;
  size_t randomUuidCalls = 0;
  size_t kvGetCalls = 0;
  size_t kvPutCalls = 0;
  bool kvPutOptionsExpected = false;
  size_t kvDeleteCalls = 0;
  size_t kvListCalls = 0;
  bool kvListInvalidResponse = false;
  bool kvGetCacheTtlExpected = false;
  LuaKvGetMode kvGetExpectedMode = LuaKvGetMode::VALUE;
  bool kvGetMissing = false;
  bool kvGetOversizedResponse = false;
};

class CancelableLuaHostOperations final: public LuaHostOperationsWithoutRequestBody {
 public:
  kj::Promise<LuaHttpResponse> fetch(LuaHttpRequest request) override {
    auto pair = kj::newPromiseAndFulfiller<LuaHttpResponse>();
    fulfiller = kj::mv(pair.fulfiller);
    return kj::mv(pair.promise).attach(kj::defer([this] { canceled = true; }));
  }

  kj::Own<kj::PromiseFulfiller<LuaHttpResponse>> fulfiller;
  bool canceled = false;
};

class CancelableProducerLuaHostOperations final: public LuaHostOperationsWithoutRequestBody {
 public:
  kj::Promise<LuaHttpResponse> fetch(LuaHttpRequest request) override {
    if (request.url == "https://example.com/pending") {
      auto pair = kj::newPromiseAndFulfiller<LuaHttpResponse>();
      fulfiller = kj::mv(pair.fulfiller);
      co_return co_await kj::mv(pair.promise).attach(kj::defer([this] { cancelled = true; }));
    }

    if (request.bodyProducer != kj::none) {
      KJ_IF_SOME(producer, request.bodyProducer) {
        while (true) {
          KJ_IF_SOME(chunk, co_await producer->nextChunk()) {
            (void)chunk;
          } else {
            break;
          }
        }
      }
    }
    co_return LuaHttpResponse{
      .status = 200,
      .headers = {},
      .body = newMemoryInputStream("outer"_kj),
    };
  }

  kj::Own<kj::PromiseFulfiller<LuaHttpResponse>> fulfiller;
  bool cancelled = false;
};

class FailingLuaHostOperations final: public LuaHostOperationsWithoutRequestBody {
 public:
  kj::Promise<LuaHttpResponse> fetch(LuaHttpRequest request) override {
    KJ_FAIL_REQUIRE("mock outbound failure");
  }
};

class InterleavingLuaHostOperations final: public LuaHostOperationsWithoutRequestBody {
 public:
  kj::Promise<LuaHttpResponse> fetch(LuaHttpRequest request) override {
    auto pair = kj::newPromiseAndFulfiller<LuaHttpResponse>();
    pending.add(kj::mv(pair.fulfiller));
    return kj::mv(pair.promise);
  }

  void complete(size_t index, kj::StringPtr bodyText) {
    pending[index]->fulfill(LuaHttpResponse{
      .status = 200,
      .headers = {},
      .body = newMemoryInputStream(bodyText),
    });
    pending[index] = nullptr;
  }

  kj::Vector<kj::Own<kj::PromiseFulfiller<LuaHttpResponse>>> pending;
};

KJ_TEST("LuaJIT KV namespace methods yield through host operations") {
  auto io = kj::setupAsyncIo();
  constexpr kj::StringPtr source = R"lua(
    return {
      fetch = function()
        local value = workerd.bindings.KV.get("lookup")
        local binary = value == string.char(0, 255, 65)
        local putResult = workerd.bindings.KV.put(
          "stored", string.char(0, 255, 65), {expirationTtl = 120})
        local expirationResult = workerd.bindings.KV.put(
          "stored", string.char(0, 255, 65), {expiration = 2000000000})
        local deleteResult = workerd.bindings.KV.delete("removed")
        return { status = 200, body = tostring(binary) .. ":" .. tostring(putResult == nil) .. ":" ..
          tostring(expirationResult == nil) .. ":" .. tostring(deleteResult == nil) }
      end
    }
  )lua"_kj;

  kj::Vector<LuaBinding> bindings;
  bindings.add(LuaBinding{
    .name = kj::str("KV"),
    .value = LuaBinding::KvNamespaceBinding{.index = 0},
  });
  LuaRuntime runtime(source, io.provider->getTimer(), bindings.releaseAsArray());
  TestLuaHostOperations host;
  host.kvPutOptionsExpected = true;
  kj::HttpHeaderTable headerTable;
  kj::HttpHeaders headers(headerTable);

  auto response =
      runtime.fetch("GET"_kj, "/"_kj, headers, nullptr, host.getPtr()).wait(io.waitScope);

  KJ_EXPECT(asString(response.body) == "true:true:true:true");
  KJ_EXPECT(host.kvGetCalls == 1);
  KJ_EXPECT(host.kvPutCalls == 2);
  KJ_EXPECT(host.kvDeleteCalls == 1);
}

KJ_TEST("LuaJIT KV missing entries return nil") {
  auto io = kj::setupAsyncIo();
  constexpr kj::StringPtr source = R"lua(
    return {
      fetch = function()
        return { status = 200, body = tostring(workerd.bindings.KV.get("lookup") == nil) }
      end
    }
  )lua"_kj;

  kj::Vector<LuaBinding> bindings;
  bindings.add(LuaBinding{
    .name = kj::str("KV"),
    .value = LuaBinding::KvNamespaceBinding{.index = 0},
  });
  LuaRuntime runtime(source, io.provider->getTimer(), bindings.releaseAsArray());
  TestLuaHostOperations host;
  host.kvGetMissing = true;
  kj::HttpHeaderTable headerTable;
  kj::HttpHeaders headers(headerTable);

  auto response =
      runtime.fetch("GET"_kj, "/"_kj, headers, nullptr, host.getPtr()).wait(io.waitScope);

  KJ_EXPECT(asString(response.body) == "true");
  KJ_EXPECT(host.kvGetCalls == 1);
}

KJ_TEST("LuaJIT KV getWithMetadata returns binary values and decoded metadata") {
  auto io = kj::setupAsyncIo();
  constexpr kj::StringPtr source = R"lua(
    return {
      fetch = function()
        local result = workerd.bindings.KV.getWithMetadata("lookup", {cacheTtl = 120})
        return { status = 200, body = table.concat({
          tostring(result.value == string.char(0, 255, 65)), result.metadata.source,
          result.cacheStatus,
        }, ":") }
      end
    }
  )lua"_kj;

  kj::Vector<LuaBinding> bindings;
  bindings.add(LuaBinding{
    .name = kj::str("KV"),
    .value = LuaBinding::KvNamespaceBinding{.index = 0},
  });
  LuaRuntime runtime(source, io.provider->getTimer(), bindings.releaseAsArray());
  TestLuaHostOperations host;
  host.kvGetCacheTtlExpected = true;
  host.kvGetExpectedMode = LuaKvGetMode::WITH_METADATA;
  kj::HttpHeaderTable headerTable;
  kj::HttpHeaders headers(headerTable);

  auto response =
      runtime.fetch("GET"_kj, "/"_kj, headers, nullptr, host.getPtr()).wait(io.waitScope);

  KJ_EXPECT(asString(response.body) == "true:runtime:HIT");
  KJ_EXPECT(host.kvGetCalls == 1);
}

KJ_TEST("LuaJIT KV getWithMetadata represents missing fields explicitly") {
  auto io = kj::setupAsyncIo();
  constexpr kj::StringPtr source = R"lua(
    return {
      fetch = function()
        local result = workerd.bindings.KV.getWithMetadata("lookup")
        return { status = 200, body = table.concat({
          tostring(result.value == workerd.null), tostring(result.metadata == workerd.null),
          result.cacheStatus,
        }, ":") }
      end
    }
  )lua"_kj;

  kj::Vector<LuaBinding> bindings;
  bindings.add(LuaBinding{
    .name = kj::str("KV"),
    .value = LuaBinding::KvNamespaceBinding{.index = 0},
  });
  LuaRuntime runtime(source, io.provider->getTimer(), bindings.releaseAsArray());
  TestLuaHostOperations host;
  host.kvGetMissing = true;
  host.kvGetExpectedMode = LuaKvGetMode::WITH_METADATA;
  kj::HttpHeaderTable headerTable;
  kj::HttpHeaders headers(headerTable);

  auto response =
      runtime.fetch("GET"_kj, "/"_kj, headers, nullptr, host.getPtr()).wait(io.waitScope);

  KJ_EXPECT(asString(response.body) == "true:true:MISS");
  KJ_EXPECT(host.kvGetCalls == 1);
}

KJ_TEST("LuaJIT KV list returns decoded keys, metadata, and cache status") {
  auto io = kj::setupAsyncIo();
  constexpr kj::StringPtr source = R"lua(
    return {
      fetch = function()
        local result = workerd.bindings.KV.list({prefix = "te", limit = 2, cursor = "token"})
        return { status = 200, body = table.concat({
          tostring(result.list_complete), result.keys[1].name,
          tostring(result.keys[1].metadata.id), result.cursor, result.cacheStatus,
          tostring(result.keys[2].metadata == workerd.null),
        }, ":") }
      end
    }
  )lua"_kj;

  kj::Vector<LuaBinding> bindings;
  bindings.add(LuaBinding{
    .name = kj::str("KV"),
    .value = LuaBinding::KvNamespaceBinding{.index = 0},
  });
  LuaRuntime runtime(source, io.provider->getTimer(), bindings.releaseAsArray());
  TestLuaHostOperations host;
  kj::HttpHeaderTable headerTable;
  kj::HttpHeaders headers(headerTable);

  auto response =
      runtime.fetch("GET"_kj, "/"_kj, headers, nullptr, host.getPtr()).wait(io.waitScope);

  KJ_EXPECT(asString(response.body) == "false:test:1:next:HIT:true");
  KJ_EXPECT(host.kvListCalls == 1);
}

KJ_TEST("LuaJIT KV list rejects malformed service results") {
  auto io = kj::setupAsyncIo();
  constexpr kj::StringPtr source = R"lua(
    return {
      fetch = function()
        local succeeded = pcall(function()
          workerd.bindings.KV.list({prefix = "te", limit = 2, cursor = "token"})
        end)
        return { status = 200, body = tostring(succeeded) }
      end
    }
  )lua"_kj;

  kj::Vector<LuaBinding> bindings;
  bindings.add(LuaBinding{
    .name = kj::str("KV"),
    .value = LuaBinding::KvNamespaceBinding{.index = 0},
  });
  LuaRuntime runtime(source, io.provider->getTimer(), bindings.releaseAsArray());
  TestLuaHostOperations host;
  host.kvListInvalidResponse = true;
  kj::HttpHeaderTable headerTable;
  kj::HttpHeaders headers(headerTable);

  auto response =
      runtime.fetch("GET"_kj, "/"_kj, headers, nullptr, host.getPtr()).wait(io.waitScope);

  KJ_EXPECT(asString(response.body) == "false");
  KJ_EXPECT(host.kvListCalls == 1);
}

KJ_TEST("LuaJIT KV response bodies are bounded") {
  auto io = kj::setupAsyncIo();
  constexpr kj::StringPtr source = R"lua(
    return {
      fetch = function()
        local succeeded = pcall(function() workerd.bindings.KV.get("lookup") end)
        return { status = 200, body = tostring(succeeded) }
      end
    }
  )lua"_kj;

  kj::Vector<LuaBinding> bindings;
  bindings.add(LuaBinding{
    .name = kj::str("KV"),
    .value = LuaBinding::KvNamespaceBinding{.index = 0},
  });
  LuaRuntime runtime(source, io.provider->getTimer(), bindings.releaseAsArray());
  TestLuaHostOperations host;
  host.kvGetOversizedResponse = true;
  kj::HttpHeaderTable headerTable;
  kj::HttpHeaders headers(headerTable);

  auto response =
      runtime.fetch("GET"_kj, "/"_kj, headers, nullptr, host.getPtr()).wait(io.waitScope);

  KJ_EXPECT(asString(response.body) == "false");
  KJ_EXPECT(host.kvGetCalls == 1);
}

KJ_TEST("LuaJIT request.cf decodes per-request metadata as a fresh Lua table") {
  auto io = kj::setupAsyncIo();
  constexpr kj::StringPtr source = R"lua(
    return {
      fetch = function(request)
        if request.cf == nil then return { status = 200, body = "missing" } end
        local country = request.cf.country
        request.cf.country = "local mutation"
        return { status = 200,
          body = country .. ":" .. request.cf.features[1] .. ":" ..
            tostring(request.cf.value == workerd.null) }
      end
    }
  )lua"_kj;

  LuaRuntime runtime(source, io.provider->getTimer());
  TestLuaHostOperations host;
  host.requestCfJson = kj::str(R"({"country":"US","features":["workers","http"],"value":null})");
  kj::HttpHeaderTable headerTable;
  kj::HttpHeaders headers(headerTable);

  auto first =
      runtime.fetch("GET"_kj, "/first"_kj, headers, nullptr, host.getPtr()).wait(io.waitScope);
  auto second =
      runtime.fetch("GET"_kj, "/second"_kj, headers, nullptr, host.getPtr()).wait(io.waitScope);
  KJ_EXPECT(asString(first.body) == "US:workers:true");
  KJ_EXPECT(asString(second.body) == "US:workers:true");

  host.requestCfJson = kj::none;
  auto withoutMetadata =
      runtime.fetch("GET"_kj, "/none"_kj, headers, nullptr, host.getPtr()).wait(io.waitScope);
  KJ_EXPECT(asString(withoutMetadata.body) == "missing");
}

KJ_TEST("LuaJIT malformed request.cf metadata raises a catchable Lua error") {
  auto io = kj::setupAsyncIo();
  constexpr kj::StringPtr source = R"lua(
    return {
      fetch = function(request)
        local succeeded, value = pcall(function() return request.cf end)
        return { status = 200, body = tostring(succeeded) .. ":" .. type(value) }
      end
    }
  )lua"_kj;

  LuaRuntime runtime(source, io.provider->getTimer());
  TestLuaHostOperations host;
  host.requestCfJson = kj::str("not JSON");
  kj::HttpHeaderTable headerTable;
  kj::HttpHeaders headers(headerTable);

  auto response =
      runtime.fetch("GET"_kj, "/invalid-cf"_kj, headers, nullptr, host.getPtr()).wait(io.waitScope);
  KJ_EXPECT(asString(response.body) == "false:string");
}

KJ_TEST("LuaIoContext cancels a pending KV host operation when its invocation ends") {
  auto io = kj::setupAsyncIo();
  auto outbound = kj::refcounted<InertSubrequestChannel>();
  kj::HttpHeaderTable headerTable;
  bool canceledRequest = false;
  kj::Vector<kj::Own<IoChannelFactory::SubrequestChannel>> kvBindings;
  kvBindings.add(kj::refcounted<PendingSubrequestChannel>(canceledRequest));
  kj::Maybe<kj::Promise<LuaKvGetResponse>> pending;

  {
    auto host =
        kj::heap<LuaIoContext>(*outbound, headerTable, IoChannelFactory::SubrequestMetadata{},
            testEntropySource(), kj::heap<UnrestrictedSubrequestLimitPolicy>(),
            kj::refcounted<RequestObserver>(), nullptr, kvBindings.asPtr());
    pending = host->kvGet(0, kj::str("key"), LuaKvGetOptions{}, LuaKvGetMode::VALUE);
    KJ_EXPECT(!KJ_ASSERT_NONNULL(pending).poll(io.waitScope));
  }

  KJ_EXPECT(canceledRequest);
  KJ_IF_SOME(operation, kj::mv(pending)) {
    auto canceled = kj::mv(operation).then(
        [](LuaKvGetResponse&&) { return false; }, [](kj::Exception&& exception) {
      return exception.getType() == kj::Exception::Type::DISCONNECTED;
    });
    KJ_EXPECT(canceled.wait(io.waitScope));
  } else {
    KJ_FAIL_ASSERT("host invocation did not produce a pending KV operation");
  }
}

KJ_TEST("LuaJIT inbound request bodies stream in async bounded chunks") {
  auto io = kj::setupAsyncIo();
  constexpr kj::StringPtr source = R"lua(
    return {
      fetch = function(request)
        local first = workerd.readRequestBody(request.bodyStream, 3)
        local second = workerd.readRequestBody(request.bodyStream, 3)
        local eof = workerd.readRequestBody(request.bodyStream, 3)
        return {
          status = 200,
          body = tostring(request.body == nil) .. ":" .. first .. second .. ":" .. tostring(eof == nil),
        }
      end
    }
  )lua"_kj;

  LuaRuntime runtime(source, io.provider->getTimer());
  auto outbound = kj::refcounted<RecordingSubrequestChannel>();
  kj::HttpHeaderTable headerTable;
  kj::HttpHeaders headers(headerTable);
  LuaIoContext host(*outbound, headerTable, {}, testEntropySource(),
      kj::heap<UnrestrictedSubrequestLimitPolicy>(), kj::refcounted<RequestObserver>());
  auto pipe = kj::newOneWayPipe();
  host.setRequestBodyStream(*pipe.in);

  auto request = runtime.fetch(
      "POST"_kj, "/stream"_kj, headers, nullptr, host.getPtr(), LuaRequestBodyMode::STREAMING);
  KJ_EXPECT(!request.poll(io.waitScope));

  pipe.out->write("abc"_kjb).wait(io.waitScope);
  KJ_EXPECT(!request.poll(io.waitScope));

  pipe.out->write("def"_kjb).wait(io.waitScope);
  KJ_EXPECT(!request.poll(io.waitScope));
  pipe.out = nullptr;

  auto response = request.wait(io.waitScope);
  host.clearRequestBodyStream();
  KJ_EXPECT(asString(response.body) == "true:abcdef:true");
}

KJ_TEST("LuaJIT request-body chunks preserve binary bytes") {
  auto io = kj::setupAsyncIo();
  constexpr kj::StringPtr source = R"lua(
    return {
      fetch = function(request)
        return { status = 200, body = workerd.readRequestBody(request.bodyStream, 3) }
      end
    }
  )lua"_kj;

  LuaRuntime runtime(source, io.provider->getTimer());
  auto outbound = kj::refcounted<RecordingSubrequestChannel>();
  kj::HttpHeaderTable headerTable;
  kj::HttpHeaders headers(headerTable);
  LuaIoContext host(*outbound, headerTable, {}, testEntropySource(),
      kj::heap<UnrestrictedSubrequestLimitPolicy>(), kj::refcounted<RequestObserver>());
  auto body = kj::heapArray<kj::byte>(3);
  body[0] = 0;
  body[1] = 255;
  body[2] = 1;
  auto input = newMemoryInputStream(body.asPtr());
  host.setRequestBodyStream(*input);

  auto response = runtime
                      .fetch("POST"_kj, "/binary"_kj, headers, nullptr, host.getPtr(),
                          LuaRequestBodyMode::STREAMING)
                      .wait(io.waitScope);
  host.clearRequestBodyStream();
  KJ_REQUIRE(response.body.size() == 3);
  KJ_EXPECT(response.body[0] == 0);
  KJ_EXPECT(response.body[1] == 255);
  KJ_EXPECT(response.body[2] == 1);
}

KJ_TEST("LuaJIT request-body read errors preserve earlier chunks") {
  auto io = kj::setupAsyncIo();
  constexpr kj::StringPtr source = R"lua(
    return {
      fetch = function(request)
        local first = workerd.readRequestBody(request.bodyStream, 3)
        local succeeded = pcall(function()
          workerd.readRequestBody(request.bodyStream, 3)
        end)
        return { status = 200, body = tostring(succeeded) .. ":" .. first }
      end
    }
  )lua"_kj;

  LuaRuntime runtime(source, io.provider->getTimer());
  auto outbound = kj::refcounted<RecordingSubrequestChannel>();
  kj::HttpHeaderTable headerTable;
  kj::HttpHeaders headers(headerTable);
  LuaIoContext host(*outbound, headerTable, {}, testEntropySource(),
      kj::heap<UnrestrictedSubrequestLimitPolicy>(), kj::refcounted<RequestObserver>());
  auto input = kj::heap<PartialThenFailInputStream>();
  host.setRequestBodyStream(*input);

  auto response = runtime
                      .fetch("POST"_kj, "/partial-error"_kj, headers, nullptr, host.getPtr(),
                          LuaRequestBodyMode::STREAMING)
                      .wait(io.waitScope);
  host.clearRequestBodyStream();
  KJ_REQUIRE(response.body.size() == 9);
  KJ_EXPECT(asString(response.body.asPtr().slice(0, 6)) == "false:");
  KJ_EXPECT(response.body[6] == 0);
  KJ_EXPECT(response.body[7] == 255);
  KJ_EXPECT(response.body[8] == 1);
}

KJ_TEST("LuaJIT cancels a pending inbound request-body read") {
  auto io = kj::setupAsyncIo();
  constexpr kj::StringPtr source = R"lua(
    return {
      fetch = function(request)
        if request.url == "/next" then return { status = 200, body = "reused" } end
        workerd.readRequestBody(request.bodyStream, 1)
        return { status = 200, body = "unexpected" }
      end
    }
  )lua"_kj;

  LuaRuntime runtime(source, io.provider->getTimer());
  auto outbound = kj::refcounted<RecordingSubrequestChannel>();
  kj::HttpHeaderTable headerTable;
  kj::HttpHeaders headers(headerTable);
  LuaIoContext host(*outbound, headerTable, {}, testEntropySource(),
      kj::heap<UnrestrictedSubrequestLimitPolicy>(), kj::refcounted<RequestObserver>());
  auto pipe = kj::newOneWayPipe();
  host.setRequestBodyStream(*pipe.in);

  auto pending = runtime.fetch(
      "POST"_kj, "/pending"_kj, headers, nullptr, host.getPtr(), LuaRequestBodyMode::STREAMING);
  KJ_EXPECT(!pending.poll(io.waitScope));
  pending = nullptr;
  host.clearRequestBodyStream();

  host.setRequestBodyStream(*pipe.in);
  auto next = runtime
                  .fetch("POST"_kj, "/next"_kj, headers, nullptr, host.getPtr(),
                      LuaRequestBodyMode::STREAMING)
                  .wait(io.waitScope);
  host.clearRequestBodyStream();
  KJ_EXPECT(asString(next.body) == "reused");
}

KJ_TEST("LuaJIT request-body stream handles are request-scoped") {
  auto io = kj::setupAsyncIo();
  constexpr kj::StringPtr source = R"lua(
    local previousBody
    return {
      fetch = function(request)
        if previousBody ~= nil then
          local succeeded = pcall(function()
            workerd.readRequestBody(previousBody, 1)
          end)
          return { status = 200, body = tostring(succeeded) }
        end
        previousBody = request.bodyStream
        return { status = 200, body = "stored" }
      end
    }
  )lua"_kj;

  LuaRuntime runtime(source, io.provider->getTimer());
  auto outbound = kj::refcounted<RecordingSubrequestChannel>();
  kj::HttpHeaderTable headerTable;
  kj::HttpHeaders headers(headerTable);
  {
    LuaIoContext firstHost(*outbound, headerTable, {}, testEntropySource(),
        kj::heap<UnrestrictedSubrequestLimitPolicy>(), kj::refcounted<RequestObserver>());
    auto firstBody = newMemoryInputStream("first"_kj);
    firstHost.setRequestBodyStream(*firstBody);
    auto first = runtime
                     .fetch("POST"_kj, "/first"_kj, headers, nullptr, firstHost.getPtr(),
                         LuaRequestBodyMode::STREAMING)
                     .wait(io.waitScope);
    firstHost.clearRequestBodyStream();
    KJ_EXPECT(asString(first.body) == "stored");
  }
  {
    LuaIoContext secondHost(*outbound, headerTable, {}, testEntropySource(),
        kj::heap<UnrestrictedSubrequestLimitPolicy>(), kj::refcounted<RequestObserver>());
    auto secondBody = newMemoryInputStream("second"_kj);
    secondHost.setRequestBodyStream(*secondBody);
    auto second = runtime
                      .fetch("POST"_kj, "/second"_kj, headers, nullptr, secondHost.getPtr(),
                          LuaRequestBodyMode::STREAMING)
                      .wait(io.waitScope);
    secondHost.clearRequestBodyStream();
    KJ_EXPECT(asString(second.body) == "false");
  }
}

KJ_TEST("LuaJIT streamed request bodies keep the total body limit") {
  auto io = kj::setupAsyncIo();
  constexpr kj::StringPtr source = R"lua(
    return {
      fetch = function(request)
        while workerd.readRequestBody(request.bodyStream, 65536) ~= nil do end
        return { status = 200, body = "unexpected" }
      end
    }
  )lua"_kj;

  LuaRuntime runtime(source, io.provider->getTimer());
  auto outbound = kj::refcounted<RecordingSubrequestChannel>();
  kj::HttpHeaderTable headerTable;
  kj::HttpHeaders headers(headerTable);
  LuaIoContext host(*outbound, headerTable, {}, testEntropySource(),
      kj::heap<UnrestrictedSubrequestLimitPolicy>(), kj::refcounted<RequestObserver>());
  auto body = kj::heapArray<kj::byte>(LuaRuntime::MAX_HTTP_BODY_BYTES + 1);
  std::memset(body.begin(), 'x', body.size());
  auto input = newMemoryInputStream(body.asPtr());
  host.setRequestBodyStream(*input);

  KJ_EXPECT(kj::runCatchingExceptions([&]() {
    (void)runtime
        .fetch("POST"_kj, "/too-large"_kj, headers, nullptr, host.getPtr(),
            LuaRequestBodyMode::STREAMING)
        .wait(io.waitScope);
  }) != kj::none);
  host.clearRequestBodyStream();
}

KJ_TEST("LuaJIT fetch forwards a request-scoped body stream handle") {
  auto io = kj::setupAsyncIo();
  constexpr kj::StringPtr source = R"lua(
    return {
      fetch = function(request)
        local response = workerd.fetch({
          method = "POST",
          url = "https://example.com/upload",
          body = request.bodyStream,
        })
        return { status = response.status, body = workerd.readAll(response.body) }
      end
    }
  )lua"_kj;

  LuaRuntime runtime(source, io.provider->getTimer());
  TestLuaHostOperations hostOperations;
  kj::HttpHeaderTable headerTable;
  kj::HttpHeaders headers(headerTable);
  auto response = runtime
                      .fetch("POST"_kj, "/upload"_kj, headers, nullptr, hostOperations.getPtr(),
                          LuaRequestBodyMode::STREAMING)
                      .wait(io.waitScope);

  KJ_EXPECT(response.status == 202);
  KJ_EXPECT(asString(response.body) == "upstream");
  KJ_IF_SOME(request, hostOperations.lastRequest) {
    KJ_REQUIRE(request.body.size() == 0);
    KJ_IF_SOME(requestId, request.bodyStreamRequestId) {
      KJ_EXPECT(requestId == "1");
    } else {
      KJ_FAIL_ASSERT("inbound body stream handle was not forwarded");
    }
  } else {
    KJ_FAIL_ASSERT("host fetch was not called");
  }
}

KJ_TEST("LuaJIT fetch body producers yield bounded binary chunks on demand") {
  auto io = kj::setupAsyncIo();
  constexpr kj::StringPtr source = R"lua(
    local index = 0
    local producerCoroutine
    local function nextChunk()
      local currentCoroutine = coroutine.running()
      if producerCoroutine == nil then
        producerCoroutine = currentCoroutine
      else
        assert(producerCoroutine == currentCoroutine, "producer coroutine changed between chunks")
      end
      index = index + 1
      if index == 1 then return "abc" end
      if index == 2 then return string.char(0, 255) end
      return nil
    end
    return {
      fetch = function()
        local response = workerd.fetch({
          method = "POST",
          url = "https://example.com/upload",
          body = nextChunk,
        })
        return { status = response.status, body = workerd.readAll(response.body) }
      end
    }
  )lua"_kj;

  LuaRuntime runtime(source, io.provider->getTimer());
  TestLuaHostOperations hostOperations;
  kj::HttpHeaderTable headerTable;
  kj::HttpHeaders headers(headerTable);
  auto response = runtime.fetch("GET"_kj, "/upload"_kj, headers, nullptr, hostOperations.getPtr())
                      .wait(io.waitScope);
  KJ_EXPECT(asString(response.body) == "upstream");
  KJ_REQUIRE(hostOperations.bodyProducerReachedEof);
  KJ_REQUIRE(hostOperations.bodyProducerChunks.size() == 2);
  KJ_EXPECT(asString(hostOperations.bodyProducerChunks[0]) == "abc");
  KJ_REQUIRE(hostOperations.bodyProducerChunks[1].size() == 2);
  KJ_EXPECT(hostOperations.bodyProducerChunks[1][0] == 0);
  KJ_EXPECT(hostOperations.bodyProducerChunks[1][1] == 255);
  KJ_IF_SOME(request, hostOperations.lastRequest) {
    KJ_EXPECT(request.bodyProducer == kj::none);
  } else {
    KJ_FAIL_ASSERT("host fetch was not called");
  }
}

KJ_TEST("LuaJIT request body producers yield for host operations") {
  auto io = kj::setupAsyncIo();
  constexpr kj::StringPtr source = R"lua(
    local produced = false
    local function nextChunk()
      if produced then return nil end
      produced = true
      workerd.sleep(1)
      local source = workerd.fetch({ url = "https://example.com/source" })
      return workerd.readAll(source.body)
    end
    return {
      fetch = function()
        local response = workerd.fetch({
          method = "POST",
          url = "https://example.com/upload",
          body = nextChunk,
        })
        return { status = response.status, body = workerd.readAll(response.body) }
      end
    }
  )lua"_kj;

  LuaRuntime runtime(source, io.provider->getTimer());
  TestLuaHostOperations hostOperations;
  kj::HttpHeaderTable headerTable;
  kj::HttpHeaders headers(headerTable);
  auto response = runtime.fetch("GET"_kj, "/upload"_kj, headers, nullptr, hostOperations.getPtr())
                      .wait(io.waitScope);

  KJ_EXPECT(asString(response.body) == "upstream");
  KJ_REQUIRE(hostOperations.bodyProducerReachedEof);
  KJ_REQUIRE(hostOperations.bodyProducerChunks.size() == 1);
  KJ_EXPECT(asString(hostOperations.bodyProducerChunks[0]) == "upstream");
}

KJ_TEST("LuaJIT cancellation tears down an asynchronous body producer") {
  auto io = kj::setupAsyncIo();
  constexpr kj::StringPtr source = R"lua(
    return {
      fetch = function()
        local response = workerd.fetch({
          method = "POST",
          url = "https://example.com/upload",
          body = function()
            local nested = workerd.fetch({ url = "https://example.com/pending" })
            return workerd.readAll(nested.body)
          end,
        })
        return { status = response.status, body = "unexpected" }
      end
    }
  )lua"_kj;

  LuaRuntime runtime(source, io.provider->getTimer());
  CancelableProducerLuaHostOperations hostOperations;
  kj::HttpHeaderTable headerTable;
  kj::HttpHeaders headers(headerTable);
  auto request = runtime.fetch("GET"_kj, "/upload"_kj, headers, nullptr, hostOperations.getPtr());
  KJ_EXPECT(!request.poll(io.waitScope));
  KJ_REQUIRE(hostOperations.fulfiller.get() != nullptr);
  request = nullptr;
  KJ_EXPECT(hostOperations.cancelled);
}

KJ_TEST("LuaJIT request body producers reject oversized chunks") {
  auto io = kj::setupAsyncIo();
  constexpr kj::StringPtr source = R"lua(
    return {
      fetch = function()
        workerd.fetch({
          method = "POST",
          url = "https://example.com/upload",
          body = function() return string.rep("x", 65537) end,
        })
        return { status = 200, body = "unexpected" }
      end
    }
  )lua"_kj;

  LuaRuntime runtime(source, io.provider->getTimer());
  TestLuaHostOperations hostOperations;
  kj::HttpHeaderTable headerTable;
  kj::HttpHeaders headers(headerTable);
  KJ_EXPECT(kj::runCatchingExceptions([&]() {
    (void)runtime.fetch("GET"_kj, "/upload"_kj, headers, nullptr, hostOperations.getPtr())
        .wait(io.waitScope);
  }) != kj::none);
  KJ_EXPECT(hostOperations.bodyProducerChunks.size() == 0);
}

KJ_TEST("LuaJIT request body producers enforce the total upload limit") {
  auto io = kj::setupAsyncIo();
  constexpr kj::StringPtr source = R"lua(
    local index = 0
    return {
      fetch = function()
        workerd.fetch({
          method = "POST",
          url = "https://example.com/upload",
          body = function()
            index = index + 1
            if index <= 17 then return string.rep("x", 65536) end
          end,
        })
        return { status = 200, body = "unexpected" }
      end
    }
  )lua"_kj;

  LuaRuntime runtime(source, io.provider->getTimer());
  TestLuaHostOperations hostOperations;
  kj::HttpHeaderTable headerTable;
  kj::HttpHeaders headers(headerTable);
  KJ_EXPECT(kj::runCatchingExceptions([&]() {
    (void)runtime.fetch("GET"_kj, "/upload"_kj, headers, nullptr, hostOperations.getPtr())
        .wait(io.waitScope);
  }) != kj::none);
  KJ_REQUIRE(hostOperations.bodyProducerChunks.size() == 16);
  size_t bodyBytes = 0;
  for (auto& chunk: hostOperations.bodyProducerChunks) bodyBytes += chunk.size();
  KJ_EXPECT(bodyBytes == LuaRuntime::MAX_HTTP_BODY_BYTES);
}

KJ_TEST("LuaJIT request body producer errors are catchable in Lua") {
  auto io = kj::setupAsyncIo();
  constexpr kj::StringPtr source = R"lua(
    return {
      fetch = function()
        local succeeded, message = pcall(function()
          workerd.fetch({
            method = "POST",
            url = "https://example.com/upload",
            body = function() error("generator failed") end,
          })
        end)
        return { status = 200, body = tostring(succeeded) .. ":" .. message }
      end
    }
  )lua"_kj;

  LuaRuntime runtime(source, io.provider->getTimer());
  TestLuaHostOperations hostOperations;
  kj::HttpHeaderTable headerTable;
  kj::HttpHeaders headers(headerTable);
  auto response = runtime.fetch("GET"_kj, "/upload"_kj, headers, nullptr, hostOperations.getPtr())
                      .wait(io.waitScope);
  auto bodyText = asString(response.body);
  KJ_EXPECT(bodyText.startsWith("false:"));
  KJ_EXPECT(bodyText.contains("generator failed"));
}

KJ_TEST("LuaJIT outbound fetch uses runtime-neutral host operations") {
  auto io = kj::setupAsyncIo();
  constexpr kj::StringPtr source = R"lua(
    return {
      fetch = function(request)
        local response = workerd.fetch({
          method = "POST",
          url = "https://example.com/upstream",
          headers = {{ name = "x-request", value = "yes" }},
          body = request.body,
        })
        return {
          status = response.status,
          headers = response.headers,
          body = workerd.readAll(response.body),
        }
      end
    }
  )lua"_kj;

  LuaRuntime runtime(source, io.provider->getTimer());
  TestLuaHostOperations hostOperations;
  kj::HttpHeaderTable headerTable;
  kj::HttpHeaders headers(headerTable);
  auto requestBody = kj::heapArray<kj::byte>(7);
  std::memcpy(requestBody.begin(), "request", 7);

  auto response = runtime.fetch("GET"_kj, "/"_kj, headers, requestBody, hostOperations.getPtr())
                      .wait(io.waitScope);

  KJ_EXPECT(response.status == 202);
  KJ_EXPECT(asString(response.body) == "upstream");
  KJ_REQUIRE(response.headers.size() == 1);
  KJ_EXPECT(response.headers[0].name == "x-upstream");
  KJ_EXPECT(response.headers[0].value == "yes");

  KJ_IF_SOME(request, hostOperations.lastRequest) {
    KJ_EXPECT(request.method == "POST");
    KJ_EXPECT(request.url == "https://example.com/upstream");
    KJ_EXPECT(asString(request.body) == "request");
    KJ_REQUIRE(request.headers.size() == 1);
    KJ_EXPECT(request.headers[0].name == "x-request");
    KJ_EXPECT(request.headers[0].value == "yes");
  } else {
    KJ_FAIL_ASSERT("host fetch was not called");
  }
}

KJ_TEST("LuaJIT cancels a suspended outbound operation with its request") {
  auto io = kj::setupAsyncIo();
  constexpr kj::StringPtr source = R"lua(
    return {
      fetch = function(request)
        if request.url == "/fast" then
          return { status = 200, body = "reused" }
        end
        workerd.fetch({ url = "https://example.com/pending" })
        return { status = 200, body = "unexpected" }
      end
    }
  )lua"_kj;

  LuaRuntime runtime(source, io.provider->getTimer());
  CancelableLuaHostOperations hostOperations;
  kj::HttpHeaderTable headerTable;
  kj::HttpHeaders headers(headerTable);
  auto request = runtime.fetch("GET"_kj, "/pending"_kj, headers, nullptr, hostOperations.getPtr());

  KJ_EXPECT(!request.poll(io.waitScope));
  request = nullptr;
  KJ_EXPECT(hostOperations.canceled);

  auto nextRequest = runtime.fetch("GET"_kj, "/fast"_kj, headers, nullptr, hostOperations.getPtr())
                         .wait(io.waitScope);
  KJ_EXPECT(asString(nextRequest.body) == "reused");
}

KJ_TEST("LuaJIT outbound fetch failures become catchable Lua errors") {
  auto io = kj::setupAsyncIo();
  constexpr kj::StringPtr source = R"lua(
    return {
      fetch = function()
        local succeeded, message = pcall(function()
          workerd.fetch({ url = "https://example.com/failure" })
        end)
        return { status = 200, body = tostring(succeeded) .. ":" .. message }
      end
    }
  )lua"_kj;

  LuaRuntime runtime(source, io.provider->getTimer());
  FailingLuaHostOperations hostOperations;
  kj::HttpHeaderTable headerTable;
  kj::HttpHeaders headers(headerTable);
  auto response =
      runtime.fetch("GET"_kj, "/"_kj, headers, nullptr, hostOperations.getPtr()).wait(io.waitScope);

  KJ_EXPECT(asString(response.body).startsWith("false:"));
  KJ_EXPECT(asString(response.body).contains("mock outbound failure"));
}

KJ_TEST("LuaJIT malformed outbound fetch input becomes a catchable Lua error") {
  auto io = kj::setupAsyncIo();
  constexpr kj::StringPtr source = R"lua(
    return {
      fetch = function()
        local succeeded, message = pcall(function()
          workerd.fetch({ url = 7 })
        end)
        return { status = 200, body = tostring(succeeded) .. ":" .. message }
      end
    }
  )lua"_kj;

  LuaRuntime runtime(source, io.provider->getTimer());
  kj::HttpHeaderTable headerTable;
  kj::HttpHeaders headers(headerTable);
  auto response = runtime.fetch("GET"_kj, "/"_kj, headers, nullptr).wait(io.waitScope);

  auto bodyText = asString(response.body);
  KJ_EXPECT(bodyText.startsWith("false:"));
}

KJ_TEST("LuaJIT response-body reads reject invalid handles and chunk sizes") {
  auto io = kj::setupAsyncIo();
  constexpr kj::StringPtr source = R"lua(
    return {
      fetch = function()
        local validHandle = pcall(workerd.readBody, "not a stream")
        local validChunk = pcall(workerd.readBody, { streamId = 1 }, 65537)
        local validCancel = pcall(workerd.cancelBody, "not a stream")
        return {
          status = 200,
          body = tostring(validHandle) .. ":" .. tostring(validChunk) .. ":" .. tostring(validCancel),
        }
      end
    }
  )lua"_kj;

  LuaRuntime runtime(source, io.provider->getTimer());
  kj::HttpHeaderTable headerTable;
  kj::HttpHeaders headers(headerTable);
  auto response = runtime.fetch("GET"_kj, "/"_kj, headers, nullptr).wait(io.waitScope);
  KJ_EXPECT(asString(response.body) == "false:false:false");
}

KJ_TEST("LuaJIT response body handles cannot cross request boundaries") {
  auto io = kj::setupAsyncIo();
  constexpr kj::StringPtr source = R"lua(
    local savedBody
    return {
      fetch = function(request)
        if request.url == "/first" then
          savedBody = workerd.fetch({ url = "https://example.com/first" }).body
          return { status = 200, body = "stored" }
        end
        local response = workerd.fetch({ url = "https://example.com/second" })
        local succeeded = pcall(workerd.readBody, savedBody, 1)
        workerd.cancelBody(response.body)
        return { status = 200, body = tostring(succeeded) }
      end
    }
  )lua"_kj;

  LuaRuntime runtime(source, io.provider->getTimer());
  TestLuaHostOperations hostOperations;
  kj::HttpHeaderTable headerTable;
  kj::HttpHeaders headers(headerTable);

  auto first = runtime.fetch("GET"_kj, "/first"_kj, headers, nullptr, hostOperations.getPtr())
                   .wait(io.waitScope);
  auto second = runtime.fetch("GET"_kj, "/second"_kj, headers, nullptr, hostOperations.getPtr())
                    .wait(io.waitScope);
  KJ_EXPECT(asString(first.body) == "stored");
  KJ_EXPECT(asString(second.body) == "false");
}

KJ_TEST("LuaJIT outbound fetch responses stay with their suspended request") {
  auto io = kj::setupAsyncIo();
  constexpr kj::StringPtr source = R"lua(
    return {
      fetch = function(request)
        local response = workerd.fetch({ url = "https://example.com/" .. request.url })
        return { status = 200, body = workerd.readAll(response.body) }
      end
    }
  )lua"_kj;

  LuaRuntime runtime(source, io.provider->getTimer());
  InterleavingLuaHostOperations hostOperations;
  kj::HttpHeaderTable headerTable;
  kj::HttpHeaders headers(headerTable);
  auto first = runtime.fetch("GET"_kj, "/first"_kj, headers, nullptr, hostOperations.getPtr());
  auto second = runtime.fetch("GET"_kj, "/second"_kj, headers, nullptr, hostOperations.getPtr());

  KJ_REQUIRE(hostOperations.pending.size() == 2);
  KJ_EXPECT(!first.poll(io.waitScope));
  KJ_EXPECT(!second.poll(io.waitScope));

  hostOperations.complete(1, "second response");
  auto secondResult = second.wait(io.waitScope);
  KJ_EXPECT(asString(secondResult.body) == "second response");
  KJ_EXPECT(!first.poll(io.waitScope));

  hostOperations.complete(0, "first response");
  auto firstResult = first.wait(io.waitScope);
  KJ_EXPECT(asString(firstResult.body) == "first response");
}

KJ_TEST("LuaJIT can issue multiple outbound fetches in one request") {
  auto io = kj::setupAsyncIo();
  constexpr kj::StringPtr source = R"lua(
    return {
      fetch = function()
        local first = workerd.fetch({ url = "https://example.com/first" })
        local second = workerd.fetch({ url = "https://example.com/second" })
        return { status = 200,
          body = workerd.readAll(first.body) .. ":" .. workerd.readAll(second.body) }
      end
    }
  )lua"_kj;

  LuaRuntime runtime(source, io.provider->getTimer());
  InterleavingLuaHostOperations hostOperations;
  kj::HttpHeaderTable headerTable;
  kj::HttpHeaders headers(headerTable);
  auto request = runtime.fetch("GET"_kj, "/"_kj, headers, nullptr, hostOperations.getPtr());

  KJ_EXPECT(!request.poll(io.waitScope));
  KJ_REQUIRE(hostOperations.pending.size() == 1);
  hostOperations.complete(0, "first");
  KJ_EXPECT(!request.poll(io.waitScope));
  KJ_REQUIRE(hostOperations.pending.size() == 2);
  hostOperations.complete(1, "second");
  auto response = request.wait(io.waitScope);
  KJ_EXPECT(asString(response.body) == "first:second");
}

KJ_TEST("LuaJIT outbound fetch propagates request trace context") {
  auto io = kj::setupAsyncIo();
  constexpr kj::StringPtr source = R"lua(
    return {
      fetch = function()
        local succeeded = pcall(function()
          workerd.fetch({ url = "https://example.com/trace" })
        end)
        return { status = 200, body = tostring(succeeded) }
      end
    }
  )lua"_kj;

  LuaRuntime runtime(source, io.provider->getTimer());
  auto observation = kj::rc<TraceObservation>();
  auto spanObserver = kj::rc<RecordingSpanObserver>(observation.addRef());
  auto outbound = kj::refcounted<RecordingSubrequestChannel>();
  auto accounting = kj::rc<SubrequestAccountingState>();
  kj::HttpHeaderTable headerTable;
  kj::HttpHeaders headers(headerTable);
  IoChannelFactory::SubrequestMetadata metadata;
  metadata.cfBlobJson = kj::str("request-cf-blob");
  metadata.parentSpan = SpanParent(spanObserver.addRef());
  metadata.userSpanParent = SpanParent(spanObserver.addRef());
  LuaIoContext host(*outbound, headerTable, kj::mv(metadata), testEntropySource(),
      kj::heap<RecordingSubrequestLimitPolicy>(accounting.addRef()),
      kj::refcounted<RecordingRequestObserver>(accounting.addRef()));

  auto response =
      runtime.fetch("GET"_kj, "/"_kj, headers, nullptr, host.getPtr()).wait(io.waitScope);

  KJ_EXPECT(asString(response.body) == "false");
  KJ_IF_SOME(cfBlobJson, outbound->cfBlobJson) {
    KJ_EXPECT(cfBlobJson == "request-cf-blob");
  } else {
    KJ_FAIL_ASSERT("outbound request must preserve cfBlobJson");
  }
  KJ_REQUIRE(outbound->parentContext != kj::none);
  KJ_REQUIRE(outbound->userParentContext != kj::none);
  KJ_EXPECT(
      KJ_ASSERT_NONNULL(outbound->parentContext).getTraceId() == tracing::TraceId(0x1234, 0x5678));
  KJ_EXPECT(KJ_ASSERT_NONNULL(outbound->parentContext).getSpanId() == tracing::SpanId(0x9));
  KJ_EXPECT(KJ_ASSERT_NONNULL(outbound->userParentContext).getTraceId() ==
      tracing::TraceId(0x1234, 0x5678));
  KJ_EXPECT(observation->operationNames.size() == 2);
  KJ_EXPECT(accounting->admissionCalls == 1);
  KJ_EXPECT(accounting->lastType == SubrequestType::EXTERNAL);
  KJ_EXPECT(accounting->observerCalls == 1);
  KJ_EXPECT(accounting->lastCount == CountSubrequest::YES);
  KJ_EXPECT(outbound->requestsStarted == 1);
  KJ_EXPECT(observation->operationNames[0] == "lua.fetch");
  KJ_EXPECT(observation->operationNames[1] == "lua.fetch");
}

KJ_TEST("LuaJIT service-binding requests preserve subrequest accounting") {
  auto io = kj::setupAsyncIo();
  constexpr kj::StringPtr source = R"lua(
    return {
      fetch = function()
        local succeeded = pcall(function()
          workerd.bindings.TARGET.fetch({ url = "https://target/path" })
        end)
        return { status = 200, body = tostring(succeeded) }
      end
    }
  )lua"_kj;

  kj::Vector<LuaBinding> bindings;
  bindings.add(LuaBinding{
    .name = kj::str("TARGET"),
    .value = LuaBinding::ServiceBinding{.index = 0},
  });
  LuaRuntime runtime(source, io.provider->getTimer(), bindings.releaseAsArray());
  auto outbound = kj::refcounted<RecordingSubrequestChannel>();
  auto serviceChannel = kj::refcounted<RecordingSubrequestChannel>();
  auto* serviceChannelPtr = serviceChannel.get();
  kj::Vector<kj::Own<IoChannelFactory::SubrequestChannel>> serviceChannels;
  serviceChannels.add(kj::addRef(*serviceChannel));
  auto accounting = kj::rc<SubrequestAccountingState>();
  kj::HttpHeaderTable headerTable;
  kj::HttpHeaders headers(headerTable);
  LuaIoContext host(*outbound, headerTable, {}, testEntropySource(),
      kj::heap<RecordingSubrequestLimitPolicy>(accounting.addRef()),
      kj::refcounted<RecordingRequestObserver>(accounting.addRef()), serviceChannels.asPtr());

  auto response =
      runtime.fetch("GET"_kj, "/"_kj, headers, nullptr, host.getPtr()).wait(io.waitScope);

  KJ_EXPECT(asString(response.body) == "false");
  KJ_EXPECT(outbound->requestsStarted == 0);
  KJ_EXPECT(serviceChannelPtr->requestsStarted == 1);
  KJ_EXPECT(accounting->admissionCalls == 1);
  KJ_EXPECT(accounting->lastType == SubrequestType::EXTERNAL);
  KJ_EXPECT(accounting->observerCalls == 1);
  KJ_EXPECT(accounting->lastCount == CountSubrequest::YES);
}

KJ_TEST("LuaJIT KV bindings preserve operation and in-house subrequest accounting") {
  auto io = kj::setupAsyncIo();
  constexpr kj::StringPtr source = R"lua(
    return {
      fetch = function()
        local getFailure = pcall(function() workerd.bindings.KV.get("key") end)
        local putFailure = pcall(function() workerd.bindings.KV.put("key", "value") end)
        local deleteFailure = pcall(function() workerd.bindings.KV.delete("key") end)
        local getWithMetadataFailure = pcall(function()
          workerd.bindings.KV.getWithMetadata("key")
        end)
        local listFailure = pcall(function() workerd.bindings.KV.list() end)
        local invalidListLimit = pcall(function() workerd.bindings.KV.list({limit = 1001}) end)
        local invalidPrefix = pcall(function()
          workerd.bindings.KV.list({prefix = string.rep("p", 513)})
        end)
        local invalidCursor = pcall(function()
          workerd.bindings.KV.list({cursor = string.rep("c", 4097)})
        end)
        local invalidListOptions = pcall(function() workerd.bindings.KV.list("bad") end)
        local unsupportedListOption = pcall(function()
          workerd.bindings.KV.list({cacheTtl = 60})
        end)
        local invalidGetCacheTtl = pcall(function()
          workerd.bindings.KV.get("key", {cacheTtl = 59})
        end)
        local invalidGetCacheTtlType = pcall(function()
          workerd.bindings.KV.get("key", {cacheTtl = 60.5})
        end)
        local unsupportedGetOption = pcall(function()
          workerd.bindings.KV.get("key", {type = "json"})
        end)
        local invalidKey = pcall(function() workerd.bindings.KV.get("") end)
        local invalidSpecialKey = pcall(function() workerd.bindings.KV.get("..") end)
        local oversizedKey = pcall(function() workerd.bindings.KV.get(string.rep("k", 513)) end)
        local oversizedValue = pcall(function()
          workerd.bindings.KV.put("key", string.rep("v", 1024 * 1024 + 1))
        end)
        local invalidPutExpirationTtl = pcall(function()
          workerd.bindings.KV.put("key", "value", {expirationTtl = 59})
        end)
        local invalidPutExpirationTtlType = pcall(function()
          workerd.bindings.KV.put("key", "value", {expirationTtl = 60.5})
        end)
        local invalidPutExpiration = pcall(function()
          workerd.bindings.KV.put("key", "value", {expiration = -1})
        end)
        local invalidPutExpirationType = pcall(function()
          workerd.bindings.KV.put("key", "value", {expiration = 1.5})
        end)
        local unsupportedPutOptions = pcall(function()
          workerd.bindings.KV.put("key", "value", {metadata = {id = 1}})
        end)
        return { status = 200, body = table.concat({tostring(getFailure), tostring(putFailure),
          tostring(deleteFailure), tostring(getWithMetadataFailure), tostring(listFailure),
          tostring(invalidListLimit),
          tostring(invalidPrefix), tostring(invalidCursor), tostring(invalidListOptions),
          tostring(unsupportedListOption), tostring(invalidGetCacheTtl),
          tostring(invalidGetCacheTtlType), tostring(unsupportedGetOption),
          tostring(invalidPutExpirationTtl), tostring(invalidPutExpirationTtlType),
          tostring(invalidPutExpiration), tostring(invalidPutExpirationType),
          tostring(invalidKey), tostring(invalidSpecialKey), tostring(oversizedKey),
          tostring(oversizedValue), tostring(unsupportedPutOptions)}, ":") }
      end
    }
  )lua"_kj;

  kj::Vector<LuaBinding> bindings;
  bindings.add(LuaBinding{
    .name = kj::str("KV"),
    .value = LuaBinding::KvNamespaceBinding{.index = 0},
  });
  LuaRuntime runtime(source, io.provider->getTimer(), bindings.releaseAsArray());
  auto outbound = kj::refcounted<RecordingSubrequestChannel>();
  auto kvChannel = kj::refcounted<RecordingSubrequestChannel>();
  auto* kvChannelPtr = kvChannel.get();
  kj::Vector<kj::Own<IoChannelFactory::SubrequestChannel>> kvChannels;
  kvChannels.add(kj::addRef(*kvChannel));
  auto accounting = kj::rc<SubrequestAccountingState>();
  kj::HttpHeaderTable headerTable;
  kj::HttpHeaders headers(headerTable);
  RecordingKvOperationLimitPolicy kvPolicy(accounting.addRef());
  LuaIoContext host(*outbound, headerTable, {}, testEntropySource(),
      kj::heap<RecordingSubrequestLimitPolicy>(accounting.addRef()),
      kj::refcounted<RecordingRequestObserver>(accounting.addRef()), nullptr, kvChannels.asPtr(),
      kvPolicy);

  auto response =
      runtime.fetch("GET"_kj, "/"_kj, headers, nullptr, host.getPtr()).wait(io.waitScope);

  KJ_EXPECT(asString(response.body) ==
      "false:false:false:false:false:false:false:false:false:false:false:false:false:false:false:false:false:false:false:false:false:false");
  KJ_EXPECT(accounting->kvAdmissionCalls == 5);
  KJ_EXPECT(accounting->kvOperations[0] == KvOperationType::GET);
  KJ_EXPECT(accounting->kvOperations[1] == KvOperationType::PUT);
  KJ_EXPECT(accounting->kvOperations[2] == KvOperationType::DELETE);
  KJ_EXPECT(accounting->kvOperations[3] == KvOperationType::GET_WITH);
  KJ_EXPECT(accounting->kvOperations[4] == KvOperationType::LIST);
  KJ_EXPECT(accounting->lastKvOperation == KvOperationType::LIST);
  KJ_EXPECT(accounting->admissionCalls == 5);
  KJ_EXPECT(accounting->lastType == SubrequestType::IN_HOUSE);
  KJ_EXPECT(accounting->observerCalls == 0);
  KJ_EXPECT(kvChannelPtr->requestsStarted == 5);
  KJ_EXPECT(kvChannelPtr->cfBlobJson == kj::none);
  KJ_EXPECT(outbound->requestsStarted == 0);
}

KJ_TEST("LuaJIT KV policy rejects an operation before routing") {
  auto io = kj::setupAsyncIo();
  constexpr kj::StringPtr source = R"lua(
    return {
      fetch = function()
        local succeeded = pcall(function() workerd.bindings.KV.delete("key") end)
        return { status = 200, body = tostring(succeeded) }
      end
    }
  )lua"_kj;

  kj::Vector<LuaBinding> bindings;
  bindings.add(LuaBinding{
    .name = kj::str("KV"),
    .value = LuaBinding::KvNamespaceBinding{.index = 0},
  });
  LuaRuntime runtime(source, io.provider->getTimer(), bindings.releaseAsArray());
  auto outbound = kj::refcounted<RecordingSubrequestChannel>();
  auto kvChannel = kj::refcounted<RecordingSubrequestChannel>();
  kj::Vector<kj::Own<IoChannelFactory::SubrequestChannel>> kvChannels;
  kvChannels.add(kj::addRef(*kvChannel));
  auto accounting = kj::rc<SubrequestAccountingState>();
  accounting->rejectKvOperation = true;
  kj::HttpHeaderTable headerTable;
  kj::HttpHeaders headers(headerTable);
  RecordingKvOperationLimitPolicy kvPolicy(accounting.addRef());
  LuaIoContext host(*outbound, headerTable, {}, testEntropySource(),
      kj::heap<RecordingSubrequestLimitPolicy>(accounting.addRef()),
      kj::refcounted<RecordingRequestObserver>(accounting.addRef()), nullptr, kvChannels.asPtr(),
      kvPolicy);

  auto response =
      runtime.fetch("GET"_kj, "/"_kj, headers, nullptr, host.getPtr()).wait(io.waitScope);

  KJ_EXPECT(asString(response.body) == "false");
  KJ_EXPECT(accounting->kvAdmissionCalls == 1);
  KJ_EXPECT(accounting->lastKvOperation == KvOperationType::DELETE);
  KJ_EXPECT(accounting->admissionCalls == 0);
  KJ_EXPECT(kvChannel->requestsStarted == 0);
  KJ_EXPECT(accounting->observerCalls == 0);
}

KJ_TEST("LuaJIT request scope rejects subrequests before routing or observation") {
  auto io = kj::setupAsyncIo();
  constexpr kj::StringPtr source = R"lua(
    return {
      fetch = function()
        local succeeded = pcall(function()
          workerd.fetch({ url = "https://example.com/denied" })
        end)
        return { status = 200, body = tostring(succeeded) }
      end
    }
  )lua"_kj;

  LuaRuntime runtime(source, io.provider->getTimer());
  auto accounting = kj::rc<SubrequestAccountingState>();
  accounting->rejectSubrequest = true;
  auto outbound = kj::refcounted<RecordingSubrequestChannel>();
  kj::HttpHeaderTable headerTable;
  kj::HttpHeaders headers(headerTable);
  LuaIoContext host(*outbound, headerTable, {}, testEntropySource(),
      kj::heap<RecordingSubrequestLimitPolicy>(accounting.addRef()),
      kj::refcounted<RecordingRequestObserver>(accounting.addRef()));

  auto response =
      runtime.fetch("GET"_kj, "/"_kj, headers, nullptr, host.getPtr()).wait(io.waitScope);

  KJ_EXPECT(asString(response.body) == "false");
  KJ_EXPECT(accounting->admissionCalls == 1);
  KJ_EXPECT(accounting->lastType == SubrequestType::EXTERNAL);
  KJ_EXPECT(accounting->observerCalls == 0);
  KJ_EXPECT(outbound->requestsStarted == 0);
}

KJ_TEST("LuaJIT worker rejects scheduled and alarm events") {
  auto io = kj::setupAsyncIo();
  LuaRuntime runtime("return { fetch = function() return { status = 200, body = '' } end }"_kj,
      io.provider->getTimer());
  auto outbound = kj::refcounted<InertSubrequestChannel>();
  kj::HttpHeaderTable headerTable;
  LuaWorkerInterface worker(runtime, *outbound, headerTable, {}, testEntropySource(),
      kj::heap<UnrestrictedSubrequestLimitPolicy>(), kj::refcounted<RequestObserver>(),
      LuaRequestBodyMode::BUFFERED);

  KJ_EXPECT(kj::runCatchingExceptions([&]() {
    worker.runScheduled(kj::UNIX_EPOCH, "*"_kj).wait(io.waitScope);
  }) != kj::none);
  KJ_EXPECT(kj::runCatchingExceptions(
                [&]() { worker.runAlarm(kj::UNIX_EPOCH, 0).wait(io.waitScope); }) != kj::none);
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

KJ_TEST("LuaJIT randomBytes returns bounded binary-safe strings") {
  auto io = kj::setupAsyncIo();
  constexpr kj::StringPtr source = R"lua(
    return {
      fetch = function()
        local bytes = workerd.randomBytes(32)
        local empty = workerd.randomBytes(0)
        local maximum = workerd.randomBytes(65536)
        local uuid = workerd.randomUUID()
        local negative = pcall(workerd.randomBytes, -1)
        local fractional = pcall(workerd.randomBytes, 1.5)
        local tooLarge = pcall(workerd.randomBytes, 65537)
        local wrongType = pcall(workerd.randomBytes, "32")
        local malformedDescriptor = pcall(function()
          workerd.await({ operation = "randomBytes", size = 65537 })
        end)
        return {
          status = 200,
          body = table.concat({
            tostring(type(bytes) == "string"), tostring(#bytes),
            tostring(string.byte(bytes, 1)), tostring(string.byte(bytes, 32)), tostring(empty == ""),
            tostring(#maximum), uuid, tostring(negative), tostring(fractional), tostring(tooLarge), tostring(wrongType),
            tostring(malformedDescriptor),
          }, ":"),
        }
      end
    }
  )lua"_kj;

  LuaRuntime runtime(source, io.provider->getTimer());
  TestLuaHostOperations hostOperations;
  kj::HttpHeaderTable headerTable;
  kj::HttpHeaders headers(headerTable);
  auto response = runtime.fetch("GET"_kj, "/random"_kj, headers, nullptr, hostOperations.getPtr())
                      .wait(io.waitScope);
  KJ_EXPECT(asString(response.body) ==
      "true:32:0:31:true:65536:01234567-89ab-4cde-8fab-0123456789ab:false:false:false:false:false");
  KJ_EXPECT(hostOperations.randomBytesCalls == 3);
  KJ_EXPECT(hostOperations.randomUuidCalls == 1);
}

KJ_TEST("LuaJIT parses remote URLs and relative references") {
  auto io = kj::setupAsyncIo();
  constexpr kj::StringPtr source = R"lua(
    return {
      fetch = function()
        local absolute = assert(workerd.parseUrl(
          "https://user:pass@example.com:8443/a%20b/b/?x=1&x=two&flag#frag"))
        local relative = assert(workerd.parseUrl("../next?ok", "https://example.com/a/b/"))
        local invalid, reason = workerd.parseUrl("relative")
        local invalidBase, baseReason = workerd.parseUrl("../", "relative")
        local body = table.concat({
          absolute.href,
          absolute.scheme,
          absolute.host,
          table.concat(absolute.path, "/"),
          tostring(absolute.hasTrailingSlash),
          tostring(#absolute.query),
          absolute.query[1].name .. "=" .. absolute.query[1].value,
          absolute.query[2].name .. "=" .. absolute.query[2].value,
          absolute.query[3].name .. "=" .. tostring(absolute.query[3].value),
          absolute.fragment,
          absolute.username,
          absolute.password,
          relative.href,
          tostring(invalid == nil),
          tostring(type(reason) == "string"),
          tostring(invalidBase == nil),
          tostring(type(baseReason) == "string"),
        }, "|")
        return { status = 200, body = body }
      end
    }
  )lua"_kj;

  LuaRuntime runtime(source, io.provider->getTimer());
  kj::HttpHeaderTable headerTable;
  kj::HttpHeaders headers(headerTable);
  auto response = runtime.fetch("GET"_kj, "/url"_kj, headers, nullptr).wait(io.waitScope);
  KJ_EXPECT(asString(response.body) ==
      "https://user:pass@example.com:8443/a%20b/b/?x=1&x=two&flag#frag|https|example.com:8443|a b/b|true|3|x=1|x=two|flag=nil|frag|user|pass|https://example.com/a/next?ok|true|true|true|true");
}

KJ_TEST("LuaJIT computes bounded binary-safe SHA-2 digests") {
  auto io = kj::setupAsyncIo();
  constexpr kj::StringPtr source = R"lua(
    local function toHex(bytes)
      return (bytes:gsub(".", function(byte)
        return string.format("%02x", byte:byte())
      end))
    end
    return {
      fetch = function()
        local maxInput = workerd.digest("SHA-256", string.rep("a", 1048576))
        local oversized = pcall(function()
          workerd.digest("SHA-256", string.rep("a", 1048577))
        end)
        local unsupported = pcall(workerd.digest, "SHA3-256", "abc")
        local wrongType = pcall(workerd.digest, "SHA-256", 123)
        local wrongArity = pcall(workerd.digest, "SHA-256", "abc", "extra")
        return {
          status = 200,
          body = table.concat({
            toHex(workerd.digest("MD5", "abc")),
            toHex(workerd.digest("SHA-1", "abc")),
            toHex(workerd.digest("SHA-256", "abc")),
            toHex(workerd.digest("sha-384", "abc")),
            toHex(workerd.digest("SHA-512", "abc")),
            toHex(workerd.digest("SHA-256", "")),
            toHex(workerd.digest("SHA-256", "a\0b")),
            tostring(#maxInput == 32),
            tostring(not oversized),
            tostring(not unsupported),
            tostring(not wrongType),
            tostring(not wrongArity),
          }, ":"),
        }
      end,
    }
  )lua"_kj;

  LuaRuntime runtime(source, io.provider->getTimer());
  kj::HttpHeaderTable headerTable;
  kj::HttpHeaders headers(headerTable);
  auto response = runtime.fetch("GET"_kj, "/digest"_kj, headers, nullptr).wait(io.waitScope);
  KJ_EXPECT(asString(response.body) ==
      "900150983cd24fb0d6963f7d28e17f72:a9993e364706816aba3e25717850c26c9cd0d89d:"
      "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad:"
      "cb00753f45a35e8bb5a03d699ac65007272c32ab0eded1631a8b605a43ff5bed"
      "8086072ba1e7cc2358baeca134c825a7:"
      "ddaf35a193617abacc417349ae20413112e6fa4e89a97ea20a9eeee64b55d39a"
      "2192992a274fc1a836ba3c23a3feebbd454d4423643ce80e2a9ac94fa54ca49f:"
      "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855:"
      "59b271ae1bbcb1d31d41929817f4b16fb439eb4f31520b5ad1d5ce98920a7138:true:true:true:true:true");
}

KJ_TEST("LuaJIT computes bounded binary-safe HMAC values") {
  auto io = kj::setupAsyncIo();
  constexpr kj::StringPtr source = R"lua(
    local function toHex(bytes)
      return (bytes:gsub(".", function(byte)
        return string.format("%02x", byte:byte())
      end))
    end
    return {
      fetch = function()
        local maxKey = workerd.hmac("SHA-256", string.rep("k", 65536), "")
        local maxInput = workerd.hmac("SHA-256", "k", string.rep("a", 1048576))
        local oversizedKey = pcall(function()
          workerd.hmac("SHA-256", string.rep("k", 65537), "")
        end)
        local oversizedInput = pcall(function()
          workerd.hmac("SHA-256", "k", string.rep("a", 1048577))
        end)
        local emptyKey = pcall(workerd.hmac, "SHA-256", "", "abc")
        local unsupported = pcall(workerd.hmac, "SHA3-256", "key", "abc")
        local wrongType = pcall(workerd.hmac, "SHA-256", "key", 123)
        local wrongArity = pcall(workerd.hmac, "SHA-256", "key", "abc", "extra")
        local message = "The quick brown fox jumps over the lazy dog"
        local mac = workerd.hmac("SHA-256", "key", message)
        local forgedMac = string.char((mac:byte(1) + 1) % 256) .. mac:sub(2)
        local verificationWorks =
          workerd.verifyHmac("SHA-256", "key", message, mac) and
          not workerd.verifyHmac("SHA-256", "key", message .. "!", mac) and
          not workerd.verifyHmac("SHA-256", "key", message, forgedMac) and
          not workerd.verifyHmac("SHA-256", "key", message, mac:sub(2)) and
          not pcall(workerd.verifyHmac, "SHA3-256", "key", message, mac) and
          not pcall(workerd.verifyHmac, "SHA-256", "key", message, 123) and
          not pcall(workerd.verifyHmac, "SHA-256", "key", message) and
          not pcall(workerd.verifyHmac, "SHA-256", "", message, mac) and
          not pcall(workerd.verifyHmac, "SHA-256", string.rep("k", 65537), "", mac) and
          not pcall(workerd.verifyHmac, "SHA-256", "key", string.rep("a", 1048577), mac)
        return {
          status = 200,
          body = table.concat({
            toHex(workerd.hmac("MD5", "key", "The quick brown fox jumps over the lazy dog")),
            toHex(workerd.hmac("SHA-1", "key", "The quick brown fox jumps over the lazy dog")),
            toHex(workerd.hmac("sha-256", "key", "The quick brown fox jumps over the lazy dog")),
            toHex(workerd.hmac("SHA-384", "key", "The quick brown fox jumps over the lazy dog")),
            toHex(workerd.hmac("SHA-512", "key", "The quick brown fox jumps over the lazy dog")),
            toHex(workerd.hmac("SHA-256", "k\0y", "a\0b")),
            tostring(#maxKey == 32),
            tostring(#maxInput == 32),
            tostring(not oversizedKey),
            tostring(not oversizedInput),
            tostring(not emptyKey),
            tostring(not unsupported),
            tostring(not wrongType),
            tostring(not wrongArity),
            tostring(verificationWorks),
          }, ":"),
        }
      end,
    }
  )lua"_kj;

  LuaRuntime runtime(source, io.provider->getTimer());
  kj::HttpHeaderTable headerTable;
  kj::HttpHeaders headers(headerTable);
  auto response = runtime.fetch("GET"_kj, "/hmac"_kj, headers, nullptr).wait(io.waitScope);
  KJ_EXPECT(asString(response.body) ==
      "80070713463e7749b90c2dc24911e275:de7c9b85b8b78aa6bc8a7a36f70a90701c9db4d9:"
      "f7bc83f430538424b13298e6aa6fb143ef4d59a14946175997479dbc2d1a3cd8:"
      "d7f4727e2c0b39ae0f1e40cc96f60242d5b7801841cea6fc592c5d3e1ae50700582a96cf35e1e554995fe4e03381c237:"
      "b42af09057bac1e2d41708e48a902e09b5ff7f12ab428a4fe86653c73dd248fb82f948a549f7b791a5b41915ee4d1ec3935357e4e2317250d0372afa2ebeeb3a:"
      "20b3a393e9f0e606ae91954f1f871ebd5cd9c3a8f26e7b22beaed26ab4891891:true:true:true:true:true:true:true:true:true");
}

KJ_TEST("LuaJIT HMAC CryptoKey bindings are opaque and enforce usages") {
  auto io = kj::setupAsyncIo();
  constexpr kj::StringPtr source = R"lua(
    local function toHex(value)
      return (value:gsub(".", function(byte)
        return string.format("%02x", byte:byte())
      end))
    end
    return {
      fetch = function()
        local key = workerd.bindings.SIGNING_KEY
        local verifyOnly = workerd.bindings.VERIFY_ONLY
        local signOnly = workerd.bindings.SIGN_ONLY
        local message = "The quick brown fox jumps over the lazy dog"
        local mac = workerd.hmac(key, message)
        local verifies = workerd.verifyHmac(key, message, mac)
        local rejectsChangedMessage = not workerd.verifyHmac(key, message .. "!", mac)
        local rejectsWrongUsage = not pcall(workerd.hmac, verifyOnly, message) and
          not pcall(workerd.verifyHmac, signOnly, message, mac)
        local supportedHashes =
          workerd.hmac(workerd.bindings.SHA1_KEY, message) == workerd.hmac("SHA-1", "key", message) and
          workerd.hmac(key, message) == workerd.hmac("SHA-256", "key", message) and
          workerd.hmac(workerd.bindings.SHA384_KEY, message) == workerd.hmac("SHA-384", "key", message) and
          workerd.hmac(workerd.bindings.SHA512_KEY, message) == workerd.hmac("SHA-512", "key", message)
        return {
          status = 200,
          body = toHex(mac) .. ":" .. tostring(verifies) .. ":" ..
            tostring(rejectsChangedMessage) .. ":" .. tostring(rejectsWrongUsage) .. ":" ..
            tostring(tostring(key) == "[CryptoKey]") .. ":" .. tostring(getmetatable(key) == false) .. ":" ..
            tostring(supportedHashes),
        }
      end,
    }
  )lua"_kj;

  auto makeKey = [](kj::StringPtr hashAlgorithm, kj::Array<LuaHmacKeyUsage> usages) {
    auto keyData = kj::heapArray<kj::byte>(3);
    std::memcpy(keyData.begin(), "key", 3);
    return kj::heap<LuaHmacKey>(kj::str(hashAlgorithm), kj::mv(keyData), kj::mv(usages));
  };
  kj::Vector<LuaBinding> bindings;
  kj::Vector<LuaHmacKeyUsage> signingAndVerifying;
  signingAndVerifying.add(LuaHmacKeyUsage::SIGN);
  signingAndVerifying.add(LuaHmacKeyUsage::VERIFY);
  bindings.add(LuaBinding{
    .name = kj::str("SIGNING_KEY"),
    .value = LuaBinding::HmacKeyValue{.key = makeKey(
                                          "SHA-256"_kj, signingAndVerifying.releaseAsArray())},
  });
  kj::Vector<LuaHmacKeyUsage> verifyOnly;
  verifyOnly.add(LuaHmacKeyUsage::VERIFY);
  bindings.add(LuaBinding{
    .name = kj::str("VERIFY_ONLY"),
    .value = LuaBinding::HmacKeyValue{.key = makeKey("SHA-256"_kj, verifyOnly.releaseAsArray())},
  });
  kj::Vector<LuaHmacKeyUsage> signOnly;
  signOnly.add(LuaHmacKeyUsage::SIGN);
  bindings.add(LuaBinding{
    .name = kj::str("SIGN_ONLY"),
    .value = LuaBinding::HmacKeyValue{.key = makeKey("SHA-256"_kj, signOnly.releaseAsArray())},
  });
  auto addSigningKey = [&](kj::StringPtr name, kj::StringPtr hashAlgorithm) {
    kj::Vector<LuaHmacKeyUsage> usages;
    usages.add(LuaHmacKeyUsage::SIGN);
    bindings.add(LuaBinding{
      .name = kj::str(name),
      .value = LuaBinding::HmacKeyValue{.key = makeKey(hashAlgorithm, usages.releaseAsArray())},
    });
  };
  addSigningKey("SHA1_KEY"_kj, "SHA-1"_kj);
  addSigningKey("SHA384_KEY"_kj, "SHA-384"_kj);
  addSigningKey("SHA512_KEY"_kj, "SHA-512"_kj);

  LuaRuntime runtime(source, io.provider->getTimer(), bindings.releaseAsArray());
  kj::HttpHeaderTable headerTable;
  kj::HttpHeaders headers(headerTable);
  auto response = runtime.fetch("GET"_kj, "/crypto-key"_kj, headers, nullptr).wait(io.waitScope);
  KJ_EXPECT(asString(response.body) ==
      "f7bc83f430538424b13298e6aa6fb143ef4d59a14946175997479dbc2d1a3cd8:true:true:true:true:true:true");
}

KJ_TEST("LuaJIT service binding fetches select their configured channel") {
  auto io = kj::setupAsyncIo();
  constexpr kj::StringPtr source = R"lua(
    return {
      fetch = function()
        local response = workerd.bindings.TARGET.fetch({ url = "https://target/path" })
        return { status = 200, body = tostring(response.status) .. ":" .. workerd.readAll(response.body) }
      end,
    }
  )lua"_kj;

  kj::Vector<LuaBinding> bindings;
  bindings.add(LuaBinding{
    .name = kj::str("TARGET"),
    .value = LuaBinding::ServiceBinding{.index = 1},
  });
  LuaRuntime runtime(source, io.provider->getTimer(), bindings.releaseAsArray());
  TestLuaHostOperations hostOperations;
  kj::HttpHeaderTable headerTable;
  kj::HttpHeaders headers(headerTable);
  auto response =
      runtime.fetch("GET"_kj, "/"_kj, headers, nullptr, hostOperations.getPtr()).wait(io.waitScope);

  KJ_EXPECT(asString(response.body) == "202:upstream");
  KJ_IF_SOME(request, hostOperations.lastRequest) {
    KJ_IF_SOME(index, request.serviceBindingIndex) {
      KJ_EXPECT(index == 1);
    } else {
      KJ_FAIL_ASSERT("service binding fetch should carry its configured channel index");
    }
  } else {
    KJ_FAIL_ASSERT("service binding fetch should reach the host");
  }
}

KJ_TEST("LuaJIT passes configured bindings as fetch env") {
  auto io = kj::setupAsyncIo();
  constexpr kj::StringPtr source = R"lua(
    return {
      fetch = function(request, env)
        env.calls = (env.calls or 0) + 1
        return {
          status = 200,
          body = env.GREETING .. ":" .. request.url .. ":" .. tostring(env.calls) .. ":" ..
            tostring(env == workerd.bindings),
        }
      end
    }
  )lua"_kj;

  kj::Vector<LuaBinding> bindings;
  bindings.add(LuaBinding{
    .name = kj::str("GREETING"),
    .value = LuaBinding::TextValue{.value = kj::str("hello")},
  });
  LuaRuntime runtime(source, io.provider->getTimer(), bindings.releaseAsArray());
  kj::HttpHeaderTable headerTable;
  kj::HttpHeaders headers(headerTable);

  auto first = runtime.fetch("GET"_kj, "/first"_kj, headers, nullptr).wait(io.waitScope);
  auto second = runtime.fetch("GET"_kj, "/second"_kj, headers, nullptr).wait(io.waitScope);
  KJ_EXPECT(asString(first.body) == "hello:/first:1:true");
  KJ_EXPECT(asString(second.body) == "hello:/second:2:true");
}

KJ_TEST("LuaJIT Workers AI run sends JSON inputs and returns model output") {
  auto io = kj::setupAsyncIo();
  constexpr kj::StringPtr source = R"lua(
    return {
      fetch = function(_, env)
        local result = env.AI:run("@cf/test/model", { prompt = "hello" }, { stop = { "x", workerd.null } })
        return { status = 200, body = result.response }
      end
    }
  )lua"_kj;

  kj::Vector<LuaBinding> bindings;
  bindings.add(LuaBinding{
    .name = kj::str("AI"),
    .value = LuaBinding::AiBinding{.fetcherIndex = 0},
  });
  LuaRuntime runtime(source, io.provider->getTimer(), bindings.releaseAsArray());
  TestLuaHostOperations host;
  host.aiMockEnabled = true;
  host.aiResponseJson = kj::str(R"({"response":"model response"})");
  kj::HttpHeaderTable headerTable;
  kj::HttpHeaders headers(headerTable);

  auto response =
      runtime.fetch("GET"_kj, "/"_kj, headers, nullptr, host.getPtr()).wait(io.waitScope);
  KJ_EXPECT(asString(response.body) == "model response");
  KJ_EXPECT(host.aiRequestUrls.size() == 1);
  KJ_EXPECT(host.aiRequestUrls[0] == "https://workers-binding.ai/run?version=3");
  KJ_EXPECT(host.aiRequestBodies[0] ==
      R"json({"inputs":{"prompt":"hello"},"options":{"stop":["x",null]}})json");
  bool hasModelHeader = false;
  bool hasSdkHeader = false;
  for (const auto& header: host.aiRequestHeaders) {
    hasModelHeader |= header.name == "cf-consn-model-id" && header.value == "@cf/test/model";
    hasSdkHeader |= header.name == "cf-consn-sdk-version" && header.value == "2.0.0";
  }
  KJ_EXPECT(hasModelHeader);
  KJ_EXPECT(hasSdkHeader);
}

KJ_TEST("LuaJIT D1 binding supports prepared first, all, and run queries") {
  auto io = kj::setupAsyncIo();
  constexpr kj::StringPtr source = R"lua(
    return {
      fetch = function(request, env)
        local statement = env.DB:prepare("SELECT ? AS answer"):bind(
          "hello", 2.5, true, workerd.null, nil)
        local first = statement:first("answer")
        local all = statement:all()
        local run = env.DB:prepare("INSERT INTO answers(value) VALUES (?)"):bind("saved"):run()
        return {
          status = 200,
          body = first .. ":" .. all.results[1].answer .. ":" .. tostring(run.meta.changes),
        }
      end
    }
  )lua"_kj;

  kj::Vector<LuaBinding> bindings;
  bindings.add(LuaBinding{
    .name = kj::str("DB"),
    .value = LuaBinding::D1DatabaseBinding{.fetcherIndex = 0},
  });
  LuaRuntime runtime(source, io.provider->getTimer(), bindings.releaseAsArray());
  TestLuaHostOperations host;
  host.d1MockEnabled = true;
  host.d1QueryResponse = kj::str(
      R"({"success":true,"results":{"columns":["answer"],"rows":[["hello"]]},"meta":{"duration":0.1,"changes":0}})");
  host.d1RunResponse =
      kj::str(R"({"success":true,"results":[],"meta":{"duration":0.2,"changes":1}})");
  kj::HttpHeaderTable headerTable;
  kj::HttpHeaders headers(headerTable);

  auto response =
      runtime.fetch("GET"_kj, "/"_kj, headers, nullptr, host.getPtr()).wait(io.waitScope);
  KJ_EXPECT(asString(response.body) == "hello:hello:1");
  KJ_EXPECT(host.d1RequestUrls.size() == 3);
  KJ_EXPECT(host.d1RequestUrls[0] == "http://d1/query?resultsFormat=ROWS_AND_COLUMNS");
  KJ_EXPECT(host.d1RequestUrls[2] == "http://d1/execute?resultsFormat=NONE");
  KJ_EXPECT(host.d1RequestBodies[0] ==
      R"({"sql":"SELECT ? AS answer","params":["hello",2.5,true,null,null]})");
  KJ_EXPECT(host.d1RequestBodies[2] ==
      R"json({"sql":"INSERT INTO answers(value) VALUES (?)","params":["saved"]})json");
}

KJ_TEST("LuaJIT D1 query text and string parameters require valid UTF-8") {
  auto io = kj::setupAsyncIo();
  constexpr kj::StringPtr invalidSql = R"lua(
    return {
      fetch = function(_, env)
        return env.DB:prepare(string.char(255)):all()
      end
    }
  )lua"_kj;
  constexpr kj::StringPtr invalidParameter = R"lua(
    return {
      fetch = function(_, env)
        return env.DB:prepare("SELECT ?"):bind(string.char(255)):all()
      end
    }
  )lua"_kj;

  kj::Vector<LuaBinding> bindings;
  bindings.add(LuaBinding{
    .name = kj::str("DB"),
    .value = LuaBinding::D1DatabaseBinding{.fetcherIndex = 0},
  });
  TestLuaHostOperations host;
  host.d1MockEnabled = true;
  kj::HttpHeaderTable headerTable;
  kj::HttpHeaders headers(headerTable);

  LuaRuntime sqlRuntime(invalidSql, io.provider->getTimer(), kj::mv(bindings).releaseAsArray());
  KJ_EXPECT_THROW(FAILED,
      sqlRuntime.fetch("GET"_kj, "/"_kj, headers, nullptr, host.getPtr()).wait(io.waitScope));

  kj::Vector<LuaBinding> parameterBindings;
  parameterBindings.add(LuaBinding{
    .name = kj::str("DB"),
    .value = LuaBinding::D1DatabaseBinding{.fetcherIndex = 0},
  });
  LuaRuntime parameterRuntime(
      invalidParameter, io.provider->getTimer(), parameterBindings.releaseAsArray());
  KJ_EXPECT_THROW(FAILED,
      parameterRuntime.fetch("GET"_kj, "/"_kj, headers, nullptr, host.getPtr()).wait(io.waitScope));
  KJ_EXPECT(host.d1RequestUrls.size() == 0);
}

KJ_TEST("LuaJIT exposes text, binary, and JSON bindings under workerd.bindings") {
  auto io = kj::setupAsyncIo();
  constexpr kj::StringPtr source = R"lua(
    return {
      fetch = function()
        local config = workerd.bindings.CONFIG
        local binary = workerd.bindings.BINARY
        local empty = workerd.bindings.EMPTY
        config.count = config.count + 1
        return {
          status = 200,
          body = workerd.bindings.GREETING .. ":" .. tostring(#empty) .. ":" ..
            tostring(#binary) .. ":" ..
            tostring(binary:byte(2)) .. ":" .. tostring(config.enabled) .. ":" ..
            tostring(config.count) .. ":" .. tostring(config.nilValue == workerd.null) .. ":" ..
            tostring(config.list[2] == workerd.null) .. ":" ..
            tostring(workerd.bindings.MISSING == workerd.null),
        }
      end
    }
  )lua"_kj;

  kj::Vector<LuaBinding> bindings;
  bindings.add(LuaBinding{
    .name = kj::str("GREETING"),
    .value = LuaBinding::TextValue{.value = kj::str("hello")},
  });
  auto binary = kj::heapArray<kj::byte>(3);
  std::memcpy(binary.begin(), "a\0b", 3);
  bindings.add(LuaBinding{
    .name = kj::str("BINARY"),
    .value = LuaBinding::DataValue{.value = kj::mv(binary)},
  });
  bindings.add(LuaBinding{
    .name = kj::str("EMPTY"),
    .value = LuaBinding::DataValue{.value = kj::heapArray<kj::byte>(0)},
  });
  bindings.add(LuaBinding{
    .name = kj::str("MISSING"),
    .value = LuaBinding::NullValue{},
  });
  bindings.add(LuaBinding{
    .name = kj::str("CONFIG"),
    .value =
        LuaBinding::JsonValue{
          .value = kj::str(R"({"enabled":true,"count":2,"nilValue":null,"list":["x",null]})")},
  });

  LuaRuntime runtime(source, io.provider->getTimer(), bindings.releaseAsArray());
  kj::HttpHeaderTable headerTable;
  kj::HttpHeaders headers(headerTable);
  auto first = runtime.fetch("GET"_kj, "/"_kj, headers, nullptr).wait(io.waitScope);
  auto second = runtime.fetch("GET"_kj, "/"_kj, headers, nullptr).wait(io.waitScope);
  KJ_EXPECT(asString(first.body) == "hello:0:3:0:true:3:true:true:true");
  KJ_EXPECT(asString(second.body) == "hello:0:3:0:true:4:true:true:true");
}

KJ_TEST("LuaJIT rejects malformed JSON bindings") {
  auto io = kj::setupAsyncIo();
  kj::Vector<LuaBinding> bindings;
  bindings.add(LuaBinding{
    .name = kj::str("INVALID"),
    .value = LuaBinding::JsonValue{.value = kj::str("{ invalid json }")},
  });
  KJ_EXPECT(kj::runCatchingExceptions([&]() {
    LuaRuntime runtime("return { fetch = function() return { status = 200, body = '' } end }"_kj,
        io.provider->getTimer(), bindings.releaseAsArray());
  }) != kj::none);
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
  KJ_IF_SOME(exception, kj::runCatchingExceptions([&]() {
    (void)thrownError.fetch("GET"_kj, "/"_kj, headers, nullptr).wait(io.waitScope);
  })) {
    KJ_EXPECT(exception.getDescription().contains("handler failed"));
    KJ_EXPECT(exception.getDescription().contains("worker.lua"));
  } else {
    KJ_FAIL_ASSERT("Lua handler error should fail the request");
  }
}

}  // namespace
}  // namespace workerd::rt::luajit
