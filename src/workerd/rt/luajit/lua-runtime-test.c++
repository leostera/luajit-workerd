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

  kj::Promise<kj::Maybe<kj::Array<kj::byte>>> readRequestBody(size_t maxBytes) override {
    KJ_FAIL_REQUIRE("mock does not provide an inbound request body stream");
  }
};

class TestLuaHostOperations final: public LuaHostOperationsWithoutRequestBody {
 public:
  bool hasRequestBodyStream() const override {
    return true;
  }

  kj::Promise<LuaHttpResponse> fetch(LuaHttpRequest request) override {
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
  LuaIoContext host(*outbound, headerTable, {}, kj::heap<UnrestrictedSubrequestLimitPolicy>(),
      kj::refcounted<RequestObserver>());
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
  LuaIoContext host(*outbound, headerTable, {}, kj::heap<UnrestrictedSubrequestLimitPolicy>(),
      kj::refcounted<RequestObserver>());
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
  LuaIoContext host(*outbound, headerTable, {}, kj::heap<UnrestrictedSubrequestLimitPolicy>(),
      kj::refcounted<RequestObserver>());
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
  LuaIoContext host(*outbound, headerTable, {}, kj::heap<UnrestrictedSubrequestLimitPolicy>(),
      kj::refcounted<RequestObserver>());
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
    LuaIoContext firstHost(*outbound, headerTable, {},
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
    LuaIoContext secondHost(*outbound, headerTable, {},
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
  LuaIoContext host(*outbound, headerTable, {}, kj::heap<UnrestrictedSubrequestLimitPolicy>(),
      kj::refcounted<RequestObserver>());
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
  LuaIoContext host(*outbound, headerTable, kj::mv(metadata),
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
  LuaIoContext host(*outbound, headerTable, {},
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
  LuaWorkerInterface worker(runtime, *outbound, headerTable, {},
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
            tostring(config.list[2] == workerd.null),
        }
      end
    }
  )lua"_kj;

  kj::Vector<LuaBinding> bindings;
  bindings.add(LuaBinding{
    .name = kj::str("GREETING"),
    .type = LuaBinding::Type::TEXT,
    .text = kj::str("hello"),
  });
  auto binary = kj::heapArray<kj::byte>(3);
  std::memcpy(binary.begin(), "a\0b", 3);
  bindings.add(LuaBinding{
    .name = kj::str("BINARY"),
    .type = LuaBinding::Type::DATA,
    .data = kj::mv(binary),
  });
  bindings.add(LuaBinding{
    .name = kj::str("EMPTY"),
    .type = LuaBinding::Type::DATA,
    .data = kj::heapArray<kj::byte>(0),
  });
  bindings.add(LuaBinding{
    .name = kj::str("CONFIG"),
    .type = LuaBinding::Type::JSON,
    .text = kj::str(R"({"enabled":true,"count":2,"nilValue":null,"list":["x",null]})"),
  });

  LuaRuntime runtime(source, io.provider->getTimer(), bindings.releaseAsArray());
  kj::HttpHeaderTable headerTable;
  kj::HttpHeaders headers(headerTable);
  auto first = runtime.fetch("GET"_kj, "/"_kj, headers, nullptr).wait(io.waitScope);
  auto second = runtime.fetch("GET"_kj, "/"_kj, headers, nullptr).wait(io.waitScope);
  KJ_EXPECT(asString(first.body) == "hello:0:3:0:true:3:true:true");
  KJ_EXPECT(asString(second.body) == "hello:0:3:0:true:4:true:true");
}

KJ_TEST("LuaJIT rejects malformed JSON bindings") {
  auto io = kj::setupAsyncIo();
  kj::Vector<LuaBinding> bindings;
  bindings.add(LuaBinding{
    .name = kj::str("INVALID"),
    .type = LuaBinding::Type::JSON,
    .text = kj::str("{ invalid json }"),
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
