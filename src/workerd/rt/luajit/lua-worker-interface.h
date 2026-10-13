// Copyright (c) 2026 Cloudflare, Inc.
// Licensed under the Apache 2.0 license found in the LICENSE file or at:
//     https://opensource.org/licenses/Apache-2.0

#pragma once

#include <workerd/io/io-channels.h>
#include <workerd/io/request-scope.h>
#include <workerd/io/worker-interface.h>
#include <workerd/rt/luajit/lua-runtime.h>

namespace workerd {
class RequestObserver;
}

namespace workerd::rt::luajit {

// Request-scoped Lua host operations. This composes the shared request accounting/observer scope
// with Lua's own route and trace metadata, without depending on JS worker or isolate state.
class LuaIoContext final: public LuaHostOperations {
 public:
  LuaIoContext(IoChannelFactory::SubrequestChannel& outbound,
      const kj::HttpHeaderTable& headerTable,
      IoChannelFactory::SubrequestMetadata metadata,
      kj::EntropySource& entropySource,
      kj::Own<SubrequestLimitPolicy> subrequestLimitPolicy,
      kj::Own<workerd::RequestObserver> observer);
  ~LuaIoContext() noexcept(false) override;

  kj::Promise<LuaHttpResponse> fetch(LuaHttpRequest request) override;
  bool hasRequestBodyStream() const override;
  kj::Promise<kj::Maybe<kj::Array<kj::byte>>> readRequestBody(size_t maxBytes) override;
  void fillRandomBytes(kj::ArrayPtr<kj::byte> output) override;

  void setRequestBodyStream(kj::AsyncInputStream& stream);
  void clearRequestBodyStream();

 private:
  IoChannelFactory::SubrequestMetadata makeSubrequestMetadata(TraceContext& tracing);

  kj::Own<SubrequestLimitPolicy> subrequestLimitPolicy;
  kj::Own<workerd::RequestObserver> observer;
  RequestScope requestScope;
  IoChannelFactory::SubrequestChannel& outbound;
  const kj::HttpHeaderTable& headerTable;
  kj::EntropySource& entropySource;
  kj::Maybe<kj::String> cfBlobJson;
  SpanParent parentSpan;
  SpanParent userSpanParent;
  kj::Maybe<kj::AsyncInputStream&> requestBodyStream;
  size_t requestBodyBytesRead = 0;
};

class LuaWorkerInterface final: public WorkerInterface {
 public:
  LuaWorkerInterface(LuaRuntime& runtime,
      IoChannelFactory::SubrequestChannel& outbound,
      const kj::HttpHeaderTable& headerTable,
      IoChannelFactory::SubrequestMetadata metadata,
      kj::EntropySource& entropySource,
      kj::Own<SubrequestLimitPolicy> subrequestLimitPolicy,
      kj::Own<workerd::RequestObserver> observer,
      LuaRequestBodyMode requestBodyMode);

  kj::Promise<void> request(kj::HttpMethod method,
      kj::StringPtr url,
      const kj::HttpHeaders& headers,
      kj::AsyncInputStream& requestBody,
      Response& response) override;
  kj::Promise<void> connect(kj::StringPtr host,
      const kj::HttpHeaders& headers,
      kj::AsyncIoStream& connection,
      ConnectResponse& response,
      kj::HttpConnectSettings settings) override;
  kj::Promise<void> prewarm(kj::StringPtr url) override;
  kj::Promise<ScheduledResult> runScheduled(kj::Date scheduledTime, kj::StringPtr cron) override;
  kj::Promise<AlarmResult> runAlarm(kj::Date scheduledTime, uint32_t retryCount) override;
  kj::Promise<CustomEvent::Result> customEvent(kj::Own<CustomEvent> event) override;

 private:
  LuaRuntime& runtime;
  LuaIoContext hostOperations;
  LuaRequestBodyMode requestBodyMode;
};

}  // namespace workerd::rt::luajit
