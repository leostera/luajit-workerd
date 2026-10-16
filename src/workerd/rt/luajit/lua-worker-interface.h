// Copyright (c) 2026 Cloudflare, Inc.
// Licensed under the Apache 2.0 license found in the LICENSE file or at:
//     https://opensource.org/licenses/Apache-2.0

#pragma once

#include <workerd/io/io-channels.h>
#include <workerd/io/worker-interface.h>
#include <workerd/io/worker-invocation.h>
#include <workerd/rt/luajit/lua-runtime.h>

namespace workerd::rt::luajit {

// Request-scoped Lua host operations. This composes WorkerInvocation with Lua's route and trace
// metadata, without depending on JS worker or isolate state.
class LuaIoContext final: public LuaHostOperations {
 public:
  LuaIoContext(IoChannelFactory::SubrequestChannel& outbound,
      const kj::HttpHeaderTable& headerTable,
      IoChannelFactory::SubrequestMetadata metadata,
      kj::EntropySource& entropySource,
      kj::Own<SubrequestLimitPolicy> subrequestLimitPolicy,
      kj::Own<WorkerInvocationObserver> observer,
      kj::ArrayPtr<kj::Own<IoChannelFactory::SubrequestChannel>> serviceBindings = nullptr,
      kj::ArrayPtr<kj::Own<IoChannelFactory::SubrequestChannel>> kvBindings = nullptr,
      kj::Maybe<KvOperationLimitPolicy&> kvOperationLimitPolicy = kj::none);
  ~LuaIoContext() noexcept(false) override;

  kj::Promise<LuaHttpResponse> fetch(LuaHttpRequest request) override;
  bool hasRequestBodyStream() const override;
  kj::Promise<kj::Maybe<kj::Array<kj::byte>>> readRequestBody(size_t maxBytes) override;
  kj::Promise<LuaKvGetResponse> kvGet(
      size_t bindingIndex, kj::String key, LuaKvGetOptions options, LuaKvGetMode mode) override;
  kj::Promise<void> kvPut(size_t bindingIndex,
      kj::String key,
      kj::Array<kj::byte> value,
      LuaKvPutOptions options) override;
  kj::Promise<void> kvDelete(size_t bindingIndex, kj::String key) override;
  kj::Promise<LuaKvListResponse> kvList(size_t bindingIndex, LuaKvListOptions options) override;
  void fillRandomBytes(kj::ArrayPtr<kj::byte> output) override;
  kj::String generateRandomUuid() override;
  kj::Maybe<kj::String> getRequestCfJson() const override;

  void setRequestBodyStream(kj::AsyncInputStream& stream);
  void clearRequestBodyStream();

 private:
  IoChannelFactory::SubrequestMetadata makeSubrequestMetadata(TraceContext& tracing);
  kj::Own<kj::HttpClient> getKvHttpClient(size_t bindingIndex,
      KvOperationType operation,
      kj::StringPtr url,
      kj::HttpHeaders& headers,
      TraceContext& tracing);

  kj::Own<SubrequestLimitPolicy> subrequestLimitPolicy;
  kj::Own<WorkerInvocationObserver> observer;
  WorkerInvocation workerInvocation;
  IoChannelFactory::SubrequestChannel& outbound;
  kj::ArrayPtr<kj::Own<IoChannelFactory::SubrequestChannel>> serviceBindings;
  kj::ArrayPtr<kj::Own<IoChannelFactory::SubrequestChannel>> kvBindings;
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
      kj::Own<WorkerInvocationObserver> observer,
      LuaRequestBodyMode requestBodyMode,
      kj::ArrayPtr<kj::Own<IoChannelFactory::SubrequestChannel>> serviceBindings = nullptr,
      kj::ArrayPtr<kj::Own<IoChannelFactory::SubrequestChannel>> kvBindings = nullptr,
      kj::Maybe<KvOperationLimitPolicy&> kvOperationLimitPolicy = kj::none);

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
