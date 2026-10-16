// Copyright (c) 2026 Cloudflare, Inc.
// Licensed under the Apache 2.0 license found in the LICENSE file or at:
//     https://opensource.org/licenses/Apache-2.0

#include <workerd/io/worker-interface.h>
#include <workerd/io/worker-invocation.h>

#include <kj/async-io.h>
#include <kj/test.h>

namespace workerd {
namespace {

class RecordingPolicy final: public SubrequestLimitPolicy {
 public:
  void checkSubrequest(SubrequestType type) override {
    ++calls;
    lastType = type;
  }

  size_t calls = 0;
  SubrequestType lastType = SubrequestType::EXTERNAL;
};

class RecordingKvPolicy final: public KvOperationLimitPolicy {
 public:
  void checkKvOperation(KvOperationType operation) override {
    ++calls;
    lastOperation = operation;
  }

  size_t calls = 0;
  KvOperationType lastOperation = KvOperationType::GET;
};

class RecordingObserver final: public WorkerInvocationObserver {
 public:
  kj::Own<WorkerInterface> wrapSubrequestClient(
      kj::Own<WorkerInterface> client, CountSubrequest countSubrequest) override {
    ++calls;
    lastCount = countSubrequest;
    return kj::mv(client);
  }

  size_t calls = 0;
  CountSubrequest lastCount = CountSubrequest::NO;
};

class PendingWorkerInterface final: public WorkerInterface {
 public:
  explicit PendingWorkerInterface(bool& canceled): canceled(canceled) {}

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

  kj::Promise<bool> test() override {
    auto* canceledFlag = &canceled;
    return kj::Promise<bool>(kj::NEVER_DONE).attach(kj::defer([canceledFlag]() {
      *canceledFlag = true;
    }));
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

class EmptyInputStream final: public kj::AsyncInputStream {
 public:
  kj::Promise<size_t> tryRead(void* buffer, size_t minBytes, size_t maxBytes) override {
    return size_t(0);
  }
};

class TestResponse final: public kj::HttpService::Response {
 public:
  kj::Own<kj::AsyncOutputStream> send(uint statusCode,
      kj::StringPtr statusText,
      const kj::HttpHeaders& headers,
      kj::Maybe<uint64_t> expectedBodySize) override {
    return kj::heap<kj::NullStream>();
  }

  kj::Own<kj::WebSocket> acceptWebSocket(const kj::HttpHeaders& headers) override {
    KJ_FAIL_ASSERT("unexpected WebSocket response");
  }
};

KJ_TEST("worker invocation owns host admission, observation, and request cancellation") {
  auto io = kj::setupAsyncIo();
  RecordingPolicy subrequestPolicy;
  RecordingKvPolicy kvPolicy;
  RecordingObserver observer;
  bool canceled = false;
  bool internalCallCanceled = false;
  kj::Maybe<kj::Promise<void>> pending;
  kj::Maybe<kj::Promise<bool>> internalPending;

  {
    WorkerInvocation invocation(
        subrequestPolicy, observer, RequestScopedCancellationDomain{}, kvPolicy);
    invocation.checkSubrequest(SubrequestType::IN_HOUSE);
    invocation.checkKvOperation(KvOperationType::PUT);
    KJ_EXPECT(subrequestPolicy.calls == 1);
    KJ_EXPECT(subrequestPolicy.lastType == SubrequestType::IN_HOUSE);
    KJ_EXPECT(kvPolicy.calls == 1);
    KJ_EXPECT(kvPolicy.lastOperation == KvOperationType::PUT);

    auto client = invocation.wrapSubrequestClient(
        kj::heap<PendingWorkerInterface>(canceled), CountSubrequest::YES);
    auto internalClient = invocation.wrapHostClientForCancellation(
        kj::heap<PendingWorkerInterface>(internalCallCanceled));
    KJ_EXPECT(observer.calls == 1);
    KJ_EXPECT(observer.lastCount == CountSubrequest::YES);

    kj::HttpHeaderTable headerTable;
    kj::HttpHeaders headers(headerTable);
    EmptyInputStream requestBody;
    TestResponse response;
    pending = client->request(kj::HttpMethod::GET, "/"_kj, headers, requestBody, response);
    internalPending = internalClient->test();
    KJ_EXPECT(!KJ_ASSERT_NONNULL(pending).poll(io.waitScope));
    KJ_EXPECT(!KJ_ASSERT_NONNULL(internalPending).poll(io.waitScope));
  }

  KJ_EXPECT(canceled);
  KJ_EXPECT(internalCallCanceled);
  KJ_IF_SOME(operation, kj::mv(pending)) {
    auto rejected = kj::mv(operation).then([]() { return false; }, [](kj::Exception&& exception) {
      return exception.getType() == kj::Exception::Type::DISCONNECTED;
    });
    KJ_EXPECT(rejected.wait(io.waitScope));
  } else {
    KJ_FAIL_ASSERT("request-scoped client did not return a pending operation");
  }
  KJ_IF_SOME(operation, kj::mv(internalPending)) {
    auto rejected =
        kj::mv(operation).then([](bool) { return false; }, [](kj::Exception&& exception) {
      return exception.getType() == kj::Exception::Type::DISCONNECTED;
    });
    KJ_EXPECT(rejected.wait(io.waitScope));
  } else {
    KJ_FAIL_ASSERT("internal host client did not return a pending operation");
  }
}

}  // namespace
}  // namespace workerd
