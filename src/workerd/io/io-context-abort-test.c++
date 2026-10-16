// Copyright (c) 2026 Cloudflare, Inc.
// Licensed under the Apache 2.0 license found in the LICENSE file or at:
//     https://opensource.org/licenses/Apache-2.0
//
// Tests of IoContext::abort() behavior.

#include <workerd/io/io-context.h>
#include <workerd/tests/test-fixture.h>

#include <kj/test.h>

namespace workerd {
namespace {

class PendingWorkerInterface final: public WorkerInterface {
 public:
  explicit PendingWorkerInterface(bool& operationDestroyed)
      : operationDestroyed(operationDestroyed) {}

  kj::Promise<void> request(kj::HttpMethod method,
      kj::StringPtr url,
      const kj::HttpHeaders& headers,
      kj::AsyncInputStream& requestBody,
      Response& response) override {
    KJ_FAIL_REQUIRE("unexpected HTTP request");
  }

  kj::Promise<void> connect(kj::StringPtr host,
      const kj::HttpHeaders& headers,
      kj::AsyncIoStream& connection,
      ConnectResponse& response,
      kj::HttpConnectSettings settings) override {
    KJ_FAIL_REQUIRE("unexpected CONNECT");
  }

  kj::Promise<void> prewarm(kj::StringPtr url) override {
    auto* destroyed = &operationDestroyed;
    return kj::Promise<void>(kj::NEVER_DONE).attach(kj::defer([destroyed]() {
      *destroyed = true;
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
  bool& operationDestroyed;
};

KJ_TEST("JS queued before abort() does not run after it") {
  TestFixture fixture;
  auto request = fixture.newIncomingRequest();
  auto& context = request->getContext();
  auto& waitScope = fixture.getWaitScope();

  bool firstRan = false;
  bool secondRan = false;

  // Simulate outstanding I/O, as in a real request that has pending subrequests, streams, etc.
  // (e.g. awaitIo() attaches a pending event to every promise it awaits). Without this, the
  // registerPendingEvent() call in runImpl() happens to throw the abort exception as a side
  // effect of hang detection, masking the gap this test covers. With an existing PendingEvent
  // alive -- or in an actor context, which doesn't use pending events at all -- it performs no
  // abort check.
  auto pendingEvent = context.registerPendingEvent();

  // Make two back-to-back calls to run(). Both pass the abort check at the top of runSingle()
  // before either has acquired the isolate lock and entered JS. The first run aborts the context,
  // so the second must be refused rather than executed.
  auto first = context.run([&firstRan](Worker::Lock&, IoContext& context) {
    firstRan = true;
    context.abort(KJ_EXCEPTION(FAILED, "jsg.Error: test abort reason"));
  });
  auto second = context.run([&secondRan](Worker::Lock&) { secondRan = true; });

  first.wait(waitScope);
  KJ_EXPECT(firstRan);

  KJ_EXPECT_THROW_MESSAGE("test abort reason", second.wait(waitScope));
  KJ_EXPECT(!secondRan);

  fixture.drainAndDestroy(kj::mv(request));
}

KJ_TEST("JS incoming requests use WorkerInvocation for subrequest admission") {
  uint checkedSubrequests = 0;
  TestFixture fixture(TestFixture::SetupParams{
    .useRealTimers = false,
    .checkedSubrequestCount = checkedSubrequests,
  });
  auto request = fixture.newIncomingRequest();
  bool operationDestroyed = false;

  auto client = request->getContext().getSubrequest(
      [&operationDestroyed](TraceContext&, IoChannelFactory&) -> kj::Own<WorkerInterface> {
    return kj::heap<PendingWorkerInterface>(operationDestroyed);
  },
      IoContext::SubrequestOptions{
        .inHouse = false,
        .wrapMetrics = true,
        .operationName = kj::none,
        .existingTraceContext = kj::none,
      });
  KJ_EXPECT(client.get() != nullptr);
  KJ_EXPECT(checkedSubrequests == 1);

  fixture.drainAndDestroy(kj::mv(request));
}

KJ_TEST("JS actor subrequests survive request handoff") {
  TestFixture fixture(TestFixture::SetupParams{
    .actorId = Worker::Actor::Id(kj::str("invocation-cancellation-test")),
    .useRealTimers = false,
  });
  auto context = fixture.newIoContext();
  auto firstRequest = fixture.newIncomingRequest(*context);
  auto& waitScope = fixture.getWaitScope();

  bool operationDestroyed = false;
  auto hostClient = context->getSubrequest(
      [&operationDestroyed](TraceContext&, IoChannelFactory&) -> kj::Own<WorkerInterface> {
    return kj::heap<PendingWorkerInterface>(operationDestroyed);
  },
      IoContext::SubrequestOptions{
        .inHouse = false,
        .wrapMetrics = true,
        .operationName = kj::none,
        .existingTraceContext = kj::none,
      });
  auto hostOperation = hostClient->prewarm("/"_kj);
  auto secondRequest = fixture.newIncomingRequest(*context);

  fixture.drainAndDestroy(kj::mv(firstRequest));
  KJ_EXPECT(!hostOperation.poll(waitScope));
  KJ_EXPECT(!operationDestroyed);

  hostClient = nullptr;
  KJ_EXPECT(!operationDestroyed);
  hostOperation = nullptr;
  KJ_EXPECT(operationDestroyed);
  fixture.drainAndDestroy(kj::mv(secondRequest));
}

}  // namespace
}  // namespace workerd
