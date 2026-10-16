// Copyright (c) 2026 Cloudflare, Inc.
// Licensed under the Apache 2.0 license found in the LICENSE file or at:
//     https://opensource.org/licenses/Apache-2.0

#include "worker-invocation.h"

#include "worker-interface.h"

namespace workerd {
namespace {

// Route each operation through the invocation canceler. The wrapper must not outlive that
// request-scoped cancellation domain.
class CancellableWorkerInterface final: public WorkerInterface {
 public:
  CancellableWorkerInterface(kj::Own<WorkerInterface> inner, kj::Canceler& canceler)
      : inner(kj::mv(inner)),
        canceler(canceler) {}

  kj::Promise<void> request(kj::HttpMethod method,
      kj::StringPtr url,
      const kj::HttpHeaders& headers,
      kj::AsyncInputStream& requestBody,
      Response& response) override {
    return canceler.wrap(inner->request(method, url, headers, requestBody, response));
  }

  kj::Promise<void> connect(kj::StringPtr host,
      const kj::HttpHeaders& headers,
      kj::AsyncIoStream& connection,
      ConnectResponse& response,
      kj::HttpConnectSettings settings) override {
    return canceler.wrap(inner->connect(host, headers, connection, response, settings));
  }

  kj::Promise<void> prewarm(kj::StringPtr url) override {
    return canceler.wrap(inner->prewarm(url));
  }

  kj::Promise<ScheduledResult> runScheduled(kj::Date scheduledTime, kj::StringPtr cron) override {
    return canceler.wrap(inner->runScheduled(scheduledTime, cron));
  }

  kj::Promise<AlarmResult> runAlarm(kj::Date scheduledTime, uint32_t retryCount) override {
    return canceler.wrap(inner->runAlarm(scheduledTime, retryCount));
  }

  kj::Promise<kj::Maybe<kj::Date>> abandonAlarm(kj::Date scheduledTime) override {
    return canceler.wrap(inner->abandonAlarm(scheduledTime));
  }

  kj::Promise<bool> test() override {
    return canceler.wrap(inner->test());
  }

  kj::Promise<CustomEvent::Result> customEvent(kj::Own<CustomEvent> event) override {
    return canceler.wrap(inner->customEvent(kj::mv(event)));
  }

 private:
  kj::Own<WorkerInterface> inner;
  kj::Canceler& canceler;
};

}  // namespace

void WorkerInvocation::checkSubrequest(SubrequestType type) {
  limitPolicy.checkSubrequest(type);
}

void WorkerInvocation::checkKvOperation(KvOperationType operation) {
  KJ_IF_SOME(policy, kvOperationLimitPolicy) {
    policy.checkKvOperation(operation);
  }
}

kj::Own<WorkerInterface> WorkerInvocation::wrapHostClientForCancellation(
    kj::Own<WorkerInterface> client) {
  KJ_SWITCH_ONEOF(cancellationDomain) {
    KJ_CASE_ONEOF(requestScoped, RequestScopedCancellationDomain) {
      return kj::heap<CancellableWorkerInterface>(kj::mv(client), *requestScoped.canceler);
    }
    KJ_CASE_ONEOF(executionContext, ExecutionContextCancellationDomain) {
      return kj::mv(client);
    }
  }
  KJ_UNREACHABLE;
}

kj::Own<WorkerInterface> WorkerInvocation::wrapSubrequestClient(
    kj::Own<WorkerInterface> client, CountSubrequest countSubrequest) {
  auto observedClient = observer.wrapSubrequestClient(kj::mv(client), countSubrequest);
  return wrapHostClientForCancellation(kj::mv(observedClient));
}

}  // namespace workerd
