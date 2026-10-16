// Copyright (c) 2026 Cloudflare, Inc.
// Licensed under the Apache 2.0 license found in the LICENSE file or at:
//     https://opensource.org/licenses/Apache-2.0

#pragma once

#include <workerd/io/worker-invocation-observer.h>

#include <kj/async.h>
#include <kj/common.h>
#include <kj/memory.h>
#include <kj/one-of.h>

namespace workerd {

class WorkerInterface;

// Whether a subrequest targets an API provided internally by the platform.
enum class SubrequestType {
  EXTERNAL,
  IN_HOUSE,
};

// The per-invocation admission policy used before dispatching a subrequest.
class SubrequestLimitPolicy {
 public:
  virtual ~SubrequestLimitPolicy() noexcept(false) = default;
  virtual void checkSubrequest(SubrequestType type) = 0;
};

// The operation class used for a KV request's separate quota accounting.
enum class KvOperationType {
  GET,
  GET_WITH,
  PUT,
  DELETE,
  LIST,
};

class KvOperationLimitPolicy {
 public:
  virtual ~KvOperationLimitPolicy() noexcept(false) = default;
  virtual void checkKvOperation(KvOperationType operation) = 0;
};

// Policies for standalone runtimes whose host does not enforce request quotas.
class UnrestrictedSubrequestLimitPolicy final: public SubrequestLimitPolicy {
 public:
  void checkSubrequest(SubrequestType type) override {}
};

class UnrestrictedKvOperationLimitPolicy final: public KvOperationLimitPolicy {
 public:
  void checkKvOperation(KvOperationType operation) override {}
};

// An invocation whose routed operations are canceled when the invocation ends.
struct RequestScopedCancellationDomain final {
  kj::Own<kj::Canceler> canceler = kj::heap<kj::Canceler>();
};

// The runtime adapter already owns cancellation for the broader execution-context lifetime.
struct ExecutionContextCancellationDomain final {};

// Runtime-neutral host-operation scope for one worker invocation.
//
// The scope borrows its policies and observer and owns its selected cancellation domain. A
// request-scoped domain owns the canceler for routed host operations; an execution-context domain
// leaves cancellation with the runtime adapter's existing async machinery. JavaScript uses the
// latter to preserve actor handoff behavior, while Lua uses the former. Runtime-specific state and
// handler cleanup remain with the adapter.
class WorkerInvocation final {
 public:
  // The caller keeps the borrowed policies and observer alive; the scope owns the cancellation
  // domain passed to it.
  WorkerInvocation(SubrequestLimitPolicy& limitPolicy,
      WorkerInvocationObserver& observer,
      RequestScopedCancellationDomain cancellationDomain,
      kj::Maybe<KvOperationLimitPolicy&> kvOperationLimitPolicy = kj::none)
      : limitPolicy(limitPolicy),
        observer(observer),
        cancellationDomain(kj::mv(cancellationDomain)),
        kvOperationLimitPolicy(kj::mv(kvOperationLimitPolicy)) {}

  WorkerInvocation(SubrequestLimitPolicy& limitPolicy,
      WorkerInvocationObserver& observer,
      ExecutionContextCancellationDomain cancellationDomain,
      kj::Maybe<KvOperationLimitPolicy&> kvOperationLimitPolicy = kj::none)
      : limitPolicy(limitPolicy),
        observer(observer),
        cancellationDomain(kj::mv(cancellationDomain)),
        kvOperationLimitPolicy(kj::mv(kvOperationLimitPolicy)) {}
  KJ_DISALLOW_COPY_AND_MOVE(WorkerInvocation);

  void checkSubrequest(SubrequestType type);
  void checkKvOperation(KvOperationType operation);

  // Apply the selected cancellation policy without adding subrequest observation. Use this for
  // host channels whose accounting is handled by their API-specific path. The returned client must
  // not outlive the selected cancellation domain.
  kj::Own<WorkerInterface> wrapHostClientForCancellation(kj::Own<WorkerInterface> client);

  // Apply subrequest observation and the selected cancellation policy.
  kj::Own<WorkerInterface> wrapSubrequestClient(
      kj::Own<WorkerInterface> client, CountSubrequest countSubrequest);

 private:
  SubrequestLimitPolicy& limitPolicy;
  WorkerInvocationObserver& observer;
  kj::OneOf<RequestScopedCancellationDomain, ExecutionContextCancellationDomain> cancellationDomain;
  kj::Maybe<KvOperationLimitPolicy&> kvOperationLimitPolicy;
};

}  // namespace workerd
