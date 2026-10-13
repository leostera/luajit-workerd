// Copyright (c) 2026 Cloudflare, Inc.
// Licensed under the Apache 2.0 license found in the LICENSE file or at:
//     https://opensource.org/licenses/Apache-2.0

#pragma once

#include <kj/common.h>
#include <kj/memory.h>

namespace workerd {

class CountSubrequest;
class RequestObserver;
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

// Shared per-invocation services for subrequest admission and observation.
//
// Runtime-specific contexts own a RequestScope and supply the policy and observer that apply to
// that invocation. Route selection, trace construction, and runtime-specific resource ownership
// remain with the caller.
class RequestScope final {
 public:
  // The caller owns both dependencies and must keep them alive for the scope's lifetime.
  RequestScope(SubrequestLimitPolicy& limitPolicy,
      RequestObserver& observer,
      KvOperationLimitPolicy* kvOperationLimitPolicy = nullptr)
      : limitPolicy(limitPolicy),
        observer(observer),
        kvOperationLimitPolicy(kvOperationLimitPolicy) {}
  KJ_DISALLOW_COPY_AND_MOVE(RequestScope);

  void checkSubrequest(SubrequestType type);
  void checkKvOperation(KvOperationType operation);
  kj::Own<WorkerInterface> wrapSubrequestClient(
      kj::Own<WorkerInterface> client, CountSubrequest countSubrequest);

 private:
  SubrequestLimitPolicy& limitPolicy;
  RequestObserver& observer;
  KvOperationLimitPolicy* kvOperationLimitPolicy;
};

}  // namespace workerd
