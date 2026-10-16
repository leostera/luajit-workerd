// Copyright (c) 2026 Cloudflare, Inc.
// Licensed under the Apache 2.0 license found in the LICENSE file or at:
//     https://opensource.org/licenses/Apache-2.0

#pragma once

#include <workerd/util/strong-bool.h>

#include <kj/memory.h>

namespace workerd {

class WorkerInterface;

// Whether an outgoing subrequest contributes to the invocation's logical subrequest count.
WD_STRONG_BOOL(CountSubrequest);

// Runtime-neutral observation hook for routed host operations.
class WorkerInvocationObserver {
 public:
  virtual ~WorkerInvocationObserver() noexcept(false) = default;
  virtual kj::Own<WorkerInterface> wrapSubrequestClient(
      kj::Own<WorkerInterface> client, CountSubrequest countSubrequest) = 0;
};

class NoopWorkerInvocationObserver final: public WorkerInvocationObserver {
 public:
  kj::Own<WorkerInterface> wrapSubrequestClient(
      kj::Own<WorkerInterface> client, CountSubrequest) override {
    return kj::mv(client);
  }
};

}  // namespace workerd
