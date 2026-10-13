// Copyright (c) 2026 Cloudflare, Inc.
// Licensed under the Apache 2.0 license found in the LICENSE file or at:
//     https://opensource.org/licenses/Apache-2.0

#include "request-scope.h"

#include "observer.h"
#include "worker-interface.h"

namespace workerd {

void RequestScope::checkSubrequest(SubrequestType type) {
  limitPolicy.checkSubrequest(type);
}

void RequestScope::checkKvOperation(KvOperationType operation) {
  if (kvOperationLimitPolicy != nullptr) {
    kvOperationLimitPolicy->checkKvOperation(operation);
  }
}

kj::Own<WorkerInterface> RequestScope::wrapSubrequestClient(
    kj::Own<WorkerInterface> client, CountSubrequest countSubrequest) {
  return observer.wrapSubrequestClient(kj::mv(client), countSubrequest);
}

}  // namespace workerd
