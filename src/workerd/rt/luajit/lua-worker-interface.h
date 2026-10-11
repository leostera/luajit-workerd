// Copyright (c) 2026 Cloudflare, Inc.
// Licensed under the Apache 2.0 license found in the LICENSE file or at:
//     https://opensource.org/licenses/Apache-2.0

#pragma once

#include <workerd/io/worker-interface.h>
#include <workerd/rt/luajit/lua-runtime.h>

namespace workerd::rt::luajit {

class LuaWorkerInterface final: public WorkerInterface {
 public:
  explicit LuaWorkerInterface(LuaRuntime& runtime): runtime(runtime) {}

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
};

}  // namespace workerd::rt::luajit
