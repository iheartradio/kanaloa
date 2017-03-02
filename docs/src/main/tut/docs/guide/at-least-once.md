---
layout: docs
title:  "At-least-once Delivery"
section: guide
---

## At-least-once delivery

Kanaloa can be configured to enable at-least-once delivery - it can keep retrying a request until it receives a response from the service for that request within the timeout. This can be configured in `kanaloa.your-proxy.work-settings.at-least-once`.

Apparently, you should only enable this for idempotent requests. In your configuration, you should set the `kanaloa.default-dispatcher.work-settings.service-timeout` carefully. The default value is 1 minute, which means if kanaloa doesn't receive any response from the service in 1 minute it will retry. If you don't want kanaloa to retry this request indefinitely, you should also set `kanaloa.default-dispatcher.work-settings.request-timeout`, kanaloa will treat requests received longer than this timeout as expired and won't retry them. In this case, the caller will get a `WorkTimedOut` response.
Kanaloa's at-least-once delivery is particularly useful when it's also used to load-balance between multiple service instances, in which case if one instance completely fails, kanaloa will retry the failed requests with another instance.

