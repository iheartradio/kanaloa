---
layout: docs
title:  "Circuit Breaker"
section: guide
---

## Circuit Breaker

When the service becomes unresponsive, i.e. requests keep timing out, kanaloa will spot sending work to it for a short period to give it service a chance to "cool down." kanaloa gradually increases the open duration on consecutive timeouts until it hit a maximum open duration.
This can be configured in `kanaloa.your-proxy.circuit-breaker`.

