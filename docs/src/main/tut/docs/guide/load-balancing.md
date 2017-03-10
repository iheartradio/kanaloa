---
layout: docs
title:  "Kanaloa and Load Balancing"
section: guide
---

## Proportional Load Balancing

Naturally, when you have multiple instances of the service, you need to load balance between them. If you use your own load balancer, make sure you place a kanaloa proxy between each instance and the load balancer. It's not recommended to put kanaloa in front of the load balancer because it's more efficient and accurate for kanaloa to throttle each different instance than all of them combined as a whole.

Kanaloa itself can load balance between multiple instances. It throttles each instance separately, and send traffic proportional to their latencies.  It achieves this proportional traffic allocation through its internal worker pulling model - slow workers pull slowly. Kanaloa load balancing is especially valuable when one of the backends degrade to an unusable state, in which case the kanaloa will send only minimum traffic towards it (it sends minimum traffic so that kanaloa can detect when the backend recovers back to normal capacity. )

To use kanaloa as a load balancer, `HandlerProvider` needs to be implemented to support locating the service instances. An Akka cluster based implementation is out of the box in

```Scala
"com.iheart" %% "kanaloa-cluster" % version
```
