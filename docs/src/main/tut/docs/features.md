---
layout: page
title:  Features
section: features
position: 2
---

## Backpressure

The main feature of Kanaloa is to, as a reverse proxy, exert backpressure for a service during oversaturated traffic.
Take the following example, the incoming traffic grows to 200 requests/second, while the capacity remains at 100 requests/second. Kanaloa will reject the excessive 100 requests/second and let the service handle the other 100 requests/second with optimal speed.
![backpressure](../img/backpressure.png)

Kanaloa ensures that the service under pressure works at optimal parallelity, while the optimal performance is unknown beforehand and changes over time. Kanaloa also enables control over latency caused by the queuing. For details see [theories](theories.html)


## Proportional Load Balancing

Kanaloa can load balance between multiple backends each with traffic proportional to its latency. It achieves this through its internal worker pulling model - slow workers pull slowly. This proportional load balancing is especially valuable when one of the backends degrade to an unusable state, in which case the kanaloa will send only minimum traffic towards it (it sends minimum traffic so that kanaloa can detect when the backend recovers back to normal capacity. )


## Circuit Breaker

When the service becomes unresponsive, i.e. requests keep timing out, kanaloa will spot sending work to it for a short period to give it service a chance to "cool down."

## At-least-once delivery

Kanaloa can be configured to enable at-least-once delivery - it can keep retrying a request until it receives a response from the service for that request.  It's useful for idempotent requests with service instance redundancy - if one instance fails to respond to a request, Kanaloa will retry it with another.


## Realtime monitoring

Kanaloa can be configured to report metrics of the service to graphite through statsD.  This enables users to monitor a set of critical metrics (throughput, failure rate, queue length, expected wait time, service process time, the number of concurrent requests, etc.) in real time. It also provides real-time insights into how the kanaloa proxy is working. Here is a screenshot of the realtime monitoring using Grafana UI.
![grafana](../img/grafana.png)
