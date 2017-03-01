---
layout: home
technologies:
 - first: ["Scala", "Kanaloa is completely written in Scala"]
 - second: ["Akka", "Kanaloa is implemented using Akka"]
 - third: ["Graphite", "Kanaloa uses Graphite to provide realtime monitoring"]
---

# Kanaloa

Kanaloa is library to make more resilient a service as a reverse proxy by providing:

1. the ability to exert backpressure during oversaturated traffic (incoming traffic exceeds capacity).
2. circuit breaker
3. At-least-once delivery
4. real-time monitor
5. proportional load balancing (if you see appropriate)
