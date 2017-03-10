---
layout: home
technologies:
 - first: ["Scala", "Kanaloa is completely written in Scala"]
 - second: ["Akka", "Kanaloa is implemented using Akka"]
 - third: ["Graphite", "Kanaloa uses Graphite to provide realtime monitoring"]
---

# ![icon](./img/navbar_brand2x.png)  Kanaloa

[![Join the chat at https://gitter.im/iheartradio/kanaloa](https://badges.gitter.im/Join%20Chat.svg)](https://gitter.im/iheartradio/kanaloa?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge&utm_content=badge)
[![Build Status](https://travis-ci.org/iheartradio/kanaloa.svg)](https://travis-ci.org/iheartradio/kanaloa)
[![Coverage Status](https://coveralls.io/repos/github/iheartradio/kanaloa/badge.svg?branch=0.5.x)](https://coveralls.io/github/iheartradio/kanaloa?branch=0.5.x)
[![Download](https://api.bintray.com/packages/iheartradio/maven/kanaloa-core/images/download.svg)](https://bintray.com/iheartradio/maven/kanaloa-core/_latestVersion)



Kanaloa is library to make more resilient a service as a reverse proxy by providing:


1. [the ability to exert backpressure during oversaturated traffic (incoming traffic exceeds capacity).](./docs/guide/backpressure.html)
2. [circuit breaker](./docs/guide/circuit-breaker.html)
3. [At-least-once delivery](./docs/guide/at-least-once.html)
4. [real-time monitor](./docs/guide/monitor.html)
5. [proportional load balancing (if you see appropriate)](./docs/guide/load-balancing.html)

For the motivation and methodologies of this library, go to [theories](./docs/theories.html).
