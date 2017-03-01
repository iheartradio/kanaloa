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

### Kanaloa can work with services as an async Function `A => Future[B]`

```tut:silent
import kanaloa._
import scala.concurrent._
import scala.concurrent.duration._
import com.typesafe.config._
import scala.concurrent.ExecutionContext.Implicits.global
val cfg = ConfigFactory.parseString("akka { log-dead-letters = off }")

val proxyFactory = ReverseProxyFactory(cfg)
```

```tut:book
val service = (i: Int) => Future(i.toString)

val proxy: Int => Future[Either[WorkException, String]] = proxyFactory(service)

val result = proxy(42)

Await.result(result, 1.second)
```

### Kanaloa can work with services as `Actor`
```tut:silent
import akka.actor.ActorDSL._
import akka.actor.ActorSystem
implicit val system = ActorSystem("demo")
```
```tut:book
val actorService = actor(new Act {
  become {
    case whateverQuestion ⇒ sender() ! "42"
  }
})

val actorProxy = proxyFactory[String, String](actorService)

val result2 = actorProxy("what's the meaning of life?")

Await.result(result2, 1.second)

```

```tut:invisible

Thread.sleep(100) //job done! let's shutdown the system

system.terminate()

proxyFactory.close()

```


## Proportional Load Balancing

Kanaloa can load balance between multiple backends each with traffic proportional to its latency. It achieves this through its internal worker pulling model - slow workers pull slowly. This proportional load balancing is especially valuable when one of the backends degrade to an unusable state, in which case the kanaloa will send only minimum traffic towards it (it sends minimum traffic so that kanaloa can detect when the backend recovers back to normal capacity. )

To use kanaloa as a load balancer, `HandlerProvider` needs to be implemented to support locating the service instances. An Akka cluster based implementation is out of the box in
```Scala
"com.iheart" %% "kanaloa-cluster" % version
```

## Circuit Breaker

When the service becomes unresponsive, i.e. requests keep timing out, kanaloa will spot sending work to it for a short period to give it service a chance to "cool down."
This can be configured in `kanaloa.your-proxy.circuit-breaker`.

## At-least-once delivery

Kanaloa can be configured to enable at-least-once delivery - it can keep retrying a request until it receives a response from the service for that request.  It's useful for idempotent requests with service instance redundancy - if one instance fails to respond to a request, Kanaloa will retry it with another.
This can be configured in `kanaloa.your-proxy.work-settings.at-least-once`.

## Realtime monitoring

Kanaloa can be configured to report metrics of the service to graphite through statsD.  This enables users to monitor a set of critical metrics (throughput, failure rate, queue length, expected wait time, service process time, the number of concurrent requests, etc.) in real time. It also provides real-time insights into how the kanaloa proxy is working. Here is a screenshot of the realtime monitoring using Grafana UI.
![grafana](../img/grafana.png)

Kanaloa has a built-in statsD reporter that allows users to monitor important metrics in real time.

To enable it, add the following to your config
```
kanaloa {
  statsD {
    host = "localhost"
    port = 8125
  }
  default-dispatcher {
    metrics {
      enabled = on
      statsD {
        namespace = "kanaloa"
      }
    }
  }
}
```

Metrics reporting settings at the dispatcher level, e.g.
```
kanaloa {
  dispatchers {
    example1 {
      metrics.enabled = off
    }
    example2 {
      metrics {
        statsD {
          event-sample-rate = 0.01
        }
      }
    }
  }
}
```
