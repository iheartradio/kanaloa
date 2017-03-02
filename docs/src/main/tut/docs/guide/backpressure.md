---
layout: docs
title:  "Backpressure"
section: guide
---

## Backpressure

The main feature of Kanaloa is to, as a reverse proxy, exert backpressure for a service during oversaturated traffic.
Take the following example, the incoming traffic grows to 200 requests/second, while the capacity remains at 100 requests/second. Kanaloa will reject the excessive 100 requests/second and let the service handle the other 100 requests/second with optimal speed.
![backpressure](../../img/backpressure.png)

Kanaloa ensures that the service under pressure works at optimal parallelity, while the optimal performance is unknown beforehand and changes over time. Kanaloa also enables control over latency caused by the queuing. For details see [theories](../theories.html)

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

