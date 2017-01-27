
[![Join the chat at https://gitter.im/iheartradio/kanaloa](https://badges.gitter.im/Join%20Chat.svg)](https://gitter.im/iheartradio/kanaloa?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge&utm_content=badge)
[![Build Status](https://travis-ci.org/iheartradio/kanaloa.svg)](https://travis-ci.org/iheartradio/kanaloa)
[![Coverage Status](https://coveralls.io/repos/github/iheartradio/kanaloa/badge.svg?branch=master)](https://coveralls.io/github/iheartradio/kanaloa?branch=master)
[![Download](https://api.bintray.com/packages/iheartradio/maven/kanaloa-core/images/download.svg)](https://bintray.com/iheartradio/maven/kanaloa-core/_latestVersion)

# Kanaloa

Add auto backpressure, proportional load balancer and realtime monitor to your service. 

### This library is still pre-beta
Right now we are at 0.4.0, the plan is the stablize the API from 0.5.0 going on. So there will still be major API changes coming. 

### TLDR
When you hit a service that has a max capacity of 100 request per second with 200 requests per second, you get:

**Without Kanaloa**
Latency increases until few requests get handled within accetable time
![Without](https://cloud.githubusercontent.com/assets/83257/17779409/8bc82d2e-6535-11e6-8917-f54ef634ad80.png)

**With Kanaloa**
Latency is controlled, half of requests are handled timely, the other half are rejected immediately. 
![withKanaloa](https://cloud.githubusercontent.com/assets/83257/17779483/c88e3f5a-6535-11e6-8594-4df40771372c.png)

Kanaloa automatically figures out the capacity of your service and let it handles at maximum capacity, rejects requests above it. 
![rejections](https://cloud.githubusercontent.com/assets/83257/17779633/55339568-6536-11e6-9bdd-27723927f076.png)

Kanaloa also adaptes to your backend capacity change automatically. 

### Behind the scene

 Kanaloa work dispatchers sit in front of your service and provides auto back pressure through the following means:
  1. **Autothrottle** - it dynamically figures out the optimal number of concurreny your service can handle, and make sure that at any given time your service handles no more than that number of concurrent requests. This simple mechanism was also ported and contributed to Akka as [Optimal Size Exploring Resizer](http://doc.akka.io/docs/akka/2.4.1/scala/routing.html#Optimal_Size_Exploring_Resizer) with some limitations. See [details of the algorithm below](#implAutothrottle).
  2. **PIE** - A traffic regulator based on the PIE algo (Proportional Integral controller Enhanced) suggested in this paper https://www.ietf.org/mail-archive/web/iccrg/current/pdfB57AZSheOH.pdf by Rong Pan and his collaborators.  See [details of the algorithm below](#implPIE).
  3. **Circuit breaker** - when error rate from your service goes above a certain threshold, the kanaloa will slow down sending work to them for a short period of time to give your service a chance to "cool down".
  4. **Real-time monitoring** - a built-in statsD reporter allows you to monitor a set of critical metrics (throughput, failure rate, queue length, expected wait time, service process time, number of concurrent requests, etc) in real time. It also provides real-time insights into how kanaloa dispatchers are working. An example on Grafana:
  ![Dashboard](https://cloud.githubusercontent.com/assets/83257/17714327/449e09d2-63cd-11e6-8527-3ee9e9bbf755.png)


### Get started

#### Install dependency
```
resolvers += Resolver.jcenterRepo

libraryDependencies +=  "com.iheart" %% "kanaloa-core" % "0.4.0"
```

#### Config
An example of a `my-dispatcher`
```
kanaloa {
  dispatchers {
    my-service1 {
      worker-pool {
       starting-pool-size = 8
      }
      default-work-settings {
       service-timeout = 3s
      }
    }
  }
}
```
For more configuration settings and their documentation please see the [reference configuration](src/main/resources/reference.conf)

#### Usage

There are two types kanaloa dispatchers: `PushingDispatcher` and `PullingDispatcher`.
`PushingDispatcher` takes work requests when you send message to it.
```Scala
val system = ActorSystem()

// suppose you wrote your service in an actor,
// which takes a SomeWork(someRequest) message and
// replies SuccessResult(someResults) when it succeeds
val serviceActor = system.actorOf(MyServiceActor.props)

val dispatcher =
  system.actorOf(PushingDispatcher.props(
    name = "my-service1",
    serviceActor
  ) {
    case SuccessResult(r) => Right(r)  // ResultChecker that tells kanaloa if the request is handled succesffully
    case _ => Left("something bad happened")
  })

dispatcher ! SomeWork("blahblah") //dispatcher replies the result (whatever wrapped in the SuccessResult) back.

```

`PullingPatcher` pulls work from an iterator, ideal when you have control over the source of the work (e.g. a task iterates through your DB)
```Scala
//assume you have the same system and serviceActor as above.

//unrealistic example of an iterator you would pass into a PullingDispatcher
val iterator = List(SomeWork("work1"), SomeWork("work2"), SomeWork("work3")).iterator

val dispatcher =
  system.actorOf(PullingDispatcher.props(
    name = "my-service2",
    iterator,
    serviceActor
  ) {
    case SuccessResult(r) => Right(r)     // this is your ResultChecker which tell kanaloa if the request is handled succesffully
    case _ => Left("something bad happened")
  })

// dispatcher will pull all work in dispatch them to service and shutdown itself when all done. 

```

We used an [ActorRef](http://doc.akka.io/api/akka/snapshot/index.html#akka.actor.ActorRef) as service here, Kanaloa also supports [Props](http://doc.akka.io/api/akka/snapshot/index.html#akka.actor.Props) and `T => Future[R]` function as service. You can use any `T` as service if you implement an implicit `T => Backend` where `Backend` is a simple trait:
```
trait Backend { def apply(af: ActorRefFactory): ActorRef }
```

### StatsD monitor

Kanaloa has a built-in statsD reporter that allows users to monitor important metrics in real time.

#### Config StatsD
Add following to your config
```
kanaloa {
  default-dispatcher {
    metrics {
      enabled = on
      statsD {
        host = "localhost" #host of your statsD server
        port = 8125
      }
    }
  }
}
```

Metrics reporting settings at the dispatcher level, e.g.
```
kanaloa {
  dispatchers {
    example {
      metrics.enabled = off
    }
    example2 {
      metrics {
        statsD {
          eventSampleRate = 0.01
        }
      }
    }
  }
}
```
For more settings please see the [reference configuration](src/main/resources/reference.conf)

#### Visualize with Grafana

We provide a [grafana dashboard](grafana/dashboard.json) if you are using grafana for statsD visualization.
We also provide a [docker image](https://github.com/iheartradio/docker-grafana-graphite) with which you can quickly get up and running a statsD server and visualization web app. Please follow the instructions there.


### <a name="implAutothrottle"></a>Implementation detail for Autothrottle

*Disclosure: some of the following descriptions were adapted from the documentation of Akka's [OptimalSizeExploringResizer](http://doc.akka.io/docs/akka/2.4.1/scala/routing.html#Optimal_Size_Exploring_Resizer), which was also written by the original author of this document.*

Behind the scene kanaloa dispatchers create a set of workers that pull work on behave of your service. These workers wait for result coming back from the service before they accept more work from the dispatcher. This way it thorttles the number of concurrent requests the service handles. The autothrottle constantly search for the optimal concurrency, i.e. the just right concurrency that allows backend to provide maximum capactity. For example, if the service achieves maximum output by handling 30 requests at the same time, kanaloa will make sure it doesn't handle more than 30 requests concurrently. Typcially, hitting a service with above its optimum  concurrency will result in either higher latency (the extra requests have to wait in a queue) or lower throughput (extra concucrrent requests have to compete for limited resource causing contention.) 

The autothrottle keeps track of throughput and latency at each pool size and perform the following three resizing operations (one at a time) periodically:

1. Downsize if it hasn't seen all workers ever fully utilized for a period of time.
2. Explore to a random nearby pool size to try and collect throughput metrics.
3. Optimize to a nearby pool size with a better (than any other nearby sizes) throughput and latency metrics.

When the pool is fully-utilized (i.e. all workers are busy), it randomly chooses between exploring and optimizing. When the pool has not been fully-utilized for a period of time, it will downsize the pool to the last seen max utilization multiplied by a configurable ratio.

By constantly exploring and optimizing, the resizer will eventually walk to the optimal size and remain nearby. When the optimal size changes it will start walking towards the new one.

If autothrottle is all you need, you should consider using [OptimalSizeExploringResizer](http://doc.akka.io/docs/akka/2.4.1/scala/routing.html#Optimal_Size_Exploring_Resizer) in an Akka router. (The caveat is that you need to make sure that your routees block on handling messages, i.e. they can’t simply asynchronously pass work to the backend service).


### <a name="implPIE"></a>Implementation cetail for PIE
A traffic regulator based on the PIE algo (Proportional Integral controller Enhanced)
suggested in this paper https://www.ietf.org/mail-archive/web/iccrg/current/pdfB57AZSheOH.pdf by Rong Pan and his collaborators.
The algo drop request with a probability, here is the pseudocode:

Every update interval `Tupdate`

  1. Estimation current queueing delay
    
     ```
     currentDelay = queueLength / averageDequeueRate
     ``` 
  
  2. Based on current drop probability, p, determine suitable step scales:
  
     ```
     if p < 1%       :  α = α΄ / 8, β = β΄ / 8
     else if p < 10% :  α = α΄ / 2, β = β΄  2
     else            :  α = α΄,  β = β΄
     ```
  3. Calculate drop probability as:
  
     ```
     p = p
        + α * (currentDelay - referenceDelay) / referenceDelay
        + β * (currentDelay - oldDelay) / referenceDelay
     ```   
  
  4. Update previous delay sample rate as
  
     ```
     OldDelay - currentDelay
     ```


The regulator allows for a burst, here is the calculation

  1. When enqueueing
  
     ```
     if burstAllowed > 0
       enqueue request bypassing random drop
     ```
  2. upon `Tupdate`
  
     ```
       if p == 0 and currentDelay < referenceDelay / 2 and oldDelay < referenceDelay / 2
        burstAllowed = maxBurst
     else
        burstAllowed = burstAllowed - timePassed (roughly Tupdate)
     ```


### Contribute

Any contribution and feedback is more than welcome.


### Special Thanks

The autothrottle algorithm was first suggested by @richdougherty. @ktoso kindly reviewed this library and provided valuable feedback.



