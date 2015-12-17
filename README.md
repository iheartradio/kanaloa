
[![Join the chat at https://gitter.im/iheartradio/kanaloa](https://badges.gitter.im/Join%20Chat.svg)](https://gitter.im/iheartradio/kanaloa?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge&utm_content=badge)
[![Build Status](https://travis-ci.org/iheartradio/kanaloa.svg)](https://travis-ci.org/iheartradio/kanaloa)
[![Download](https://api.bintray.com/packages/iheartradio/maven/kanaloa/images/download.svg)](https://bintray.com/iheartradio/maven/kanaloa/_latestVersion)

# Kanaloa


#### A set of work dispatchers implemented using Akka actors
Note: kanaloa work dispatchers are not Akka [MessageDispatcher](http://doc.akka.io/docs/akka/snapshot/scala/dispatchers.html).

### Motivation
 Kanaloa work dispatchers sit in front of your service and dispatch received work to them. They make your service more resilient through the following means:
  1. **Auto scaling** - it dynamically figures out the optimal number of concurrent requests your service can handle, and make sure that at any given time your service handles no more than that number of concurrent requests. This simple mechanism was also ported and contributed to Akka's [Optimal Size Exploring Resizer](http://doc.akka.io/docs/akka/2.4.1/scala/routing.html#Optimal_Size_Exploring_Resizer) with some limitations. See details of the algorithm below.
  2. **Back pressure control** - this control is [Little's law](https://en.wikipedia.org/wiki/Little%27s_law) inspired. It rejects requests when estimated wait time for which exceeds a certain threshold.
  3. **Circuit breaker** - when error rate from your service goes above a certain threshold, the kanaloa dispatcher stops all requests for a short period of time to give your service a chance to "cool down".
  4. **Real-time monitoring** - a built-in statsD reporter allows you to monitor a set of critical metrics (throughput, failure rate, queue length, expected wait time, service process time, number of concurrent requests, etc) in real time. It also provides real-time insights into how kanaloa dispatchers are working. An example on Grafana:
  ![Dashboard](https://github.com/iheartradio/docker-grafana-graphite/blob/master/dashboard.png)

For the detailed algorithm please see the [implementation detail](#impl) below.

### Get started

#### Install dependency
```
resolvers += Resolver.jcenterRepo

libraryDependencies +=  "com.iheart" %% "kanaloa" % "0.2.0"
```

#### Config
An example of a `my-dispatcher`
```
kanaloa {
  dispatchers {
    my-service1 {
      workerPool {
       startingPoolSize = 8
      }
      workTimeout = 3s
      circuitBreaker {
        errorRateThreshold = 0.7
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
    case _ => Left("shit happened")
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
    case _ => Left("shit happened")
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
  metrics {
    statsd {
      host = "localhost" #host of your statsD server
      port = 8125
    }
  }
}
```
For more settings please see the [reference configuration](src/main/resources/reference.conf)

#### Visualize with Grafana

We provide a [grafana dashboard](grafana/dashboard.json) if you are using grafana for statsD visualization.
We also provide a [docker image](https://github.com/iheartradio/docker-grafana-graphite) with which you can quickly get up and running a statsD server and visualization web app. Please follow the instructions there.


### <a name="impl"></a>Implementation Detail

*Disclosure: some of the following descriptions were adapted from the documentation of Akka's [OptimalSizeExploringResizer](http://doc.akka.io/docs/akka/2.4.1/scala/routing.html#Optimal_Size_Exploring_Resizer), which was also written by the original author of this document.*

Behind the scene kanaloa dispatchers create a set of workers that work with your services. These workers wait for result coming back from the service before they accept more work from the dispatcher. This way it controls the number of concurrent requests dispatchers send to services. It auto-scales the work pool to an optimal size that provides the highest throughput.

This auto-scaling works best when you expect the pool size to performance function to be a convex function, with which you can find a global optimal by walking towards a better size. For example, a CPU bound service may have an optimal worker pool size tied to the CPU cores available. When your service is IO bound, the optimal size is bound to optimal number of concurrent connections to that IO service - e.g. a 4 node Elasticsearch cluster may handle 4-8 concurrent requests at optimal speed.

The dispatchers keep track of throughput at each pool size and perform the following three resizing operations (one at a time) periodically:

1. Downsize if it hasn't seen all workers ever fully utilized for a period of time.
2. Explore to a random nearby pool size to try and collect throughput metrics.
3. Optimize to a nearby pool size with a better (than any other nearby sizes) throughput metrics.

When the pool is fully-utilized (i.e. all workers are busy), it randomly chooses between exploring and optimizing. When the pool has not been fully-utilized for a period of time, it will downsize the pool to the last seen max utilization multiplied by a configurable ratio.

By constantly exploring and optimizing, the resizer will eventually walk to the optimal size and remain nearby. When the optimal size changes it will start walking towards the new one.


### Contribute

Any contribution and feedback is more than welcome.


### Special Thanks

The auto scaling algorithm was first suggested by @richdougherty. @ktoso kindly reviewed this library and provided valuable feedback.



