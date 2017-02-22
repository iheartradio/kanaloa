---
layout: page
title:  Theories
section: theories
position: 2
---


## Motivations


Kanaloa is designed mainly to help your service to deal with traffic overflow, i.e. when the incoming traffic exceeds the capacity of your service. When traffic overflow occurs, all the excessive traffic will have to wait in some queues in the service. Since the queue length usually grows exponentially, the latencies also grow exponentially to the point where no requests get served within the acceptable time frame. Worse, if the incoming traffic doesn't significantly slow down due to the high latency, e.g. when the users keep retrying regardless of the failed requests, the service will probably experience out-of-memory melt-down further down the line.

There are two types of traffic overflow scenarios which I will call as **positive overflow** and **negative overflow** respectively.
**Positive overflow** refers to the ones that occur when the incoming traffic increases to above the capacity;  **negative overflow** refers to the ones that occur when the capacity is so negatively affected that it's below the incoming traffic.

One common way to deal with positive overflow is to set a traffic threshold according to known capacity and shed the excessive traffic above it.  The capacity of the service has to be measured beforehand using some stress tests. Obviously, this approach won't work in the negative overflow scenario in which the capacity is changing and can no longer be measured beforehand. Negative overflow happens more often complex service system, such as microservices, where serving a request involves multiple levels of internal and/or external dependencies. To name a few, network anomaly, external service degradation and disruptive competition of internal resources all can negatively affect the capacity of your service.

One approach of exerting backpressure with dynamic capacity is pull-based backpressure. The basic idea is to have each worker in the processing chain "pull" work from the source. A recent example of this approach is the [Akka Stream](http://doc.akka.io/docs/akka/2.4/scala/stream/index.html). The main implementation cost of this approach is that **all** processing steps must implement the pull mechanism based on their capacity, this may be challenging for a service that relies on other services, e.g. a database, whose interface is push based.

## The Kanaloa solution

Kanaloa took a different approach to exert backpressure with changing capacity. It's a reverse proxy in front of the service that treats the service as a black box. It adaptively throttles the concurrent requests the service handles and thus limits the number of requests waiting in the queues internal to the service.  This throttle is adaptive in the sense that, during an overflow, the concurrent requests the service handles is constantly being optimized according to the performance (measured by throughput and latency) Kanaloa observes from it. With this throttle in place, Kanaloa buffers the excessive traffic in its own queue with which it applies a traffic control mechanism called [Proportional Integral controller Enhanced (PIE)](https://www.ietf.org/mail-archive/web/iccrg/current/pdfB57AZSheOH.pdf ).
Here is the diagram:
![Typeclass hierarchy](http://g.gravizo.com/g?
  digraph G {
    margin="0.3"
    rankdir=LR
    node [shape=box,style="rounded"]
    incoming [label="incoming traffic"]
    incoming -> PIE
    subgraph cluster_s3{
    queue [label="Kanaloa queue"]
    throttle [label="adaptive concurrency throttle"]
    PIE [label="PIE regulator"]
     PIE -> queue
     queue -> PIE [style="dotted"]
     queue -> throttle
     graph[label="Kanaloa",style="rounded"]
    }
    throttle-> service
  }
)

The traffic goes through Kanaloa towards the service. The adaptive throttle in Kanaloa makes sure that the concurrent requests the service handles are bounded. When the concurrent requests at the service reach the bound, excessive requests will wait in the kanaloa queue. The PIE regulator rejects requests with a possibility based on the waiting time in the kanaloa queue.

## The Adaptive Concurrency Throttle

To understand how adaptive concurrency throttle in Kanaloa works, we need to understand a bit more about how the service deal with concurrency. Usually, the service can handle only up to a certain number of concurrent request in parallel with optimal speed. When it receives more requests above the optimal concurrency, there will be some queuing and possibly contention inside the service. In another sentence, sending more requests to the service beyond the optimal concurrency isn't going to improve the situation during traffic overflow. The enqueuing of the requests in the service will increase the latency. Furthermore, if the service implementation doesn't control contention well, it will degrade the performance exponentially.

The kanaloa adaptive throttle controls the concurrent request the service handles by having a set of workers that pull work on behave of the service. These workers wait for result coming back from the service before they accept more work from the dispatcher. This way, the number of concurrent requests the service handles cannot exceed the number of the workers, i.e. the worker pool size.
The kanaloa adaptive throttle then keeps track of throughput and latency at each pool size and optimizes the worker pool size (i.e. the concurrency) towards the one that provides the best performance (measured by throughput and latency). It achieves this by performing the following two resizing operations (one at a time) periodically:

1. Explore to a random nearby pool size to try and collect performance metrics.
2. Optimize to a nearby pool size with a better performance metrics than any other nearby sizes.

By constantly exploring and optimizing, the resizer will eventually walk to the optimal size and remain nearby. The optimal concurrency number changes when the environment changes, e.g. when the service recovers from degradation, or when the service capacity is ramped up with more instance deployment.  In such cases, the adaptive throttle will start walking towards the new optimal worker pool size.

## The PIE regulator

Now that the number of concurrent requests handled by the service is throttled, the excessive requests go to a queue created inside kanaloa. Kanaloa monitors this queue and apply a traffic regulation algorithm called PIE  (Proportional Integral controller Enhanced)
suggested in [this paper by Rong Pan and his collaborators](https://www.ietf.org/mail-archive/web/iccrg/current/pdfB57AZSheOH.pdf).

Based on Little's law, PIE regulator controls the time for which the requests wait in the kanaloa queue by dropping requests with a probability according to the current queue length and historical dequeue speed. Here is the pseudocode of the algorithm:

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


## Summary

Kanaloa protects your service against traffic overflow by adaptively throttles the concurrent requests the service handles and regulate the incoming traffic using a Little's law based algorithm called PIE that drops requests based on estimated wait time.  The main advantages of Kanaloa are:
1. it requires little knowledge of the service capacity beforehand - it learns it on the fly.
2. it's adaptive to the dynamic capacity of the service and thus is suitable to deal with both positive and negative traffic overflows.
3. it's a reverse proxy in front of the service. No implementation is needed at the service side.
