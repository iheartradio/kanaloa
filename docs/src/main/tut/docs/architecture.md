---
layout: page
title:  Architecture
section: architecture
position: 4
---


## The architecture of the internal implementation

Kanaloa is developed using Akka. This document record the architecture of the actors, mainly for the purpose getting new contributors on board.
![Typeclass hierarchy](http://g.gravizo.com/g?
  digraph G {
    margin="0.3"
    node [shape=box,style="rounded"]
    { rank = same; Queue; WorkerPoolManager; }
    { rank = same; Queue; QueueSampler; }
    { rank = same; Worker, WorkerPoolSampler,CircuitBreaker, Autothrottler;}
    Dispatcher
    WorkerPoolManager [group=wp, color=blue]
    WorkerPoolSampler [group=wp]
    Autothrottler [group=wp]
    CircuitBreaker [group=wp]
    Regulator
    QueueSampler [group=queue]
    Worker [group=wp, color=blue]
    Queue [group=queue, color=blue]
       HandlerProvider -> Handler
    subgraph cluster_0 {
     Handler [shape=oval, group=service]
     HandlerProvider [shape=oval, group=service]
      graph[style=dotted, label="service"]
    }
    Worker -> Queue [label="dequeu request", style=dotted]
    Worker -> Handler [label="send request", style=dotted]
    Dispatcher -> Queue
    Dispatcher -> Queue [label="enqueue request", style=dotted]
    Dispatcher -> QueueSampler
    QueueSampler -> Queue [label="sample metrics", style=dotted]
    Dispatcher -> WorkerPoolManager
    Dispatcher -> Regulator
    Regulator -> Dispatcher [label="regulates", style=dotted]
    Regulator -> QueueSampler [label="retrieve metrics", style=dotted]
    WorkerPoolManager -> Worker
    WorkerPoolManager -> CircuitBreaker
    WorkerPoolManager -> Autothrottler
    Autothrottler -> WorkerPoolManager [label="resize worker pool",style=dotted]
    WorkerPoolManager -> Handler [Label="associated with", style=dotted]
    WorkerPoolManager -> WorkerPoolSampler
    WorkerPoolSampler -> WorkerPoolManager [label="sample metrics", style=dotted]
    Dispatcher -> HandlerProvider [label="retrieve handlers", style=dotted]
    Autothrottler -> WorkerPoolSampler [label="retreive metrics", style=dotted]
  }
)

The core of this system is `Queue`, `WorkerPoolManager` and `Worker`. The `Queue` buffers requests, `WorkerPoolManager` creates and manages `Worker`s who dequeue requests from the `Queue` and send them to the service. The service is abstracted as `Handler` and `HandlerProvider`. A `Handler` represents one instance of the service, while the `HandlerProvider` provides a list of `Handler`s. These two traits are not actors. They are implemented to support different protocols and service locators.

On the queue side, the `QueueSampler` samples the queue's length and dequeue speed, the `Regulator` then applies the PIE traffic regulation logic based on the metrics collected by `QueueSampler`.
On the worker pool side, the `WorkerPoolSampler` samples the performance of the `Handler`, i.e. the service, at each pool size, then the `AutoThrottler` resizes the worker pool based on the optimization logic during traffic oversaturations. The `Dispatcher` is the top supervisor in the hierarchy.

When kanaloa load balances between multiple service instances, it creates for each service instance a dedicated `WorkerPoolManager`. This `WorkerPoolManager` comes with its own `WorkerPoolSampler` and `Autothrottler`, which means that the concurrency throttling is per service instance.
