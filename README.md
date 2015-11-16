# work-dispatcher

[![Join the chat at https://gitter.im/kailuowang/kanaloa](https://badges.gitter.im/Join%20Chat.svg)](https://gitter.im/kailuowang/kanaloa?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge&utm_content=badge)

[![Build Status](https://travis-ci.org/iheartradio/kanaloa.svg)](https://travis-ci.org/iheartradio/kanaloa)

An Akka work pipeline with back pressure control, circuit breaker and auto scaling. 

The main goal of this work pipeline library is to protect backing service (e.g. a DB) from unbounded incoming requests and impose back pressure on the incoming source (Since akka actors are none-blocking, there is no limit on how many concurrent requests an actor can forward to a backend service). 

The work pipeline is implemented with a queue - worker system, each worker waits for the result back from the backend service before it grabs more work from the queue. 

The system calculates the expected wait time on the fly based on recent stats and only accepts new work if the expected wait time is below a certain threshold. It circuit breaks a worker when it hits consecutive errors. And it automatically figures out the best size of worker pool to best utilize the backend service (too many concurrent workers are wasteful and beat the purpose of having a upperbound of concurrent requests to the backend service while too few workers will underutilize the backend service, limiting the throughput unnecessary)
