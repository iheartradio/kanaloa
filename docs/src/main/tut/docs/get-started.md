---
layout: page
title:  Get Started
section: get-started
position: 1
---


#### Install dependency
```
resolvers += Resolver.jcenterRepo

libraryDependencies +=  "com.iheart" %% "kanaloa-core" % "0.5.0"
```

#### Use proxy

```tut:silent
import kanaloa._
import scala.concurrent.Future
import scala.concurrent.ExecutionContext.Implicits.global
```
```tut:book
val proxyFactory = ReverseProxyFactory()

//assuming this is your service
val service = (i: Int) => Future(i.toString)

val proxy: Int => Future[Either[WorkException, String]] = proxyFactory(service)
//voilà, you have a super resilient toString service now.

val result = proxy(42)

result.foreach(println)

Thread.sleep(100) //job done! let's shutdown the system
proxyFactory.close()

```


#### Config kanaloa [Optional]

An example of a `my-service1`
```
kanaloa {
  dispatchers {
    my-service1 {
      worker-pool {
       starting-pool-size = 50
      }
    }
  }
}
```
For more configuration settings and their documentation please see the [reference configuration](https://github.com/iheartradio/kanaloa/blob/master/core/src/main/resources/reference.conf)
