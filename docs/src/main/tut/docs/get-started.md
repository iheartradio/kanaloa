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

#### Add configuration

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
For more configuration settings and their documentation please see the [reference configuration](core/src/main/resources/reference.conf)
