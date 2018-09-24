## Getting Started

This uses sbt (w/ trial support for mill, perhaps fury at some point) to build.

The core component contains all the APIs and business logic for a raft cluster in a framework-free way.

That core can then be put behind a stream (monix, fs2, akka-streams, RxScala, etc), akka actor, REST service, whatever.

It also provides scalajs support, should you fancy running your server code in a browser, which can be a nifty way to more easily
test multiple components in a cluster. Imagine starting/stopping a component in your micro-service suite by just opening a new 
browser tab!

### Building
To build:
```
sbt package
```

### Documentation

```bash
./makeSite.sh
```