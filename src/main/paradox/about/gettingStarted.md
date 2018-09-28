# Getting Started

This uses sbt (w/ trial support for mill, perhaps fury at some point) to build.

The core component contains all the APIs and business logic for a raft cluster in a framework-free way.

That core can then be put behind a stream (monix, fs2, akka-streams, RxScala, etc), akka actor, REST service, whatever.

It also provides scalajs support, should you fancy running your server code in a browser, which can be a nifty way to more easily
test multiple components in a cluster. Imagine starting/stopping a component in your micro-service suite by just opening a new 
browser tab!

## Usage

I've not officially released any version of this yet.
The indent is to end up with a riff-monix, riff-akka, riff-http4s, riff-vertx, etc, and so you'd bring in
whichever is relevant to you ... or just riff-core and add it to your own transport, etc.

The above could just be considered examples of how to factor software w/o a dependency on any particular 'framework'.

I think in general projects should focus more on representing their business logic accurately and modeling it in a way that
makes sense, and then just putting that code behind a service, queue, socket, etc.




## Building

The main build still uses sbt, and so you could just:

```
sbt package
```

I've made a start moving to [mill](https://github.com/lihaoyi/mill), but it's not there yet.


### Documentation

To generate this documentation, run
```bash
./makeSite.sh
```