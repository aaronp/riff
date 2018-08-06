# Riff
![sbt test](https://travis-ci.org/aaronp/riff.svg?branch=master)

'Riff' is (another) Raft implementation, written in scala.

Having had a look at existing implementations, most on the JVM seemed strongly biased to a particular framework.
The scala ones in particular being tied to Akka.

I tried to take the approach where a Raft node's instance is represented by a single stateful class which takes some requests
as inputs and produces requests/responses as output.

That stateful class can then be 'lifted' into some other context... put behind an akka actor, into a monix/fs2 stream, REST service, etc.
(as opposed to being coupled to any of those directly).

From the outside, the Raft node takes requests, responses and timer messages as inputs and produces requests/responses as
outputs.

Internally, the Raft log or ephemeral node state can also easily implement a reactive streams Publisher so that clients
of this library can e.g. observe the log, state transitions, etc.

## Building

It is currently built using sbt, though I hope to add Mill (and fury?) support shortly
Mill certainly looks really promising!

```
sbt clean coverage test coverageReport
```

You can read more about the design and check the documentation [here](https://aaronp.github.io/riff)
