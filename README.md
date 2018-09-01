# Riff
[![Build Status](https://travis-ci.org/aaronp/riff.svg?branch=master)](https://travis-ci.org/aaronp/riff)

A Raft implementation written in scala which cleanly separates the Raft logic from any particular
transport/framework.

The core project only has a dependency on eie (a tiny IO library), and then offers support for akka,monix,fs2,http4s,etc
in separate sub-projects.

The outer-most representation clearly takes its inputs (requests, responses and timer messages) and produces addressed
messages which can then be used by the sub-projects to send REST requests, put messages on queues, pipe them through a
stream, etc.

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
