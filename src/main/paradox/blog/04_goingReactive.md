# Going Reactive

## In the beginning...
I'm once again at that point where I'm playing around with different compromises.

It seems projects love to tout that they have zero dependencies -- and it's understandable why.
Everyone's had to work on that one project (at least) where they wasted hours if not days in dependency hell.

And it's been a strong factor in the decisions I've made on this project. This whole exercise has been exactly
that - an exercise. Like most developers, I like to stay on top of new approaches, paradigms, technologies, etc.
And implementing the Raft protocol here has been a fantastic pet project... it's very well spec'd out, has to deal with
communication/failure of disparate components, random timers, both disk and network IO, etc.

And so it's proven a great place to try out a "real" application against some compelling ideas:
implementations using Free monads/Free applicatives, Tagless Final (finally tagless), effects systems, just "plain old scala", etc.

Most of all, I wanted a core place to try and accurately implement the specification without explicitly tying it to a library.
And to that end, that's what the [RaftMessageHandler](https://aaronp.github.io/riff/api/riffCoreCrossProject/riff/raft/node/RaftMessageHandler.html) and [RaftNode](https://aaronp.github.io/riff/api/riffCoreCrossProject/riff/raft/node/RaftNode.html) are.
The RaftMessageHandler just takes an input (a [RaftMessage](https://aaronp.github.io/riff/api/riffCoreCrossProject/riff/raft/messages/RaftMessage.html)) and produces an output (a [RaftNodeResult](https://aaronp.github.io/riff/api/riffCoreCrossProject/riff/raft/messages/RaftNodeResult.html)).

That, I think, is the general pattern I try to reach for ... a simple complete function to represent some business logic. And in this case the input represents a union
of an append request (from a client), a timer message, or a request or response from another node.


## The proof of the pudding
That's all well and good, but ultimately for this 'experiment' of different approaches to be successful, I'll need to actually deliver a working solution.

And it should be readable, performant, and easily maintainable/extendable. And by readable, that should apply both at the end-user level and the internals as well.

For the end-user, I intend to just be able to have a stream of data via a raft cluster. Something aking to:

```scala
val localDataStream : SomeStreamType[Data] = ...

// this is where the magic happens
val distributedStream : SomeStreamType[Data] = localDataStream.via(raftCluster)

```

or put another way, something like
```scala
A => IO[A]
```
for some effectful IO type.

In both cases they have a stream of data, but the semantics of the second is that consuming the stream of data ensures it is replicated
across a Raft cluster. And the behaviour of that transformation can be explicitly configurable. By default it would produce the output types once
quorum is reached across the cluster, but it could also be after an unacknowledged append, or on full commit, whatever.

And to accomplish that, you end up with (I think) essentially a signature that takes some type 'A' as input and produces a stream of [AppendStatus](https://aaronp.github.io/riff/api/riffCoreCrossProject/riff/raft/AppendStatus.html)) as output,
where each AppendStatus notification tells you the information you'd need to know if you were to honor those semantics, or e.g. represent the append as a progress bar.

## Adding a reactive streams implementation
And it's at this point where it gets tricky. My goal has been to keep raft-core as pure as possible. It doesn't even rely on slf4j
(though it does rely on [reactive-streams](http://www.reactive-streams.org), as that should be quite non-controversial, tiny, and solid as anything).

My first 'real world' implementation was going to be riff-monix, as monix provides a solid streaming library. But it wasn't quite as tiny/effortless
as I had hoped. There was a bit of wiring in of the different node streams, and some logic around message handling into the nodes while still satisfying
non-cluster client requests, such as returning a stream of status updates for only one of the RaftMessage input types ([AppendData](https://aaronp.github.io/riff/api/riffCoreCrossProject/riff/raft/messages/AppendData.html)):

And so, it seemed I had a choice of a simple, clean raft-core, but which left what might be repetitive and tedious wiring-in for downstream subprojects
to implement (e.g. repeat similar code for monix, fs2, akka, http4s, etc).

In the end I decided to go the way the mongo scala-driver has, and provided a basic implementation of the reactive-streams interfaces. I've lately been
quite critical with my current client how they've built up a massive code-ball, and that they should really strive to decouple/split out their monolith,
and now I feel I may have crossed a small line... but I think it's worth it.

I've provided 'AsPublisher' and 'AsSubscriber' type-classes which specialised subprojects can implement, as well as some identity instances. I feel that
it was the right choice, as now the riff-core library is usable itself, even if the publishers/subscribers it offers aren't as tuned/performant as what
monix or fs2 can offer. Luckily the reactive-stream project provides a TCK as well, so I can be confident my default reference implementation is correct.

## Conclusions
Ultimately what convinced me was that I could spin off a separate project just for my basic reactive-streams implementation, but for that to get me
anywhere I'd have to make it a dependency of riff-core... and if I were going to do that, I might as well grow a pair and depend on an existing, mature
implementation directly like monix, fs2 or akka-streams.

Alternatively I could just take all of reactive-streams out of riff-core and leave it pure ... which is arguably a better way, but for the price of about a
dozen simple extra classes, I think having a single, solid, working reference core implementation which only depends on an API library containing 3 interfaces
is a price worth paying

