# Event Sourcing !

I'm probably not as big a fan as Dan North as this blog might lead you to believe, but this next iteration once again
reminded me of his [Software Faster](https://www.youtube.com/watch?v=USc-yLHXNUg) talk.

I'd like to fill out the core a bit more, but I also want to gain some value in getting end-to-end, and the
[EventSource](https://aaronp.github.io/riff/api/riffMonix/riff/monix/EventSource$.html) piece is one of those
end-points which I've been waiting to get to.

There are around 10 lines of code for this, whose punchline is something like:

```scala
        // some assumptions:
        val log : ObservableLog[A] = ...
        val latestIndex : LogIndex = ... // a means to find the last written down index

        val entries: Observable[(LogCoords, A)] = log.committedEntriesFrom(latestIndex)
        entries.zipWithIndex.scan(latestSnapshot) {
            case (state, ((coords, next), i)) =>
              val newState = combine(state, next)
              if (i != 0 && i % snapEvery == 0) {
                dao.writeDown(coords, newState)
              }
              newState
        }
```

That essentially is saying "Given a stream of data of type A, apply it to some state (which represents your business logic) of type S,
and write it down every 'snapEvery' events":

By putting one of those in front of your raft cluster, you're able to just get an ```Observable[S]``` from which you can base the
rest of your business logic on. And that Observable starts with some "state of the world" and gets updated whenever an entry is committed (e.g. can safely
be applied) and fed into your logic.

Anyway, I just thought that was really nifty. And most of that nift is brought to you via reactive-streams (in this case [monix](https://monix.io/docs/3x/reactive/observable.html)).

Some neat take-aways, just to spell it out, are:
- The consistuent pieces are separated and each stand on their own, adhering pretty closely to a single responsibility principle:
  The Raft bit all lives in riff-core with no ties to any transport, business logic, etc. It's just the data structures and logic
  for implementing RAFT, with a tiny bit of an opinion on IO (which you could take or leave)
- The event sourcing bit should (I hope -- comments welcome) also be easily understood. It's not some big library brought in, but
  just the mapping of a stream of data. Again, this is less about this riff project and more about writing software in terms of
  streams of data.
