# Meaningful Integration Testing

At this point in the project, I've written some of the 'units':
* The underlying log
* The timers
* Voting/Election logic
* Append entries logic
* Leader behaviour

I'm relatively happy that, given some inputs, those units provide the outputs expected.
What I'm not **as** confident about is if the assertions I've made in those tests are necessarily the best place for them. 

Should certain validation happen at each level, for example, or is it ok to assume
that certain conditions will always hold by the time an entry is appended to the log?

Decisions such as these aren't always easily representable by the type system (and even if technically they can
be represented, the object allocation tax or cognitive overheads may not be worth it).

And so at this point, to gain confidence the system behaves as expected, I'd typically
reach for an 'integration test'. i.e., spin-up some actual [RaftNode](https://aaronp.github.io/riff/api/riffCoreCrossProject/riff/raft/node/RaftNode.html)s and have them talk to each other.

## Wait Before you Integrate!
My problem is this: I want to gain confidence that the system can drive itself.
According to the [Raft Spec](https://raft.github.io/), we should be able to start w/ some initial nodes with
random heartbeat time-outs. When we "let go", the system should drive itself -- the first node(s) to timeout should
send [RaftNode](https://aaronp.github.io/riff/api/riffCoreCrossProject/riff/raft/messages/RequestVote.html) messages


I'm not a strong advocate of "test-first" (see myriad talks by Dan North on this), but I *do* want confidence that the tests are a good
indication that the code works.

I want realistic implementations of the components ([RaftTimer](https://aaronp.github.io/riff/api/riffCoreCrossProject/riff/raft/timer/RaftTimer.html), 
[RaftCluster](https://aaronp.github.io/riff/api/riffCoreCrossProject/riff/raft/cluster/RaftCluster.html), etc) to show that messages can be created from 
the members of a cluster, consumed by the other cluster nodes, replied to, etc (as a production system would do).

And so often at this point, projects tend to do full integration tests to achieve this kind of confidence/coverage.

Except ... there are a number of non-trivial problems I find with that:

### Integration test setup is often messy (and sequential)

You end up having to mess about w/ properly starting up services (and ensuring they're torn down). A problem which gets harder when working in teams. Will current (and future) team members know to do this properly? Will they have to consult the project docs or a wiki to know how to do it right?


Also, you end up having to mess about ensuring each service is on its own port, even when running tests in parallel. And even if that's done right, it's a lot slower than what it has to be.


### They often Introduce Compromises
Even if you're not spinning up whole services, but just testing in multiple threads:

- **You have to put in effort to get meaningful errors when things fail.**
Perhaps just by turning logging on, but you could also end up setting up awkward hooks in your code just so you can subclass within your tests in order to track state
- **You introduce non-determinism.** Unless you're explicitly sprinkling semaphores/latches/whatever throughout your code -- again, perhaps via some 'just-for-testing' hooks. Your tests run locally, but fail on the build server, or a colleague's machine. Or they fail 1 time in 100, and you're forever putting them in a loop to try and catch the state from that one time.
- **They're harder to debug**. You often can't just stick a breakpoint in one part and not affect the test results. You can turn logging on (or up), but piecing together events logs can be time-intensive to figure out, for even simple scenarios.
- **They're slow.** You have to use real time-outs/delays, and so the threads end up actually having to wait for whatever those are set to. Even if you turn everything down to tens or hundreds of milliseconds, you actually have to wait for those tens/hundreds of milliseconds, which is a big tax to pay when you add more test scenarios.
- **They're even slower than that** ... because you also have to wait for things *not* to happen. How long should you wait to ensure a message ISN'T going to be delivered? And how confident can you ever be?
- **They can be harder to reason about.** In a test, you want to control the environment of what you're testing. *This* happens, then *that*. Every time, and know the scenario you're testing. If you want to see what happens if the events happen in a different order, then you should explicitly model and test that.
- **You can't always capture intermediate state.** Without all that special "hooking-in" logic, things can happen before you can observe them
- **Stack-traces are less meaningful.** A small point, but not when the tests start failing or someone else has to look at them
- **It's the wrong level of abstraction.** We want to test the logic of the messages, not whether they can be sent to an actor, over a websocket, via a REST call, put on a queue, whatever. This isn't purist -- those things can be misconfigured, have their own bugs/problems, etc. Our "will the cluster behave when given XYZ" tests shouldn't start to fail because something like the configuration parser was changed.



Remember, at this point we just wanted to test that our system can dive itself via events. 

And if we write those tests in full multi-threaded, integration-test style, that becomes the foundation for all our meaningful tests as we proceed.
Any regression-tests for bugs, performance refactorings, etc, end up paying these prices to just prove the system is still functionally correct.

## So, how should we test?

Well, at the crux of it, we want to assert a series of events. If I set up a cluster w/ certain initial conditions, then some events should occur. 

So if we controll both the scheduler and the "randomness" of the timer, then those events should occur in the same order - every time.

Also, we can poke the cluster in a certain way (append some data via the leader, disconnect a node) and observe the events the cluster generates. This avoid the "wait for things not to happen" tests, as we can see if the timers were used to schedule anything on the timeline. Remember, we know exactly what's going to happen next!


### Mocking Sucks

A quick digression to just mention a few soft rules I often have in the back of my head:

- Source files should tend to be fewer than 250 lines, even w/ comments
- Classes and parameter lists should have fewer than about 6 arguments/parameters/members
- If you have to mock, you've likely done something wrong

And just to explain that last one... if you have some non-trivial logic you want to test, perhaps buried deep in a thing which has some sizeable parameters itself,
I try to first see if I can just take the pieces of information out needed for that logic and put it in a helper function (for which I can write a set of tests),
as opposed to having to mock-out all the other unneeded bits. 

That test/logic can then more easily be read, tested, moved, etc. And you're not having to read that code later saying "why do I need to provide X,Y and Z that I don't need just to compute this thing?"

## Getting to the point

What I've tried which I'm pretty happy with, is writing an implementation of the RaftTimer and RaftCluster which just sticks the events on a timeline (a stack) instead of sending them directly to their recipients.

Then 'RaftSimulator' is the implementaion of the [RaftCluster](https://aaronp.github.io/riff/api/riffCoreCrossProject/riff/raft/cluster/RaftCluster.html)).
The tests ask the 'RaftSimulator' to advance the timeline, which just pops the next event off the stack and hands it to the intended recipient, whose response in-turn is then pushed on the stack.

The RaftSimulator also injects a RaftTimer into the nodes whose 'schedule' and 'cancel' implementations which insert and remove events at particular times.

These test-time components are providing a couple of real implementations (the timer and cluster) to the production code. And to sound more functional, they just allow us to "lift" our real code into a test context by using the interfaces we've already worked out we need in exactly the same way we'll have to "lift" the code to be behind an actor, service, stream, etc.

If we success at this -- proving our logic/functionality produces events generated/consumed are correct -- then we've done it in a cheap way, and so can run them on every change/commit without worry.

Another big plus is that we can easlily run **just** these tests with coverage, which is handy to help identify code that either can just be deleted or is unreachable via the public API.

### Show me the code ... what does it look like?

Well, the main pieces are the [Timeline](https://github.com/aaronp/riff/blob/master/riff-core/shared/src/test/scala/riff/raft/integration/simulator/Timeline.scala), as well as [HasTimeline](https://github.com/aaronp/riff/blob/master/riff-core/shared/src/test/scala/riff/raft/integration/simulator/HasTimeline.scala) that offers some
convenience methods to anything which as a Timeline.

That's then driven by the [RaftSimulator](https://github.com/aaronp/riff/blob/master/riff-core/shared/src/test/scala/riff/raft/integration/simulator/RaftSimulator.scala) which got a bit bigger than I like, but mostly to provide some utility to the tests.

Then [tests](https://github.com/aaronp/riff/blob/master/riff-core/shared/src/test/scala/riff/raft/integration/IntegrationTest.scala) can just use that simulator.
They don't have to extend some special test template, remember to try/finally stop anything, etc. 

And so a snippet from a test as I write it may look like this:
```scala
      When("A follower times out and becomes leader")
      val newLeader = nameForIdx(4)

      // force a timeout
      simulator.applyTimelineEvent(ReceiveTimeout(newLeader))
      simulator.advanceUntil(_.leaderOpt.exists(_.name == newLeader))

      And("The stopped node is restarted")
      simulator.restartNode(someFollower.state.id)

      Then("The new leader should bring the restarted node's log up-to-date")
      simulator.advanceUntilDebug { res =>
        res.nodeSnapshots.forall(_.log.latestCommit == 6)
      }

      simulator.nodes().foreach(_.persistentState.currentTerm shouldBe 2)
```

The 'advanceUntil' is a method which continues to pop events off the timeline (up to some max) until the given predicate is true.
Also, while writing or debugging the tests, you can append a **'Debug'** suffix to dump the timeline/state at every step:

```scala
// this will dump the state to std-out. Replace the 'Debug' when you've understood why the cluster behaves as it is and fix/test accordingly:
simulator.advanceUntilDebug(_.leaderOpt.exists(_.name == newLeader))
```

which spits out the current state of the cluster and the timeline:

```
  - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - 
 0ms   : SendRequest(Node 4,Node 1,RequestVote(2,{term=1, index=6}))
+11ms  : SendRequest(Node 4,Node 2,RequestVote(2,{term=1, index=6}))
+23ms  : SendRequest(Node 4,Node 3,RequestVote(2,{term=1, index=6}))
+107ms : SendTimeout(Node 1)
+126ms : (removed) ReceiveTimeout(Node 1)
+180ms : (removed) ReceiveTimeout(Node 3)
+201ms : (removed) ReceiveTimeout(Node 4)
+222ms : (removed) ReceiveTimeout(Node 3)
+235ms : (removed) ReceiveTimeout(Node 3)
+286ms : ReceiveTimeout(Node 2)
+288ms : (removed) ReceiveTimeout(Node 3)
+290ms : ReceiveTimeout(Node 4)
+342ms : (removed) ReceiveTimeout(Node 4)
+353ms : ReceiveTimeout(Node 3)
+395ms : (removed) ReceiveTimeout(Node 4)
+534ms : (removed) ReceiveTimeout(Node 4)
+600ms : (removed) ReceiveTimeout(Node 4)
+665ms : (removed) ReceiveTimeout(Node 4)

Node 1 (Leader) in cluster of 3 peers: [Node 2,Node 3,Node 4]
    term 1, voted for Node 1
    Node 2 --> Peer(nextIndex=1, matchIndex=0)
    Node 3 --> Peer(nextIndex=7, matchIndex=6)
    Node 4 --> Peer(nextIndex=7, matchIndex=6)    latestCommit=6
    0   | 1   | some entry 0
    1   | 1   | some entry 1
    2   | 1   | some entry 2
    3   | 1   | some entry 3
    4   | 1   | some entry 4
    5   | 1   | some entry 5
Node 2 [stopped] (Follower) in cluster of 3 peers: [Node 1,Node 3,Node 4]
    term 1, voted for Node 1

Node 3 (Follower) in cluster of 3 peers: [Node 1,Node 2,Node 4]
    term 1, voted for Node 1
    latestCommit=6
    0   | 1   | some entry 0
    1   | 1   | some entry 1
    2   | 1   | some entry 2
    3   | 1   | some entry 3
    4   | 1   | some entry 4
    5   | 1   | some entry 5
Node 4 (Candidate) in cluster of 3 peers: [Node 1,Node 2,Node 3]
    term 2, voted for Node 4
    latestCommit=6
    0   | 1   | some entry 0
    1   | 1   | some entry 1
    2   | 1   | some entry 2
    3   | 1   | some entry 3
    4   | 1   | some entry 4
    5   | 1   | some entry 5
```

#### This Approach vs Record/Replay

Sometimes people write fancy "recorders" and serialize some results under e.g. src/test/resources/regressionTests/testFoo.dat

For me, if you're going to be doing that, then just provide a way to provide it as a small text snippet, and then just
put that expectation directly in the test code in a readable way. And that is what 'simulator.timelineAsExpectation' is for.

You would

  * add the following line someplace where you want to assert the state of events:
    ```scala
    println(simulator.timelineAsExpectation)
    ```
  * Run the test
  * Check the output. Does it look like what you expected? If not, edit it so it does. 

    Then, just replace the println w/ its output. That's your "saved" state, which will be reproduced every time on any machine ... until someone
    changes the code:
    ```scala
          simulator.timelineAssertions shouldBe List(
            "SendResponse(Node 4, Node 1, RequestVoteResponse(term=1, granted=true))",
            "SendRequest(Node 1, Node 2, AppendEntries(previous=LogCoords(0, 0), term=1, commit=0, []))",
            "SendRequest(Node 1, Node 3, AppendEntries(previous=LogCoords(0, 0), term=1, commit=0, []))",
            "SendRequest(Node 1, Node 4, AppendEntries(previous=LogCoords(0, 0), term=1, commit=0, []))",
            "SendTimeout(Node 1)",
            "ReceiveTimeout(Node 2)",
            "ReceiveTimeout(Node 4)",
            "ReceiveTimeout(Node 3)"
          )
    ```
    * This is possible because our simulator has explicitly controlled the "random" timeout durations, etc.
      So now, barring a logic (or format) chagne, subsequent runs will always have the same events in the same order.


## This doesn't replace real integration tests

In general, I'm very happy w/ this approach, but it isn't a complete replacement for real, full production integration tests. 

Of course you still need those - but only to prove the integration, and not for basic functionality/correctness.

Put another way, the responsibility of what we're require from the integration/system tests is significantly reduced.

Also, if/when our full integration tests __do__ find errors, we should be able to write a test for it using our single-threaded,
RaftSimulator approach, unless the bug truely is related specifically to threading, queues, sockets, etc.

And it's worth noting that the code we're testing is, by design, not thread-safe anyway. It's intended
to be put being something which can guarantee thread safety (e.g. lifted into a reactive stream, actor, into a service, etc).


## That's it!

We're using the main, production code to generate messages, and then feed those messages to their recipients. 

The code to do that is still relatively small - just a stack for the timeline (w/ the convenience of a tracked history and removed events), and a 'RaftSimulator' for advancing that timeline and applying the messages to their destinations.
