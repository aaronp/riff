# Writing integration tests using a stack for a timeline

Why hello, future self (and perhaps one other person who might accidentally read this).

I'm just in the process of flushing out the 'RaftSimulator' and 'Timeline' stuff and how best to use it.

In general I'm really pleased with this approach. The 'IntegrationTest' gives me a lot of
confidence by letting the components all generate their own messages, set (and reset/cancel) their
own timeouts, etc. Those parts are all real, production code using the interfaces given to
them.

The actual implementations for that test is given to the nodes by the RaftSimulator,
which then just puts the events (timeouts, resets, messages, whatever), on a stack (the Timeline).

The timers used even have a 'random' element ... except in these tests those random times
are completely deterministic.

## A bit of a breakthrough? ...making it easy to quickly inspect and write (hopefully meaningful) tests

I've just introduced a helper method on HasTimeline - the 'timelineAsExpectation' and 'timelineAssertions'.

These are two sides to the same coin, and are a means of explicitly putting something in code which we all (I think)
do manually. And that is run some code, dump some state, and then take that text dump and turn it into an assertion.

Sometimes people write fancy "recorders" and serialize some results under e.g. src/test/resources/regressionTests/testFoo.dat

For me, if you're going to be doing that, then just provide a way to provide it as a small text snippet, and then just
put that expectation directly in the test code in a readable way. And that is what 'simulator.timelineAsExpectation' is for.

You would

  * add the following line someplace where you want to assert the state of events:
    ```scala
    println(simulator.timelineAsExpectation)
    ```
  * Run the test
  * Check the output. Does it look like what you expected? If not, edit it. Otherwise, just paste that
    output over the println:
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

## And ... profit!
And so, we have very fast, meaningful integration tests.

We've gained:
  * Speed!
    We're always just popping the next event off the stack, which we can do immediately.
    e.g., if the next even will occur in 500ms, we don't have to actually wait for
    500ms as we would if this were multi-threaded. We just pop off that even and advance the current time by 500ms.
    "But, what if another event were to ..." -- STOP! 'cause it won't. If it did, it'd be on the timeline.
  * The ability to manufacture error scenarios. Since we have a timeline we're advancing, we can manually manipulate it
    at any time to drop events, insert erroneous events, etc. These are scenarios which would take some serious effort (and
    likely polluted prod code) to have to engineer otherwise.
  * Meaningful stack traces. It's all on the same test thread.
  * Not having to wait some arbitrary time for an event NOT to happen. If there is some timeout set in XYZ ms in
    the future, we can investigate the timeline and prove nothing's been queued.

We've lost:
  * Being forced to set very small timeouts, or tweak timeouts for different or slow environments.


## This doesn't replace real integration tests

... but it does reduce the load significantly of what we're expecting from those kinds of tests.
We can be happy that the events generated/consumed are correct in a cheap way, and so can run them
on every change/commit without worry.

We don't have to check for free ports, spin up or consume any services, etc.

And it's worth noting that the code we're testing is, by design, not thread-safe anyway. It's intended
to be put being something which can guarantee thread safety (e.g. lifted into a reactive stream, actor, into a service, etc).

This approach isn't a replacement for real, full production integration tests. You still need those -
but only to prove the integration, and not basic functionality/correctness.

## That's it!

We're using the main, production code to generate messages, and then
feed those messages to their recipients. The code to do that is still relatively small -
just a stack for the timeline (w/ the convenience of a tracked history and removed events),
and a 'RaftSimulator' for advancing that timeline and applying the messages to their destinations.
