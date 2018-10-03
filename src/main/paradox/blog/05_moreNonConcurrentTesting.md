# Another (non) concurrent test case
I'd wrote [earlier](01_raftSimulatorForTheWin.html) about the benefits of writing what would otherwise be
a concurrent test in a single-threaded way using a timeline utility.

I've recently had another, perhaps more straight-forward example I wanted to mention.

Again, I should first say there's nothing necessarily wrong with testing multi-threaded code. I'm just
trying to provide an argument for taking some pause before just throwing in an "eventually this should happen" style
of test.

And to recap, some of the benefits of testing in a single thread are:
 - no need to wait an arbitrary amount of time to gain some certainty an unexpected event will happen
 - the test itself may be faster as it doesn't have to spin up/destroy threads or thread pools
 - the test may be simpler - no additional latches, semaphores, exchangers, whatever to coordinate conditions
 - added determinism... not having tests which fail on slow build agents, etc
 - more easily debugged/stepped through

And that of course comes at a price, but that price may be small. The specific instance in this case
was for a test case around decoupled producers/consumers.

I'd decided to include a basic reference implementation of reactive-streams with riff-core, and so needed to
have some test for an async, decoupled producer and consumer of events.

I went with a simple blocking queue which would be populated by events from either the producer or consumer.
Essentially just folding the inputs over [AsyncSubscriptionState](https://aaronp.github.io/riff/api/riffCoreCrossProject/riff/reactive/AsyncSubscriptionState.html)

The state itself is immutable, and an [AsyncSubscription](https://aaronp.github.io/riff/api/riffCoreCrossProject/riff/reactive/AsyncSubscription.html) holds a reference to the current state.
All that's left is for the producer and subscriber to each enqueue their commands to that subscription, and a Runnable is submitted to an ExecutionContext in
the companion object which is effectively just runs:

```scala
      while (sub.processNext()) {
        // processNext has already don the work, just loop and pull the next input
      }
```

## Obviously no bugs or no obvious bugs?
I can't really speak for the [AsyncSubscription](https://aaronp.github.io/riff/api/riffCoreCrossProject/riff/reactive/AsyncSubscription.html) as a whole,
but certainly the loop above fits the description of the former more than the latter.

And so when it came time to run the test, I was pretty confident that that submitting that while loop to an ExecutionContext was going to
work just fine, and so I decided that the test case could run the producer (publisher) and consumer in the same test thread, allowing
me to reap some of the benefits I mentioned. I just wanted to make sure that I put a little safety net around the
code which pulled from the queue.

Given that it's a blocking queue, A future change/refactor could end up making that test block indefinitely if it tried to take from
an empty queue, so I just put a 'safeNext' utility function within my test case:

```scala
      val subUnderTest = new AsyncSubscription[Int](wrappedSubscriber, 10, cancelled)
      subUnderTest.inputQueueSize() shouldBe 0

      // this is 'safe' because 'processNext' is a blocking call on the next queue input
      def safeNext(): Boolean = {
        if (subUnderTest.inputQueueSize() > 0) {
          subUnderTest.processNext()
        } else {
          false
        }
      }
```

It's certainly not the most earth-shattering code (or test), but I am pleased to have another example of using an arguable simpler approach to what
would otherwise be a more complicated test. And I could hear faint echos of Rich Hickey's ["Simple Made Easy"](https://www.infoq.com/presentations/Simple-Made-Easy) talk in the back of my head when I wrote it ("Just use a queue").
I hope he wouldn't object to the reference.