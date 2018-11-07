package riff.reactive

import java.util.concurrent.{CountDownLatch, TimeUnit}

import org.reactivestreams.{Publisher, Subscriber, Subscription}
import riff.RiffThreadedSpec
import riff.reactive.AsPublisher.syntax._
import riff.reactive.ReactivePipe.single

import scala.collection.mutable.ListBuffer

class ReactivePipeTest extends RiffThreadedSpec {

  "ReactivePipe.single" should {

    "be threadsafe" in {
      // a bunch of stuff which attempts to build confidence (though not prove) a subscriber to our  'MultiSubscriberProcessor'
      // will invoke 'onNext' in a threadsafe manner
      object setup {
        val numItemsToPush = 500
        val numThreads = 6
        val queueSize = numItemsToPush * numThreads
        val underTest = ReactivePipe.single[String](queueSize, 10, 100)

        object TestSubscriber extends Subscriber[String] {
          @volatile private var error = ""
          def inError = error != ""
          @volatile var currentThreadName = ""
          var subscribeCalls = 0
          @volatile var handled = List[String]()

          override def onSubscribe(s: Subscription): Unit = {
            subscribeCalls = subscribeCalls + 1
            s.request(Long.MaxValue)
          }
          override def onNext(t: String): Unit = {
            val tname = Thread.currentThread().getName
            if (currentThreadName != "") {
              if (error == "") {
                error = s"both ${tname} and $currentThreadName called onNext ($t) at the same chuffing time"
              }
            }
            currentThreadName = tname
            Thread.`yield`()
            handled = t :: handled
            currentThreadName = ""
          }

          def validate(): Unit = {
            eventually {
              handled.size shouldBe queueSize
            }
            error shouldBe ""
          }

          // we never call these in our test
          override def onError(t: Throwable): Unit = {}
          override def onComplete(): Unit = {}
        }
        underTest.output.subscribe(TestSubscriber)
        TestSubscriber.subscribeCalls shouldBe 1

        // ensure all our threads are set ready to go
        val latch = new CountDownLatch(numThreads)
        val threads = (0 until numThreads).map { i =>
          val threadName = s"${getClass.getSimpleName}-thread-$i"
          val t = new Thread {
            override def run() = {
              latch.countDown()
              latch.await(testTimeout.toMillis, TimeUnit.MILLISECONDS)
              var counter = numItemsToPush
              while (counter > 0 && !TestSubscriber.inError) {
                underTest.input.onNext(s"$threadName pushing $counter")
                counter = counter - 1
              }
            }
          }
          t.setDaemon(true)
          t.setName(threadName)
          t.start()
          t
        }
      }

      try {
        setup.threads.foreach(_.join(testTimeout.toMillis))
        setup.TestSubscriber.validate()

      } finally {
        setup.underTest.close()
      }
    }

    "propagate events from the input to the output" in {

      Given("a single pipe")
      val pipe = ReactivePipe.single[Int](100, 10, 100)

      try {
        When("a listener subscribes to the output")
        val listener = pipe.output.subscribeWith(new TestListener[Int])

        And("data is fed to the input")
        FixedPublisher(1 to 10, true).subscribe(pipe.input)

        Then("Our listener should receive all the events")
        listener.request(10)
        eventually {
          listener.received.size shouldBe 10
        }
      } finally {
        pipe.close()
      }
    }
    "request {batch size} messages when it falls below its threshold" in {

      val batchSize = 7
      val minThreshold = 3
      Given(s"A pipe w/ min threshold of  $minThreshold items and batch size $batchSize")
      val pipe = ReactivePipe.single[String](100, minThreshold, batchSize)

      try {
        object Pub extends Publisher[String] with Subscription {
          private val requestedBuffer = ListBuffer[Long]()
          def requested = requestedBuffer.toList
          var subscriber: Subscriber[_ >: String] = null
          override def subscribe(s: Subscriber[_ >: String]): Unit = {
            subscriber = s
            s.onSubscribe(this)
          }
          override def request(n: Long): Unit = {
            requestedBuffer.append(n)
          }
          override def cancel(): Unit = ???
        }

        When("the input is subscribed to a pipe")
        Pub.subscribe(pipe.input)

        Then(s"It should request the batch size ($batchSize)")
        Pub.requested shouldBe List(batchSize)

        And(s"When it's fed ${batchSize - minThreshold - 1} elements")
        (0 until (batchSize - minThreshold - 1)).foreach { i => //
          Pub.subscriber.onNext(i.toString)
        }
        Then("It should NOT request any more elements")
        Pub.requested shouldBe List(batchSize)

        When(s"It's given its next element, taking it to the 'minThreshold' of $minThreshold")
        Pub.subscriber.onNext("threshold")

        Then(s"It should request ${batchSize} more elements")
        Pub.requested shouldBe List(batchSize, batchSize)
      } finally {
        pipe.close()
      }
    }
  }
  "ReactivePipe.multi" should {

    "be able to feed another processor" in {
      val feedAndSink = single[Int](100, 10, 100)

      val multiSubscriber: MultiSubscriberProcessor[Int] = MultiSubscriberProcessor[Int](20, true)
      try {
        multiSubscriber.subscribe(feedAndSink.input)

        // this should get stuff from multiSubscriber
        val downstream = feedAndSink.output.subscribeWith(new TestListener[Int](10, 100))
        FixedPublisher(1, 2, 3, 4).subscribe(multiSubscriber)
        eventually {
          downstream.received should contain allOf (1, 2, 3, 4)
        }
      } finally {
        multiSubscriber.close()
        feedAndSink.close()
      }
    }
    "publish events received from a single subscription" in {
      val multiPipe = ReactivePipe.multi[Int](20, true)
      try {

        When("a listener subscribes")
        val listener = multiPipe.output.subscribeWith(new TestListener[Int])

        And("the input is fed some data")
        FixedPublisher(0, 1, 2, 3).subscribe(multiPipe.input)

        Then("the listener should receive those inputs")
        listener.request(2)
        eventually {
          listener.received shouldBe List(0, 1)
        }

        listener.request(2)
        eventually {
          listener.received shouldBe List(0, 1, 2, 3)
        }
      } finally {
        multiPipe.close()
      }
    }
    "publish events received from multiple subscriptions" in {
      val multiPipe = ReactivePipe.multi[Int](20, true)

      try {
        val listener = multiPipe.output.subscribeWith(new TestListener[Int]())
        FixedPublisher(1, 2, 3).subscribe(multiPipe.input)
        FixedPublisher(4, 5, 6).subscribe(multiPipe.input)
        FixedPublisher(7, 8, 9).subscribe(multiPipe.input)

        listener.request(10)
        eventually {
          listener.received should contain theSameElementsAs (1 until 10)
        }
      } finally {
        multiPipe.close()
      }
    }
    "publish events received from multiple subscriptions concurrently" in {

      val numPublisters = 10
      val numToPublish = 100
      val expectedReceived = numPublisters * numToPublish

      Given("a multi pipe")
      // the publishers in this test ignore back-pressure and just send elements as fast as they can.
      // That means, in the worst-case scenario, all the publishers could push all their elements before
      // the listener pops a single one, in which case we need to specify a queue big enough to fit all
      // our test elements
      val queueSize = expectedReceived
      val multiPipe =
        ReactivePipe.multi[String](queueSize, true, minimumRequestedThreshold = 10, subscriptionBatchSize = 500)

      try {

        When("a listener subscribes")
        val listener = multiPipe.output.subscribeWith(new TestListener[String]())

        And("events are published concurrently")
        val waitToStart = new CountDownLatch(numPublisters)
        (1 to numPublisters).foreach { i =>
          execCtxt.execute(new Runnable() {
            override def run(): Unit = {
              val pub: FixedPublisher[String] = FixedPublisher((1 to numToPublish).map(x => s"publisherUnderTest $i: $x"), true)
              waitToStart.countDown()
              waitToStart.await(testTimeout.toMillis, TimeUnit.MILLISECONDS)
              pub.subscribe(multiPipe.input)
            }
          })
        }

        Then("Our listener should receive all the events")
        waitToStart.await(testTimeout.toMillis, TimeUnit.MILLISECONDS)
        listener.received.size shouldBe 0

        listener.request(expectedReceived * 2)
        eventually {
          withClue(s"${listener.received.size} != $expectedReceived") {
            listener.received.size shouldBe expectedReceived
          }
        }
      } finally {
        multiPipe.close()
      }
    }
  }
}
