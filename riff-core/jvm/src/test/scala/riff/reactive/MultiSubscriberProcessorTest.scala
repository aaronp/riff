package riff.reactive
import java.util.concurrent.{CountDownLatch, TimeUnit}

import org.reactivestreams.{Subscriber, Subscription}
import riff.RiffThreadedSpec

import scala.concurrent.duration._

class MultiSubscriberProcessorTest extends RiffThreadedSpec {

  override implicit def testTimeout: FiniteDuration = 15.seconds

  "MultiSubscriberProcessor" should {
    "be threadsafe" in {
      withExecCtxt { implicit ctxt =>
        // a bunch of stuff which attempts to build confidence (though not prove) a subscriber to our  'MultiSubscriberProcessor'
        // will invoke 'onNext' in a threadsafe manner
        object setup {
          val numItemsToPush  = 500
          val numThreads      = 6
          val queueSize       = numItemsToPush * numThreads
          val multiSubscriber = MultiSubscriberProcessor[String](queueSize, true)

          object TestSubscriber extends Subscriber[String] {
            @volatile private var error     = ""
            def inError                     = error != ""
            @volatile var currentThreadName = ""
            var subscribeCalls              = 0
            @volatile var handled           = List[String]()

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
            override def onError(t: Throwable): Unit = ???
            override def onComplete(): Unit          = {}
          }
          multiSubscriber.subscribe(TestSubscriber)
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
                  multiSubscriber.onNext(s"$threadName pushing $counter")
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
          setup.multiSubscriber.close()
        }

      }
    }

    "publish elements from all publishers" in {
      withExecCtxt { implicit execCtxt =>
        val subscriberUnderTest = MultiSubscriberProcessor[String](100, false)

        try {
          val downstream = subscriberUnderTest.subscribeWith(new TestListener[String]())

          val publishedValues = (0 to 10).map(_.toString)
          val fp              = FixedPublisher(publishedValues, true)
          fp.subscribe(subscriberUnderTest)

          downstream.request(publishedValues.size)
          eventually {
            downstream.received should contain theSameElementsAs (publishedValues)
          }
          downstream.completed shouldBe false
          subscriberUnderTest.complete()
          eventually {
            downstream.completed shouldBe true
          }
        } finally {
          subscriberUnderTest.close()
        }
      }
    }
  }
}
