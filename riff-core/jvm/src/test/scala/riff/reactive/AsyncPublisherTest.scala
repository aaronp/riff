package riff.reactive
import org.reactivestreams.Subscriber
import riff.RiffThreadedSpec

import scala.concurrent.ExecutionContext

class AsyncPublisherTest extends RiffThreadedSpec {

  "AsyncPublisher" should {

    "enqueue but not publish elements before they are requested" in {

      Given("A AsyncPublisher with a single subscriber (which doesn't initially request any elements)")
      val publisherUnderTest = new TestInstance
      val listener = publisherUnderTest.subscribeWith(new TestListener[String](0, 0))
      publisherUnderTest.subscription.inputQueueSize() shouldBe 0

      When("the subscriber requests 3 elements")
      listener.request(3)

      Then("the subscriber should've had that request enqueued")
      publisherUnderTest.subscription.inputQueueSize() shouldBe 1
      val subscribersFirstInput = publisherUnderTest.popNextFromSubscription()
      subscribersFirstInput shouldBe AsyncSubscriptionState.Request(3)

      publisherUnderTest.subscription.inputQueueSize() shouldBe 0

      When("The request message is applied")
      // manually apply what the 'Drain' thread would do
      publisherUnderTest.subscription.updateState(subscribersFirstInput)
      publisherUnderTest.subscription.currentState().totalRequested shouldBe 3
      publisherUnderTest.subscription.currentState().cancelled shouldBe false
      publisherUnderTest.subscription.currentState().complete shouldBe false
      publisherUnderTest.subscription.currentState().valueQueue shouldBe (empty)

      And("An element is enqueued")
      publisherUnderTest.push("hello")

      Then("the subscriber should be notified of the single element and now only have 2 requested")
      publisherUnderTest.subscription.inputQueueSize() shouldBe 1
      val subscribersSecondInput = publisherUnderTest.popNextFromSubscription()
      subscribersSecondInput shouldBe AsyncSubscriptionState.Push("hello")

      // manually apply what the 'Drain' thread would do
      publisherUnderTest.subscription.updateState(subscribersSecondInput)

      publisherUnderTest.subscription.inputQueueSize() shouldBe 0
      publisherUnderTest.subscription.currentState().totalRequested shouldBe 2
      publisherUnderTest.subscription.currentState().cancelled shouldBe false
      publisherUnderTest.subscription.currentState().complete shouldBe false
      publisherUnderTest.subscription.currentState().valueQueue shouldBe (empty)
    }
  }

  class TestInstance extends AsyncPublisher[String] {
    override protected def maxQueueSize: Int = 200
    override implicit def ctxt: ExecutionContext = execCtxt

    // keep track of/expose this new subscriptions for the test
    var subscription: AsyncSubscription[String] = null

    /**
      * we use this function in the test for a bit of safety, as calling popNextInput on the queue could potentially
      * block forever, and we're calling it within the test thread.
      *
      * @return the next input (or error if the queue is empty)
      */
    def popNextFromSubscription() = {

      require(subscription.inputQueueSize() > 0)
      subscription.popNextInput()
    }

    def push(value: String) = enqueueMessage(value)
    def pushAll(values: Iterable[String]) = enqueueMessages(values)
    def complete() = enqueueComplete()
    def error(t: Throwable) = enqueueError(t)

    /**
      * We override the creation of a subscriber within the test, a that drives the queue via the exec context.
      * We elect instead to manually pull from/apply the queued messages within the test
      */
    override protected def newAsyncSubscription(s: Subscriber[_ >: String], onCancel: AsyncSubscription[String] => Unit): AsyncSubscription[String] = {
      subscription shouldBe null
      subscription = new AsyncSubscription[String](s, 100, onCancel)
      subscription
    }
  }
}
