package riff.reactive
import riff.RiffSpec

/**
  * This tests the AsyncSubscription. In practice a Runnable is started which will drive 'processNext' in another
  * thread. This test is single-threaded, however, and so is deterministic, although not completely indicative
  * of a real scenario.
  *
  *
  * The Runnable will simply call 'processNext' on the AsyncSubscription, which is what we do here in this test.
  */
class AsyncSubscriptionTest extends RiffSpec {

  // REMEMBER - the 'processNext' is invoked in a thread started in the AsyncSubscription's companion object.
  // in this test, however, we 'new one up' directly, and so have complete control over simulating:
  // 1) a producer calling 'enqueue' to push elements, 'complete' to stop, or 'onError' to notify of an error
  // 2) The wrapped subscriber requesting more elements
  // 3) the AsyncSubscription processing each elements in its input queue, which will be populated by events from 1 and 2
  //
  // in order to do that safely*, we use 'safeNext' instead of calling the 'processNext' directly. This is because 'processNext'
  // may block indefinitely if the queue is empty, and so
  "AsyncSubscription.processNext" should {
    "return false when completed and the underlying subscriber has received all elements" in {

      Given("An AsyncSubscription")
      var cancelCalls = 0
      def cancelled(inst: AsyncSubscription[Int]) = {
        cancelCalls = cancelCalls + 1
      }
      val wrappedSubscriber = new TestListener[Int]()
      val subUnderTest      = new AsyncSubscription[Int](wrappedSubscriber, 10, cancelled)
      subUnderTest.inputQueueSize() shouldBe 0

      // this is 'safe' because 'processNext' is a blocking call on the next queue input
      def safeNext(): Boolean = {
        if (subUnderTest.inputQueueSize() > 0) {
          subUnderTest.processNext()
        } else {
          false
        }
      }

      When("Some elements are pushed from a publisher")
      // push 3 elements and then complete
      subUnderTest.enqueue(1)
      subUnderTest.enqueue(2)
      subUnderTest.enqueue(3)
      subUnderTest.complete()

      //Then("Those elements should not yet be pushed until they are requested from the underlying subscription")
      // our subscription's not yet pulled any values, so it's queue should be empty
      subUnderTest.inputQueueSize() shouldBe 4
      subUnderTest.currentState().valueQueue.size shouldBe 0

      // the process loop pushes those to the state, but they don't yet notify the subscriber
      safeNext() shouldBe true

      When("The underlying subscription requests some elememnts")
      // pull some elements
      subUnderTest.request(10)

      Then("processNext should work through the queue")
      // our subscription's not yet pulled any values, so it's queue should be empty
      subUnderTest.currentState().valueQueue should contain only (1)
      // process 2 and 3 and complete
      safeNext() shouldBe true // Push 2
      subUnderTest.currentState().valueQueue should contain only (1, 2)
      safeNext() shouldBe true // Push 3
      subUnderTest.currentState().valueQueue should contain only (1, 2, 3)
      subUnderTest.currentState().complete shouldBe false
      subUnderTest.currentState().completeSignaled shouldBe false

      safeNext() shouldBe true // Complete
      subUnderTest.currentState().valueQueue should contain only (1, 2, 3)
      subUnderTest.currentState().complete shouldBe true
      subUnderTest.currentState().completeSignaled shouldBe false
      subUnderTest.canContinue() shouldBe true
      wrappedSubscriber.received shouldBe empty

      safeNext() shouldBe false // Take 10
      wrappedSubscriber.received should contain only (1, 2, 3)
      wrappedSubscriber.completed shouldBe true
      subUnderTest.currentState().valueQueue shouldBe empty
      subUnderTest.currentState().complete shouldBe true
      subUnderTest.currentState().completeSignaled shouldBe true
      withClue("if canContinue returned false, then we would block this thread indefinitely") {
        subUnderTest.canContinue() shouldBe false
      }
      safeNext() shouldBe false

    }
  }
}
