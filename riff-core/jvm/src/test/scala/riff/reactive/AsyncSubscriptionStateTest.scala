package riff.reactive
import org.reactivestreams.{Subscriber, Subscription}
import riff.RiffSpec
import riff.reactive.AsyncSubscriptionState.Request

class AsyncSubscriptionStateTest extends RiffSpec {

  "AsyncSubscriptionState.update" should {
    "process all the elements when the total queued is greater than the batch size" in {
      val batchSize = 4
      val lots: Vector[Long] = (0L to batchSize * 3).toVector

      var receivedCount = 0L
      val subscriber = new Subscriber[Long] {
        override def onSubscribe(s: Subscription): Unit = ???
        override def onNext(t: Long): Unit = {
          receivedCount = receivedCount + 1
        }
        override def onError(t: Throwable): Unit = ???
        override def onComplete(): Unit = ???
      }

      Given(s"A state where ${batchSize} is already requested and there are ${lots.size} elements")
      val state = AsyncSubscriptionState[Long](subscriber, batchSize, lots, false, false, batchSize = batchSize)

      val newState = state.update(Request(3))
      // we should only have 10 - 3 (or 7) elements left
      newState.totalRequested shouldBe 0
      receivedCount shouldBe 4 + 3
      newState.valueQueue.size shouldBe lots.size - 7
    }
  }
}
