package riff.reactive
import org.scalatest.BeforeAndAfterAll
import org.scalatest.concurrent.Eventually
import riff.RiffSpec

import scala.concurrent.ExecutionContext

class ReplayPublisherTest extends RiffSpec with BeforeAndAfterAll with Eventually {

  private val execContext = newContext()

  override def afterAll(): Unit = {
    execContext.shutdown()
  }
  "ReplayPublisher" should {
    "send elements pushed before a subscription was received" in {

      object TestPublisher extends ReplayPublisher[String] {
        override protected def maxQueueSize: Int = 200
        override implicit def ctxt: ExecutionContext = execContext
        def push(value: String) = enqueueMessage(value)
        def complete() = enqueueComplete()
      }

      TestPublisher.push("first")
      TestPublisher.push("second")
      TestPublisher.push("third")

      val listener = TestPublisher.subscribeWith(new TestListener[String](0, 0))
      listener.completed shouldBe false
      listener.received shouldBe empty

      listener.request(2)
      eventually {
        listener.received.toList shouldBe List("first", "second")
      }

      TestPublisher.push("last")
      TestPublisher.complete()

      listener.request(6)
      eventually {
        listener.received.toList shouldBe List("first", "second", "third", "last")
      }
      listener.completed shouldBe true
    }
  }
}
