package riff.reactive
import riff.RiffThreadedSpec

import scala.concurrent.ExecutionContext

class CollectPublisherTest extends RiffThreadedSpec {

  "CollectPublisher" should {
    "filter based on the predicate while still honoring the total number requested" in {
      Given("A filtered publisher on even integers")
      object Integers extends AsyncPublisher[Int] {
        override implicit def ctxt: ExecutionContext = execCtxt
        def push(values: Iterable[Int]) = {
          values.foreach(enqueueMessage)
          enqueueComplete()
        }
        override protected def maxQueueSize: Int = 200
      }

      val filtered = CollectPublisher(Integers) {
        case i if i % 2 == 0 => i + 100
      }

      When("a subscriber requests 2 elements")
      val listener = new TestListener[Int](0, 0)
      filtered.subscribe(listener)

      Integers.push(0 until 10)

      listener.received shouldBe (empty)
      listener.request(2)

      Then("the listener should receive those two elements")
      eventually {
        listener.received.toList should contain only (100, 102)
      }

      When("the listener requests the remaining 3 elements")
      listener.request(3)

      When("the listener should receive the remaining 3 positive ints")
      eventually {
        listener.received.toList should contain only (100, 102, 104, 106, 108)
      }

      When("the listener requests the next element")
      listener.request(1)
      Then("it should be told the elements are complete")
      eventually {
        listener.completed shouldBe true
      }
    }
  }
}
