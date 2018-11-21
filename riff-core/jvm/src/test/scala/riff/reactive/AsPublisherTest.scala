package riff.reactive
import org.reactivestreams.Publisher
import riff.RiffThreadedSpec

class AsPublisherTest extends RiffThreadedSpec {

  import AsPublisher._
  import AsPublisher.syntax._
  "AsPublisher.takeWhile" should {
    "complete when the predicate completes" in {
      val integers: Publisher[Int] = FixedPublisher((0 to 10), false)
      val pub                      = integers.takeWhile(_ != 4)

      val listener = pub.subscribeWith(new TestListener[Int](100, 100))
      listener.received.toList shouldBe List(0, 1, 2, 3)
      listener.completed shouldBe true
    }
  }
  "AsPublisher.takeWhileIncludeLast" should {
    "complete when the predicate completes but include the first result which returned false" in {
      val integers: Publisher[Int] = FixedPublisher((0 to 10), false)
      val pub                      = integers.takeWhileIncludeLast(_ != 4)

      val listener = pub.subscribeWith(new TestListener[Int](100, 100))
      listener.received.toList shouldBe List(0, 1, 2, 3, 4)
      listener.completed shouldBe true
    }
  }
}
