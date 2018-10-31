package riff.reactive
import riff.BaseSpec

class FixedPublisherTest extends BaseSpec {

  "FixedPublisher" should {
    "publish the elements" in {
      val listener = new TestListener[Int]()
      FixedPublisher(10 to 13, true).subscribe(listener)
      listener.request(1)
      listener.received.size shouldBe 1
      listener.completed shouldBe false

      listener.request(10)
      listener.received.size shouldBe 4
      listener.completed shouldBe true

    }
  }
}
