package riff.reactive
import riff.RiffSpec

class PublishersTest extends RiffSpec {

  "Publishers.NoOpSubscription" should {
    "not error" in {
      Publishers.NoOpSubscription.request(123)
      Publishers.NoOpSubscription.cancel()
    }
  }
  "Publishers.Completed" should {
    "notify subscribers that there is no data upon subscription" in {
      val sub = new TestListener[String]
      Publishers.Completed.subscribe(sub)
      sub.completed shouldBe true
    }
  }
  "Publishers.InError" should {
    "notify subscribers that there is an error" in {
      object Bang extends Exception
      val sub = new TestListener[String]
      Publishers.InError(Bang).subscribe(sub)
      sub.errors.toList shouldBe List(Bang)
    }
  }
  "Publishers.Fixed" should {
    "notify subscribers of the inputs" in {

      val sub = new TestListener[Int](0, 0)
      Publishers.Fixed(List(1, 2, 3, 4)).subscribe(sub)
      sub.received should be(empty)
      sub.request(2)
      sub.received should contain only (1, 2)
      sub.completed shouldBe false
      sub.request(1)
      sub.received should contain only (1, 2, 3)
      sub.completed shouldBe false

      sub.request(1)
      sub.received should contain only (1, 2, 3, 4)
      sub.completed shouldBe true
    }
  }
}
