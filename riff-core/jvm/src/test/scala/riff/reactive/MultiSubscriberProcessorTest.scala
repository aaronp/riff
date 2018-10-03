package riff.reactive
import riff.RiffThreadedSpec

class MultiSubscriberProcessorTest extends RiffThreadedSpec {

  "MultiSubscriberProcessor" should {
    "publish elements from all publishers" in {
      val subscriberUnderTest = MultiSubscriberProcessor[String](100, false)
      val downstream = subscriberUnderTest.subscribeWith(new TestListener[String]())

      val publishedValues = (0 to 10).map(_.toString)
      val fp = FixedPublisher(publishedValues)
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
    }
  }
}
