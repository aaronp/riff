package riff.reactive
import org.reactivestreams.{Publisher, Subscriber, Subscription}
import riff.RiffSpec

class SubscribersTest extends RiffSpec {

  "Subscribers.NoOp" should {
    "cancel its subscription immediately" in {
      val s = Subscribers.NoOp[Int]
      val pub = new Publisher[Int] with Subscription {
        override def subscribe(sub: Subscriber[_ >: Int]): Unit = {
          sub.onSubscribe(this)
        }
        override def request(n: Long): Unit = {
          fail("request called")
        }
        var cancelled = false
        override def cancel(): Unit         = {
          cancelled = true
        }
      }

      pub.subscribe(s)
      pub.cancelled shouldBe true


      // none of these should fail
      s.onComplete()
      s.onNext(123)
      s.onError(new Exception)
    }
  }
}
