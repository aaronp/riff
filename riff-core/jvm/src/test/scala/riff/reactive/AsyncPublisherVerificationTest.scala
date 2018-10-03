package riff.reactive
import eie.io.Lazy
import org.reactivestreams.Publisher
import org.reactivestreams.tck.PublisherVerification
import org.testng.annotations.AfterTest

import scala.concurrent.ExecutionContext

class AsyncPublisherVerificationTest extends PublisherVerification[Long](testEnv) {

  private val lazyCtxt = Lazy(newContextWithThreadPrefix(getClass.getSimpleName))

  implicit def execContext = lazyCtxt.value

  @AfterTest
  def afterAll(): Unit = {
    lazyCtxt.foreach(_.shutdown())
  }
  override def createPublisher(elements: Long): Publisher[Long] = {
    new AsyncPublisher[Long] {

      // use a larger queue as we enqueue chunks
      override def maxQueueSize: Int =
        if (elements > 10000) {
          100000
        } else {
          100
        }
      override implicit def ctxt: ExecutionContext = execContext
      override protected def add(sub: AsyncSubscription[Long]): AsyncSubscription[Long] = {

        val r = super.add(sub)
        if (elements > 10000) {
          execContext.execute(new Runnable {
            override def run() = {

              var i = elements
              while (i > 0) {
                val next = i.min(500).toInt
                enqueueMessages((0 until next).map(_.toLong))
                i = i - next
                Thread.`yield`()
              }
              enqueueComplete()
            }
          })

        } else {
          var i = elements
          while (i > 0) {
            val next = i.min(Int.MaxValue).toInt
            enqueueMessages((0 until next).map(_.toLong))
            i = i - next
          }
          enqueueComplete()
        }
        r
      }
    }
  }
  override def createFailedPublisher(): Publisher[Long] = {
    new AsyncPublisher[Long] {
      override protected def maxQueueSize: Int = 200
      override implicit def ctxt: ExecutionContext = execContext
      enqueueError(new Exception("kapow"))
    }
  }
}
