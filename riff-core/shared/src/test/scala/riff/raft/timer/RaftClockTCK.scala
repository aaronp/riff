package riff.raft.timer

import java.util.concurrent.atomic.AtomicInteger

import riff.RiffSpec

import scala.concurrent.duration._

trait RaftClockTCK extends RiffSpec {

  def newTimer(sendHeartbeatTimeout: FiniteDuration, receiveHeartbeatTimeout: FiniteDuration): RaftClock

  def falseHeartbeatTimeout = 10.millis
  def slowHeartbeatTimeout  = 100.millis

  val scalingFactor = 5
  "RaftClock" should {
    "not immediately timeout upon creation" in {
      implicit val callback = new TestCallback

      val heartbeatTimeout = falseHeartbeatTimeout

      // create a timer which we don't reset
      newTimer(
        sendHeartbeatTimeout = heartbeatTimeout,
        receiveHeartbeatTimeout = heartbeatTimeout
      )

      // give it adequate time to invoke our timeout
      assertAfter(heartbeatTimeout * scalingFactor) {
        callback.sentCalls.get shouldBe 0
        callback.receivedCalls.get shouldBe 0
      }
    }
    "not invoke the send callback if cancelled within the timeout" in {
      implicit val callback = new TestCallback

      val heartbeatTimeout = slowHeartbeatTimeout
      val timer: RaftClock = newTimer(
        sendHeartbeatTimeout = heartbeatTimeout,
        receiveHeartbeatTimeout = 1.minute
      )

      When("The send heartbeat is reset")

      val resetTime = System.currentTimeMillis()
      val c         = timer.resetSendHeartbeatTimeout(callback)

      assertAfter(heartbeatTimeout / scalingFactor) {
        And("then cancelled before the timeout is reached")
        val cancelTime = System.currentTimeMillis()
        timer.cancelTimeout(c)

        withClue(s"cancel failed after a reset was called at ${resetTime}, then cancelled at $cancelTime w/ timeout of $heartbeatTimeout") {
          callback.sentCalls.get shouldBe 0
          callback.receivedCalls.get shouldBe 0
        }
      }
    }
    "invoke callbacks if not reset within the timeout" in {
      implicit val callback = new TestCallback

      val heartbeatTimeout = slowHeartbeatTimeout
      val timer: RaftClock = newTimer(
        sendHeartbeatTimeout = heartbeatTimeout,
        receiveHeartbeatTimeout = heartbeatTimeout
      )

      val lastResetTime = System.currentTimeMillis()

      timer.resetSendHeartbeatTimeout(callback)
      timer.resetReceiveHeartbeatTimeout(callback)

      assertAfter((heartbeatTimeout * 1.5).asInstanceOf[FiniteDuration]) {

        withClue(
          s"the callback(s) were invoked even after a reset was called at ${lastResetTime}, time now is ${System.currentTimeMillis()} w/ hb timeout $heartbeatTimeout") {
          callback.sentCalls.get should be > 0
          callback.receivedCalls.get should be > 0
        }
      }
    }
  }


  def assertAfter[T](time : FiniteDuration)(f : => T)

  class TestCallback extends TimerCallback[Int] {
    var sentCalls     = new AtomicInteger(0)
    var receivedCalls = new AtomicInteger(0)
    override def onSendHeartbeatTimeout() = sentCalls.incrementAndGet()
    override def onReceiveHeartbeatTimeout() = receivedCalls.incrementAndGet()
  }
}
