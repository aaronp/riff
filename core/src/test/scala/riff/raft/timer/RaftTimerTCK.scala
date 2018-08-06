package riff.raft.timer
import java.time.LocalDateTime

import riff.RiffSpec

import scala.concurrent.duration._

trait RaftTimerTCK extends RiffSpec {

  def newTimer[A](sendHeartbeatTimeout: FiniteDuration, receiveHeartbeatTimeout: FiniteDuration)(implicit callback: TimerCallback[A]): RaftTimer[A]

  def falseHeartbeatTimeout = 10.millis
  def slowHeartbeatTimeout  = 100.millis

  "RaftTimer" should {
    "not immediately timeout upon creation" in {
      implicit val callback = new TestCallback

      val heartbeatTimeout = falseHeartbeatTimeout
      val timer: RaftTimer[String] = newTimer[String](
        sendHeartbeatTimeout = heartbeatTimeout,
        receiveHeartbeatTimeout = heartbeatTimeout
      )

      // give it adequate time to invoke our timeout
      Thread.sleep((heartbeatTimeout * 3).toMillis)

      callback.sentCalls shouldBe empty
      callback.receivedCalls shouldBe empty
    }
    "not invoke the send callback if cancelled within the timeout" in {
      implicit val callback = new TestCallback

      val heartbeatTimeout = slowHeartbeatTimeout
      val timer: RaftTimer[String] = newTimer[String](
        sendHeartbeatTimeout = heartbeatTimeout,
        receiveHeartbeatTimeout = 1.minute
      )

      When("The send heartbeat is reset")

      val resetTime = LocalDateTime.now()
      val c         = timer.resetSendHeartbeatTimeout("test", None)

      Thread.sleep((heartbeatTimeout / 2).toMillis)

      And("then cancelled before the timeout is reached")
      val cancelTime = LocalDateTime.now()
      timer.cancelTimeout(c)

      withClue(s"cancel failed after a reset was called at ${resetTime}, then cancelled at $cancelTime w/ timeout of $heartbeatTimeout") {
        callback.sentCalls shouldBe empty
        callback.receivedCalls shouldBe empty
      }
    }
    "not invoke the callbacks if continually reset" in {
      implicit val callback = new TestCallback

      val heartbeatTimeout = slowHeartbeatTimeout
      val timer: RaftTimer[String] = newTimer[String](
        sendHeartbeatTimeout = heartbeatTimeout,
        receiveHeartbeatTimeout = heartbeatTimeout
      )

      var lastResetTime   = LocalDateTime.now()
      var previousSend    = Option(timer.resetSendHeartbeatTimeout("test", None))
      var previousReceive = Option(timer.resetReceiveHeartbeatTimeout("test", None))

      val deadline = (heartbeatTimeout * 3).fromNow
      while (!deadline.isOverdue()) {
        Thread.sleep((heartbeatTimeout / 2).toMillis)

        previousSend = Option(timer.resetSendHeartbeatTimeout("test", previousSend))
        previousReceive = Option(timer.resetReceiveHeartbeatTimeout("test", previousReceive))

        withClue(
          s"the callback(s) were invoked even after a reset was called at ${lastResetTime}, time now is ${LocalDateTime.now()} w/ hb timeout $heartbeatTimeout") {
          callback.sentCalls shouldBe empty
          callback.receivedCalls shouldBe empty
        }

        lastResetTime = LocalDateTime.now()
      }
    }
    "invoke callbacks if not reset within the timeout" in {
      implicit val callback = new TestCallback

      val heartbeatTimeout = slowHeartbeatTimeout
      val timer: RaftTimer[String] = newTimer[String](
        sendHeartbeatTimeout = heartbeatTimeout,
        receiveHeartbeatTimeout = heartbeatTimeout
      )

      val lastResetTime = LocalDateTime.now()

      timer.resetSendHeartbeatTimeout("test", None)
      timer.resetReceiveHeartbeatTimeout("test", None)

      Thread.sleep((heartbeatTimeout * 1.5).toMillis)

      withClue(
        s"the callback(s) were invoked even after a reset was called at ${lastResetTime}, time now is ${LocalDateTime.now()} w/ hb timeout $heartbeatTimeout") {
        callback.sentCalls should not be empty
        callback.receivedCalls should not be empty
      }
    }
  }

  class TestCallback extends TimerCallback[String] {
    var sentCalls     = List[String]()
    var receivedCalls = List[String]()
    private object Lock
    override def onSendHeartbeatTimeout(node: String): Unit = {
      Lock.synchronized {
        sentCalls = node :: sentCalls
      }
    }
    override def onReceiveHeartbeatTimeout(node: String): Unit = {
      Lock.synchronized {
        receivedCalls = node :: receivedCalls
      }
    }
  }
}
