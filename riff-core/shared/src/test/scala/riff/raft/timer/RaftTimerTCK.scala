package riff.raft.timer

import riff.RiffSpec

import scala.concurrent.duration._

trait RaftTimerTCK extends RiffSpec {

  def newTimer(sendHeartbeatTimeout: FiniteDuration, receiveHeartbeatTimeout: FiniteDuration)(implicit callback: TimerCallback): RaftTimer

  def falseHeartbeatTimeout = 10.millis
  def slowHeartbeatTimeout  = 100.millis

  val scalingFactor = 5
  "RaftTimer" should {
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
        callback.sentCalls shouldBe empty
        callback.receivedCalls shouldBe empty
      }
    }
    "not invoke the send callback if cancelled within the timeout" in {
      implicit val callback = new TestCallback

      val heartbeatTimeout = slowHeartbeatTimeout
      val timer: RaftTimer = newTimer(
        sendHeartbeatTimeout = heartbeatTimeout,
        receiveHeartbeatTimeout = 1.minute
      )

      When("The send heartbeat is reset")

      val resetTime = System.currentTimeMillis()
      val c         = timer.resetSendHeartbeatTimeout("test", None)

      assertAfter(heartbeatTimeout / scalingFactor) {
        And("then cancelled before the timeout is reached")
        val cancelTime = System.currentTimeMillis()
        timer.cancelTimeout(c)

        withClue(s"cancel failed after a reset was called at ${resetTime}, then cancelled at $cancelTime w/ timeout of $heartbeatTimeout") {
          callback.sentCalls shouldBe empty
          callback.receivedCalls shouldBe empty
        }
      }
    }
    "not invoke the callbacks if continually reset" in {
      implicit val callback = new TestCallback

      val heartbeatTimeout = slowHeartbeatTimeout
      val timer: RaftTimer = newTimer(
        sendHeartbeatTimeout = heartbeatTimeout,
        receiveHeartbeatTimeout = heartbeatTimeout
      )

      var lastResetTime   = System.currentTimeMillis()
      var previousSend    = Option(timer.resetSendHeartbeatTimeout("test", None))
      var previousReceive = Option(timer.resetReceiveHeartbeatTimeout("test", None))

      val deadline = (heartbeatTimeout * 3).fromNow
      while (!deadline.isOverdue()) {
        assertAfter(heartbeatTimeout / 2) {
          previousSend = Option(timer.resetSendHeartbeatTimeout("test", previousSend))
          previousReceive = Option(timer.resetReceiveHeartbeatTimeout("test", previousReceive))

          withClue(s"the callback(s) were invoked even after a reset was called at ${lastResetTime}, w/ hb timeout $heartbeatTimeout") {
            callback.sentCalls shouldBe empty
            callback.receivedCalls shouldBe empty
          }

          lastResetTime = System.currentTimeMillis()
        }
      }
    }
    "invoke callbacks if not reset within the timeout" in {
      implicit val callback = new TestCallback

      val heartbeatTimeout = slowHeartbeatTimeout
      val timer: RaftTimer = newTimer(
        sendHeartbeatTimeout = heartbeatTimeout,
        receiveHeartbeatTimeout = heartbeatTimeout
      )

      val lastResetTime = System.currentTimeMillis()

      timer.resetSendHeartbeatTimeout("test", None)
      timer.resetReceiveHeartbeatTimeout("test", None)

      assertAfter((heartbeatTimeout * 1.5).asInstanceOf[FiniteDuration]) {

        withClue(
          s"the callback(s) were invoked even after a reset was called at ${lastResetTime}, time now is ${System.currentTimeMillis()} w/ hb timeout $heartbeatTimeout") {
          callback.sentCalls should not be empty
          callback.receivedCalls should not be empty
        }
      }
    }
  }


  def assertAfter[T](time : FiniteDuration)(f : => T)

  class TestCallback extends TimerCallback {
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
