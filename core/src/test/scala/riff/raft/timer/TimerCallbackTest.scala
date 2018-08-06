package riff.raft.timer
import java.util.concurrent.atomic.AtomicInteger

import riff.RiffSpec

class TimerCallbackTest extends RiffSpec {
  "TimerCallback.ops" should {
    "improve our code coverage" in {
      import TimerCallback.ops._
      implicit val cb = new TimerCallback[String] {
        val sendCalls    = new AtomicInteger(0)
        val receiveCalls = new AtomicInteger(0)
        override def onSendHeartbeatTimeout(node: String): Unit = {
          sendCalls.incrementAndGet()
        }
        override def onReceiveHeartbeatTimeout(node: String): Unit = {
          receiveCalls.incrementAndGet()
        }
      }

      "a".onSendHeartbeatTimeout
      "b".onSendHeartbeatTimeout
      "c".onSendHeartbeatTimeout
      cb.sendCalls.get() shouldBe 3

      "".onReceiveHeartbeatTimeout
      cb.receiveCalls.get() shouldBe 1
    }
  }

}
