package riff
import java.util.concurrent.TimeUnit

import org.scalatest.BeforeAndAfterAll
import org.scalatest.concurrent.Eventually
import riff.raft.timer.{RaftClock, RandomTimer}
import riff.reactive.newContextWithThreadPrefix

import scala.concurrent.ExecutionContextExecutorService
import scala.concurrent.duration._

class RiffThreadedSpec extends RiffSpec with BeforeAndAfterAll with Eventually {

  override implicit def testTimeout: FiniteDuration = 5.seconds

  protected def newExecCtxt(): ExecutionContextExecutorService = {
    newContextWithThreadPrefix(getClass.getSimpleName)
  }

  def withExecCtxt[T](f: ExecutionContextExecutorService => T): T = {
    val ctxt = newExecCtxt()
    try {
      f(ctxt)
    } finally {
      ctxt.shutdown()
      ctxt.awaitTermination(testTimeout.toMillis, TimeUnit.MILLISECONDS) shouldBe true
    }
  }

  def withClock[T](hbTimeout: FiniteDuration)(thunk: RaftClock => T): T = {
    val clock = RaftClock(hbTimeout, RandomTimer(hbTimeout + 200.millis, hbTimeout + 300.millis))
    try {
      thunk(clock)
    } finally {
      clock.close()
    }
  }

}
