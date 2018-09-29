package riff.raft.timer
import scala.concurrent.duration._

/**
  * If you have a custom timer (e.g. in monix, cats, fs2, akka, etc), you should just be able to extend the TCK
  * and plug in your implementation like here
  */
class RaftTimerTest extends RaftTimerTCK {
  override def newTimer(sendHeartbeatTimeout: FiniteDuration, receiveHeartbeatTimeout: FiniteDuration): RaftTimer = {
    RaftTimer(sendHeartbeatTimeout, receiveHeartbeatTimeout)
  }

  override def assertAfter[T](time: FiniteDuration)(f: => T) = {
    Thread.sleep(time.toMillis)
    f
  }
}
