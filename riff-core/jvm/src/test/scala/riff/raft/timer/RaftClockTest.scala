package riff.raft.timer
import scala.concurrent.duration._

/**
  * If you have a custom timer (e.g. in monix, cats, fs2, akka, etc), you should just be able to extend the TCK
  * and plug in your implementation like here
  */
class RaftClockTest extends RaftClockTCK {
  override def newTimer(sendHeartbeatTimeout: FiniteDuration, receiveHeartbeat: RandomTimer): RaftClock = {
    RaftClock(sendHeartbeatTimeout, receiveHeartbeat)
  }

  override def assertAfter[T](time: FiniteDuration)(f: => T) = {
    Thread.sleep(time.toMillis)
    f
  }
}
