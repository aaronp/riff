package riff.raft.timer
import scala.concurrent.duration._
import scala.util.Random

/**
  * A utility class for use when implementing a [[RaftClock]].
  * It simply contains a 'next' for producing random timeout durations based around a percentage.
  *
  * e.g. if you wanted to timeout +/- 25% of 1 minute, 'next' would produce values between 45 and 75 seconds (60 seconds plus or minus 25%, or 15 seconds)
  *
  * @param timeout the base timeout value
  * @param percentageOfTimeout the "plus or minus" percentage expressed as a decimal (e.g. 20% would be 0.2, which is the default)
  */
class RandomTimer(minTimeout: FiniteDuration, maxTimeout: FiniteDuration) {
  require(maxTimeout >= minTimeout)
  private def range: Long = (maxTimeout - minTimeout).toMillis

  /**  @return the next random value in the range
    */
  def next(): FiniteDuration = {
    if (range == 0) {
      minTimeout
    } else {
      val randDuration: FiniteDuration = (Random.nextLong() % range).abs.millis
      minTimeout + randDuration
    }
  }
}
object RandomTimer {
  def apply(minTimeout: FiniteDuration, maxTimeout: FiniteDuration): RandomTimer = new RandomTimer(minTimeout, maxTimeout)
}