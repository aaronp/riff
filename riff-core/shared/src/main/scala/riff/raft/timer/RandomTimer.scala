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
class RandomTimer(timeout: FiniteDuration, percentageOfTimeout: Double = 0.2) {
  private def range = timeout.toMillis * percentageOfTimeout
  private val minTimeout = timeout.toMillis - range
  private val factor = (range * 2).toInt

  def next(): FiniteDuration = (minTimeout + Random.nextInt(factor)).millis
}
