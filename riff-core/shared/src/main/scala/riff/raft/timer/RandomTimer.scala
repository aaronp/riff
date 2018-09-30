package riff.raft.timer
import scala.concurrent.duration._
import scala.util.Random

/**
  * Adds some functionality to creating a random range for a timeout
  *
  * @param timeout
  */
class RandomTimer(timeout: FiniteDuration, percentageOfTimeout: Double = 0.2) {
  private def range = timeout.toMillis * percentageOfTimeout
  private val minTimeout = timeout.toMillis - range
  private val factor = (range * 2).toInt

  def next(): FiniteDuration = {
    val millis = minTimeout + Random.nextInt(factor)
    millis.millis
  }
}
